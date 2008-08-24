begin
  require 'securerandom'
rescue LoadError
  require 'uuid'
end

module Mongo
  class Collection

    def initialize ns, client = nil
      @client = client || (@@client ||= Mongo::Client.connect)
      @ns = ns.to_s.freeze
    end

    # find {:a => 1}, :b < 2, :a.asc, :b.desc, :limit => 10, :skip => 5
    def find obj, *args, &blk
      opts = Hash.new(0)
      opts.update(args.pop) if args.size > 1 and args.last.keys.find{|k| [:limit, :skip].include? k }
      
      # to sort: { query : { ... } , orderby : { ... } }
      obj = {
        :query => obj,
        :orderby => args.inject({}){ |h,a| h.update(a) }
      } if args.any?

      @client.send 2004, :int,     reserved = 0,
                         :cstring, @ns,
                         :int,     opts[:skip],
                         :int,     opts[:limit],
                         :bson,    obj,
                   &blk
    end

    def first *args
      opts = args.pop if args.size > 1 and args.last.keys.find{|k| [:limit, :skip].include? k }
      opts ||= {}
      opts[:limit] = 1
      
      find *(args << opts) do |res|
        yield res.first if block_given?
      end
    end

    def insert obj, add_id = true
      obj[:_id] ||= if defined? SecureRandom
                      SecureRandom.hex(12)
                    else
                      UUID.new(:compact).gsub(/^(.{20})(.{8})(.{4})$/){ $1+$3 }
                    end if add_id

      @client.send 2002, :int,     reserved = 0,
                         :cstring, @ns,
                         :bson,    obj
      obj
    end

    def remove obj
      @client.send 2006,  :int,     reserved = 0,
                          :cstring, @ns,
                          :int,     0,
                          :bson,    obj
    end

    def index obj
      obj = { obj => true } if obj.is_a? Symbol

      indexes.insert({ :name => 'num',
                       :ns => @ns,
                       :key => obj }, false)
    end

    def indexes obj = {}, &blk
      @indexes ||= self.class.new("#{@ns.split('.').first}.system.indexes")
      blk ? @indexes.find(obj, &blk) : @indexes
    end

    def method_missing meth
      (@subns ||= {})[meth] ||= self.class.new("#{@ns}.#{meth}", @client)
    end

  end
end