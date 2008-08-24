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

    # commands

    # find {}, :a.asc, :b.desc, :limit => 10, :skip => 5
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

    def insert obj
      obj[:_id] ||= if defined? SecureRandom
                      SecureRandom.hex(12)
                    else
                      UUID.new(:compact).gsub(/^(.{20})(.{8})(.{4})$/){ $1+$3 }
                    end

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

    def method_missing meth
      (@subns ||= {})[meth] ||= self.class.new("#{@ns}.#{meth}", @client)
    end

  end
end