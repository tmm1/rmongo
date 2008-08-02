require 'rubygems'
require 'eventmachine'
require 'buffer'

module Mongo
  class Error < Exception; end

  module Client
    include EM::Deferrable

    def initialize opts = {}
      @settings = opts
      @id = 0
      @responses = {}
      @namespace = 'default.test'
      timeout 0.5
      errback{
        raise Error, 'could not connect to server'
      }
    end

    def namespace ns = nil
      begin
        old_ns = @namespace
        @namespace = ns
        yield
      ensure
        @namespace = old_ns
      end if ns

      @namespace
    end
    attr_writer :namespace

    # EM hooks

    def connection_completed
      log 'connected'
      @buf = Buffer.new
      succeed
    end

    def receive_data data
      # log 'receive_data', data
      @buf << data

      until @buf.empty?
        # packet size
        size = @buf.read(:int)
        # log :size => size
        
        break unless @buf.size >= size-4

        # header
        id, response, operation = @buf.read(:int, :int, :int)
        # log :id => id, :response => response, :operation => operation
      
        # body
        reserved, cursor, start, num = @buf.read(:int, :longlong, :int, :int)
        # log :reserved => reserved, :cursor => cursor, :start => start, :num => num

        # bson results
        results = (1..num).map do
          @buf.read(:bson)
        end
        # log :results => results
      
        if cb = @responses.delete(response)
          cb.call(results)
        end

        # close if no more responses pending
        @on_close.succeed if @on_close and @responses.size == 0
      end
    end
    
    def send_data data
      # log 'send_data', data
      super
    end

    def unbind
      log 'disconnected'
    end

    # commands
    
    # to sort: { query : { ... } , orderby : { ... } }
    def find obj, orderby = nil, &cb
      obj = { :query => obj,
              :orderby => orderby } if orderby

      send(2004) do |buf|
        # body
        buf.write :int,     reserved = 0,
                  :cstring, @namespace,
                  :int,     skip = 0,
                  :int,     ret = 0

        # bson
        buf.write :bson, obj
      end

      (@responses ||= {})[ @id ] = cb if cb
    end

    def insert obj
      send(2002) do |buf|
        # body
        buf.write :int,     reserved = 0,
                  :cstring, @namespace
        # bson
        buf.write :bson, obj

        # log :execute_insert, obj
      end
    end

    def remove obj
      send(2006) do |buf|
        # body
        buf.write :int,     reserved = 0,
                  :cstring, @namespace,
                  :int,     0

        # bson
        buf.write :bson, obj

        # log :execute_remove, obj
      end
    end

    # connection
    
    def close
      @on_close = EM::DefaultDeferrable.new
      @on_close.callback{
        close_connection
        yield if block_given?
      }
    end
    
    def self.connect opts = {}
      opts[:host] ||= '127.0.0.1'
      opts[:port] ||= 27017

      EM.connect(opts[:host], opts[:port], self, opts)
    end

    private
  
    def log *args
      require 'pp'
      pp args
      puts
    end
    
    def send(command_id, &cb)
      buf = Buffer.new
      buf.write :int, id = @id+=1,
                :int, response = 0,
                :int, operation = command_id
      yield buf
      callback{
        send_data [ buf.size + 4 ].pack('i')
        send_data buf.data
      }
    end
  end
  
end

# XXX this is terrible, yes i know!
class Symbol

  {
    :<  => :$lt,
    :<= => :$lte,
    :>  => :$gt,
    :>= => :$gte
  }.each do |func, key|
    class_eval %[
      def #{func} num
        { self => { :#{key} => num }}
      end
    ]
  end
end

EM.run{
  def log *args
    require 'pp'
    pp args
    puts
  end

  mongo = Mongo::Client.connect

  log 'remove all objects in the database'
  mongo.remove({})

  log 'insert a complex object'
  mongo.insert :_id => '4892ae52771f9ae3002d9cf4',
               :array => [1,2,3],
               :float => 123.456,
               :hash => {:boolean => true},
               :nil => nil,
               :symbol => :name,
               :string => 'hello world'

  mongo.find({}) do |results|
    log 'all objects', :found, results
  end

  # log 'insert some new objects'
  # mongo.insert(:n => 1, :tags => ['ruby', 'js'], :_id => '4892ae52771f9ae3002d9cf5')
  # mongo.insert(:n => 2, :tags => ['js', 'java'], :_id => '4892ae52771f9ae3002d9cf6')
  # mongo.insert(:n => 3, :tags => ['java', 'c#'], :_id => '4892ae52771f9ae3002d9cf7')

  # log 'add index on n'
  # mongo.namespace = 'default.system.indexes'
  # mongo.insert(:name => 'n', :ns => 'default.test', :key => { :n => -1 })
  # mongo.namespace = 'default.test'

  # mongo.find({ :tags => 'ruby' }) do |results|
  #   log 'objects tagged with ruby', :found, results
  # end

  # mongo.find({}, :n => -1) do |results|
  #   log 'all objects, sorted by n desc', :found, results
  # end

  # mongo.find({ :_id => '4892ae52771f9ae3002d9cf6' }) do |results|
  #   log 'object with specific id', :found, results
  # end

  # mongo.find(:n >= 1) do |results|
  #   log 'objects where n >= 1', :found, results
  # end
  
  mongo.close{ EM.stop_event_loop }
}

__END__

["connected"]

[:found, [{:n=>2.0, :_id=>"4892ae52771f9ae3002d9cf6"}]]
[:found,
 [{:n=>2.0, :_id=>"4892ae52771f9ae3002d9cf6"},
  {:n=>3.0, :_id=>"4892ae52771f9ae3002d9cf7"}]]

["disconnected"]