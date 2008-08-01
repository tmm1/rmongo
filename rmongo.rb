require 'rubygems'
require 'eventmachine'
require 'buffer'
require 'pp'

module Mongo

  module Client
    include EM::Deferrable

    def initialize opts = {}
      @settings = opts
      @id = 0
    end

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
        # p [size]
        
        break unless @buf.size >= size-4

        # header
        id, response, operation = @buf.read(:int, :int, :int)
        # p [id, response, operation]
      
        # body
        reserved, cursor, start, num = @buf.read(:int, :longlong, :int, :int)
        # p [reserved, cursor, start, num]

        # bson results
        results = (1..num).map do
          @buf.read(:bson)
        end
        # pp results
      
        if cb = @responses.delete(response)
          cb.call(results)
        end
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
    def find obj, &cb
      data = send(2004) do |buf|
        # body
        buf.write :int,     reserved = 0,
                  :cstring, namespace = 'default.test',
                  :int,     skip = 0,
                  :int,     ret = 0

        # bson
        buf.write :bson, obj
      end

      (@responses ||= {})[ @id ] = cb if cb

      callback{
        send_data [ data.size + 4 ].pack('i')
        send_data data
      }
    end

    def insert obj
      data = send(2002) do |buf|
        # body
        buf.write :int,     reserved = 0,
                  :cstring, namespace = 'default.test'
        # bson
        buf.write :bson, obj
      end

      callback{
        send_data [ data.size + 4 ].pack('i')
        send_data data
      }
    end

    def remove obj
      data = send(2006) do |buf|
        # body
        buf.write :int,     reserved = 0,
                  :cstring, namespace = 'default.test',
                  :int,     0

        # bson
        buf.write :bson, obj
      end

      callback{
        send_data [ data.size + 4 ].pack('i')
        send_data data
      }
    end

    # connection
    
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
      buf.data
    end
  end
  
end

EM.run{
  mongo = Mongo::Client.connect

  mongo.remove({})

  mongo.insert({ :n => 1, :_id => '4892ae52771f9ae3002d9cf5' })
  mongo.insert({ :n => 2, :_id => '4892ae52771f9ae3002d9cf6' })
  mongo.insert({ :n => 3, :_id => '4892ae52771f9ae3002d9cf7' })

  mongo.find({ :_id => '4892ae52771f9ae3002d9cf6' }) do |results|
    pp [:found, results]
  end

  mongo.find({ :n => { :$gt => 1 } }) do |results|
    pp [:found, results]
    puts
    EM.stop_event_loop
  end
}

__END__

["connected"]

[:found, [{:n=>2.0, :_id=>"4892ae52771f9ae3002d9cf6"}]]
[:found,
 [{:n=>2.0, :_id=>"4892ae52771f9ae3002d9cf6"},
  {:n=>3.0, :_id=>"4892ae52771f9ae3002d9cf7"}]]

["disconnected"]