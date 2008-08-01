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
      log 'receive_data', data
      @buf << data

      # packet size
      size = @buf.read(:int)
      # p [size]

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
    
    def send_data data
      log 'send_data', data
      super
    end

    def unbind
      log 'disconnected'
    end

    # commands
    
    # to sort: { query : { ... } , orderby : { ... } }
    def find obj, &cb
      buf = Buffer.new
      
      # header
      buf.write :int, id = @id+=1
      buf.write :int, response = 0
      buf.write :int, operation = 2004

      # body
      buf.write :int,     reserved = 0
      buf.write :cstring, namespace = 'default.test'
      buf.write :int,     skip = 0
      buf.write :int,     ret = 0

      # bson
      buf.write :bson, obj

      (@responses ||= {})[ @id ] = cb if cb

      callback{
        send_data [ buf.size + 4 ].pack('i')
        send_data buf.data
      }
    end

    def insert obj
      buf = Buffer.new
      
      # header
      buf.write :int, id = @id+=1
      buf.write :int, response = 0
      buf.write :int, operation = 2002

      # body
      buf.write :int,     reserved = 0
      buf.write :cstring, namespace = 'default.test'

      # bson
      buf.write :bson, obj

      callback{
        send_data [ buf.size + 4 ].pack('i')
        send_data buf.data
      }
    end

    def remove obj
      buf = Buffer.new
      
      # header
      buf.write :int, id = @id+=1
      buf.write :int, response = 0
      buf.write :int, operation = 2006

      # body
      buf.write :int,     reserved = 0
      buf.write :cstring, namespace = 'default.test'
      buf.write :int,     0

      # bson
      buf.write :bson, obj

      callback{
        send_data [ buf.size + 4 ].pack('i')
        send_data buf.data
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
  end
  
end

EM.run{
  mongo = Mongo::Client.connect

  mongo.remove({})

  mongo.insert({ :n => 1, :_id => '4892ae52771f9ae3002d9cf5' })
  mongo.insert({ :n => 1, :_id => '4892ae52771f9ae3002d9cf6' })

  mongo.find({:n=>1}) do |results|
    pp [:found, results]
    puts
    EM.stop_event_loop
  end
}

__END__

["connected"]

["send_data", "*\000\000\000"]

["send_data",
 "\001\000\000\000\000\000\000\000\326\a\000\000\000\000\000\000default.test\000\000\000\000\000\005\000\000\000\000"]

["send_data", "B\000\000\000"]

["send_data",
 "\002\000\000\000\000\000\000\000\322\a\000\000\000\000\000\000default.test\000!\000\000\000\001n\000\000\000\000\000\000\000\360?\a_id\000\343\232\037wR\256\222H\365\234-\000\000"]

["send_data", "B\000\000\000"]

["send_data",
 "\003\000\000\000\000\000\000\000\322\a\000\000\000\000\000\000default.test\000!\000\000\000\001n\000\000\000\000\000\000\000\360?\a_id\000\343\232\037wR\256\222H\366\234-\000\000"]

["send_data", "9\000\000\000"]

["send_data",
 "\004\000\000\000\000\000\000\000\324\a\000\000\000\000\000\000default.test\000\000\000\000\000\000\000\000\000\020\000\000\000\001n\000\000\000\000\000\000\000\360?\000"]

["receive_data",
 "f\000\000\000\3056\027\245\004\000\000\000\001\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\002\000\000\000!\000\000\000\001n\000\000\000\000\000\000\000\360?\a_id\000\343\232\037wR\256\222H\365\234-\000\000!\000\000\000\001n\000\000\000\000\000\000\000\360?\a_id\000\343\232\037wR\256\222H\366\234-\000\000"]

[:found,
 [{:n=>1.0, :_id=>"4892ae52771f9ae3002d9cf5"},
  {:n=>1.0, :_id=>"4892ae52771f9ae3002d9cf6"}]]

["disconnected"]