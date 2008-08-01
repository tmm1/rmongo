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
      # log 'send_data', data
      super
    end

    def unbind
      log 'disconnected'
    end

    # commands
    
    def find obj, &cb
      buf = Buffer.new
      
      # header
      buf.write :int, id = @id+=1
      buf.write :int, response = 0
      buf.write :int, operation = 2004

      # body
      buf.write :int, reserved = 0
      buf.write :cstring, 'default.test'
      buf.write :int, skip = 0
      buf.write :int, ret = 0

      # bson
      buf.write :bson, obj

      (@responses ||= {})[ @id ] = cb if cb

      callback{
        send_data [ buf.size + 4 ].pack('i')
        send_data buf.data
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
  end
  
end

EM.run{
  mongo = Mongo::Client.connect
  mongo.find({}) do |results|
    pp [:found, results]
  end
}