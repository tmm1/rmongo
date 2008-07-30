require 'rubygems'
require 'eventmachine'

class Mongo

  module Client
    include EM::Deferrable

    def initialize opts = {}
      @settings = opts
      @id = 0
    end

    # EM hooks

    def connection_completed
      log 'connected'
      succeed
    end

    def receive_data data
      log 'receive_data', data
      
      # packet size
      size = *data.slice!(0,4).unpack('i')
      p [size]

      # header
      id, response, operation = data.slice!(0,12).unpack('i3')
      p [id, response, operation]
      
      # body
      reserved, cursor, start, num = data.slice!(0,16).unpack('iNii')
      p [reserved, cursor, start, num]
      
      # bson
      len = *data.slice!(0,4).unpack('i')
      p [len, data]
    end
    
    def send_data data
      log 'send_data', data
      super
    end

    def unbind
      log 'disconnected'
    end

    # commands
    
    def find
      size =   0
      header = [ id = @id+=1, response = 0, operation = 2004 ].pack('i3')
      body =   [ 0, ns = 'default.test', eos = 0, skip = 0, ret = 0 ].pack('ia*cii')
      bson =   [ length = 5, eoo = 0 ].pack('ic')

      callback{
        send_data [ size=(header+body+bson).size+4, header, body, bson ].pack('ia*a*a*')
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
  mongo.find
}