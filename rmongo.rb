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
      reserved, upper, lower, start, num = data.slice!(0,20).unpack('iNNii')
      cursor = upper << 32 | lower
      p [reserved, cursor, start, num]

      # bson
      seen = 0
      while seen < num
        len = *data.slice!(0,4).unpack('i')
        if len > 0
          while true
            case type = data.slice!(0,1).unpack('c').first
            when 0 # eoo
              p :eoo
              break

            when 1 # number
              str = ''
              chr = "\0"
              str += chr while chr = data.slice!(0,1).unpack('a').first and chr != "\0"
              name = str

              n = *data.slice!(0,8).unpack('d') # XXX why not in network byte order?
              p [:num, name, n]

            when 7 # oid
              str = ''
              chr = "\0"
              str += chr while chr = data.slice!(0,1).unpack('a').first and chr != "\0"
              name = str

              base, inc = data.slice!(0,12).unpack('Ni')

              p [:oid, name, base, inc]

            else
              p type
            end
          end
        end
        seen += 1
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