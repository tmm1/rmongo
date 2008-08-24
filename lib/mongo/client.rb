module Mongo
  module Client
    include EM::Deferrable

    def initialize opts = {}
      @settings = opts
      @id = 0
      @responses = {}

      timeout opts[:timeout] || 0.5
      errback{
        raise Error, 'could not connect to server'
      }
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

        # XXX put size back into the buffer!!
        break unless @buf.size >= size-4

        # header
        id, response, operation = @buf.read(:int, :int, :int)

        # body
        reserved, cursor, start, num = @buf.read(:int, :longlong, :int, :int)

        # bson results
        results = (1..num).map do
          @buf.read(:bson)
        end

        if cb = @responses.delete(response)
          cb.call(results)
        end

        # close if no more responses pending
        @on_close.succeed if @on_close and @responses.size == 0
      end
    end

    # def send_data data
    #   # log 'send_data', data
    #   super
    # end

    def unbind
      log 'disconnected'
    end

    # connection

    def close
      @on_close = EM::DefaultDeferrable.new
      @on_close.callback{
        close_connection
        yield if block_given?
      }
    end

    def send command_id, *args, &blk
      id = @id+=1

      # EM.next_tick do
        callback{
          buf = Buffer.new
          buf.write :int, id,
                    :int, response = 0,
                    :int, operation = command_id

          buf.write *args

          send_data [ buf.size + 4 ].pack('i')
          send_data buf.data
        }
      # end
      
      @responses[id] = blk if blk
      
      id
    end

    def self.connect opts = {}
      opts[:host] ||= '127.0.0.1'
      opts[:port] ||= 27017

      EM.connect(opts[:host], opts[:port], self, opts)
    end

    private

    def log *args
      return
      pp args
      puts
    end

  end
end