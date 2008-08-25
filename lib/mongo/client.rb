module Mongo
  module Client
    include EM::Deferrable

    def initialize opts = {}
      @settings = opts
      @id = 0
      @responses = {}
      @connected = false

      @on_close = proc{
        raise Error, "could not connect to server #{opts[:host]}:#{opts[:port]}"
      }

      timeout opts[:timeout] if opts[:timeout]
      errback{ @on_close.call }
    end

    def connected?() @connected end

    # EM hooks

    def connection_completed
      log 'connected'
      @buf = Buffer.new
      @connected = true
      @on_close = proc{
        raise Error, 'disconnected from server'
      }
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
        close_connection if @close_pending and @responses.size == 0
      end
    end

    # def send_data data
    #   # log 'send_data', data
    #   super
    # end

    def unbind
      log 'disconnected'
      @connected = false
      @on_close.call unless $!
    end

    # connection

    def close
      @on_close = proc{ yield if block_given? }
      if @responses.empty?
        close_connection
      else
        @close_pending = true
      end
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