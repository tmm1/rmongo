require 'rubygems'
require 'eventmachine'
require 'buffer'
require 'pp'

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
      callback{
        begin
          old_ns = @namespace
          @namespace = ns
          yield
        ensure
          @namespace = old_ns
        end if ns
      }

      @namespace
    end

    def namespace= ns
      callback{
        @namespace = ns
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
    def find obj, orderby = nil
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

        # proc to call with response
        @responses[@id] = proc{ |res|
          puts "#{'-'*80}\n"
          puts "\nFIND #{obj.inspect} =>\n\n"
          pp res
          puts
          yield(res)
        } if block_given?
      end
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
      pp args
      puts
    end
    
    def send command_id
      # EM.next_tick do
        callback{
          buf = Buffer.new
          buf.write :int, id = @id+=1,
                    :int, response = 0,
                    :int, operation = command_id

          yield buf

          send_data [ buf.size + 4 ].pack('i')
          send_data buf.data
        }
      # end
    end
  end
  
end

# XXX this is terrible, yes i know!
class Symbol

  { :<  => :$lt,
    :<= => :$lte,
    :>  => :$gt,
    :>= => :$gte }.each do |func, key|
                     class_eval %[
                       def #{func} num
                         { self => { :#{key} => num }}
                       end
                     ]
                   end

  def in values
    { self => { :$in => values }}
  end

  def asc
    { self => 1 }
  end
  
  def desc
    { self => -1 }
  end

end

EM.run{
  # connect to mongo
  mongo = Mongo::Client.connect :port => 27017

  # remove all objects in the database
  mongo.remove({})

  # insert a simple object with a string
  mongo.insert :_id => '000000000000000000000001',
               :hello => 'world'
  
  # find all objects
  mongo.find({}) do |results|
    
  end

  # find specific object
  mongo.find(:_id => '000000000000000000000001') do |results|
    
  end

  # insert complex object
  mongo.insert :_id => '000000000000000000000002',
               :array => [1,2,3],
               :float => 123.456,
               :hash => {:boolean => true},
               :nil => nil,
               :symbol => :name,
               :string => 'hello world',
               :time => Time.now,
               :regex => /abc$/ix

  # find all objects
  mongo.find({}) do |results|
    
  end

  # query nested properties
  mongo.find(:'hash.boolean' => true) do |results|
    
  end

  # insert test data
  mongo.insert(:_id => '000000000000000000000010', :n => 1, :array  => [1,2,3])
  mongo.insert(:_id => '000000000000000000000011', :n => 2, :string => 'ruby and js')
  mongo.insert(:_id => '000000000000000000000012', :n => 3, :number => 112233.445566)
  mongo.insert(:_id => '000000000000000000000013', :n => 4, :null => nil)
  mongo.insert(:_id => '000000000000000000000014', :n => 5, :object => { :boolean => true })
  mongo.insert(:_id => '000000000000000000000015', :n => 6, :symbol => :Constant)

  # add index on n
  mongo.namespace('default.system.indexes') do
    mongo.insert(:name => 'n', :ns => 'default.test', :key => { :n => true })
  end

  # simple searches
  mongo.find(:n > 1) do |results|
    
  end

  # in queries # XXX why does this match objects that have no value for n
  mongo.find :n.in([ 1,3,5 ]) do |results|
    
  end

  # sorting # XXX requires an index to sort
  mongo.find({}, :n.desc) do |results|
    
  end

  # switch to editors namespace
  mongo.namespace = 'default.editors'
  
  # delete all rows
  mongo.remove({})
  
  # insert editors with platforms supported tags
  mongo.insert(:_id => '000000000000000000000101', :name => :textmate, :platform => [:osx])
  mongo.insert(:_id => '000000000000000000000102', :name => :vim,      :platform => [:osx, :linux])
  mongo.insert(:_id => '000000000000000000000103', :name => :eclipse,  :platform => [:osx, :linux, :windows])
  mongo.insert(:_id => '000000000000000000000104', :name => :notepad,  :platform => [:windows])
  
  # find all editors
  mongo.find({}) do |results|
    
  end

  # add multikey index on platforms property
  mongo.namespace('default.system.indexes') do
    mongo.insert(:name => 'platforms', :ns => 'default.editors', :key => { :platform => true })
  end

  # find objects with linux tag # XXX how can i find an object with two tags?
  mongo.find(:platform => :linux) do |results|
    
  end

  # close the connection and stop the reactor
  mongo.close{ EM.stop_event_loop }
}

__END__

["connected"]

--------------------------------------------------------------------------------

FIND {} =>

[{:_id=>"000000000000000000000001", :hello=>"world"}]

--------------------------------------------------------------------------------

FIND {:_id=>"000000000000000000000001"} =>

[{:_id=>"000000000000000000000001", :hello=>"world"}]

--------------------------------------------------------------------------------

FIND {} =>

[{:_id=>"000000000000000000000001", :hello=>"world"},
 {:_id=>"000000000000000000000002",
  :hash=>{:boolean=>true},
  :regex=>/abc$/ix,
  :float=>123.456,
  :symbol=>:name,
  :array=>[1.0, 2.0, 3.0],
  :nil=>nil,
  :string=>"hello world",
  :time=>Sat Aug 02 02:08:27 -0700 2008}]

--------------------------------------------------------------------------------

FIND {:"hash.boolean"=>true} =>

[{:_id=>"000000000000000000000002",
  :hash=>{:boolean=>true},
  :regex=>/abc$/ix,
  :float=>123.456,
  :symbol=>:name,
  :array=>[1.0, 2.0, 3.0],
  :nil=>nil,
  :string=>"hello world",
  :time=>Sat Aug 02 02:08:27 -0700 2008}]

--------------------------------------------------------------------------------

FIND {:n=>{:$gt=>1}} =>

[{:_id=>"000000000000000000000011", :n=>2.0, :string=>"ruby and js"},
 {:_id=>"000000000000000000000012", :number=>112233.445566, :n=>3.0},
 {:_id=>"000000000000000000000013", :n=>4.0, :null=>nil},
 {:_id=>"000000000000000000000014", :n=>5.0, :object=>{:boolean=>true}},
 {:_id=>"000000000000000000000015", :adf=>"123", :n=>6.0}]

--------------------------------------------------------------------------------

FIND {:n=>{:$in=>[1, 3, 5]}} => # XXX why does this match objects that have no value for n

[{:_id=>"000000000000000000000001", :hello=>"world"},
 {:_id=>"000000000000000000000002",
  :hash=>{:boolean=>true},
  :regex=>/abc$/ix,
  :float=>123.456,
  :symbol=>:name,
  :array=>[1.0, 2.0, 3.0],
  :nil=>nil,
  :string=>"hello world",
  :time=>Sat Aug 02 02:08:27 -0700 2008},
 {:_id=>"000000000000000000000010", :array=>[1.0, 2.0, 3.0], :n=>1.0},
 {:_id=>"000000000000000000000012", :number=>112233.445566, :n=>3.0},
 {:_id=>"000000000000000000000014", :n=>5.0, :object=>{:boolean=>true}}]

--------------------------------------------------------------------------------

FIND {:orderby=>{:n=>-1}, :query=>{}} =>

[{:symbol=>:Constant, :n=>6.0, :_id=>"000000000000000000000015"},
 {:object=>{:boolean=>true}, :n=>5.0, :_id=>"000000000000000000000014"},
 {:null=>nil, :n=>4.0, :_id=>"000000000000000000000013"},
 {:number=>112233.445566, :n=>3.0, :_id=>"000000000000000000000012"},
 {:n=>2.0, :_id=>"000000000000000000000011", :string=>"ruby and js"},
 {:array=>[1.0, 2.0, 3.0], :n=>1.0, :_id=>"000000000000000000000010"}]

--------------------------------------------------------------------------------

FIND {} =>

[{:name=>:textmate, :_id=>"000000000000000000000101", :platform=>[:osx]},
 {:name=>:vim, :_id=>"000000000000000000000102", :platform=>[:osx, :linux]},
 {:name=>:eclipse,
  :_id=>"000000000000000000000103",
  :platform=>[:osx, :linux, :windows]},
 {:name=>:notepad, :_id=>"000000000000000000000104", :platform=>[:windows]}]

--------------------------------------------------------------------------------

FIND {:platform=>:linux} =>

[{:_id=>"000000000000000000000102", :name=>:vim, :platform=>[:osx, :linux]},
 {:_id=>"000000000000000000000103",
  :name=>:eclipse,
  :platform=>[:osx, :linux, :windows]}]

--------------------------------------------------------------------------------

["disconnected"]