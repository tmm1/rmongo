if [].map.respond_to? :with_index
  class Array
    def enum_with_index
      each.with_index
    end
  end
else
  require 'enumerator'
end

module Mongo
  class Buffer
    class Overflow    < Exception; end
    class InvalidType < Exception; end
    
    def initialize data = ''
      @data = data
      @pos = 0
    end
    attr_reader :pos
    
    def data
      @data.clone
    end
    alias :contents :data
    alias :to_s :data

    def << data
      @data << data.to_s
      self
    end
    
    def length
      @data.length
    end
    alias :size :length
    
    def empty?
      pos == length
    end
    
    def rewind
      @pos = 0
    end
    
    def read *types
      values = types.map do |type|
        case type
        when :byte
          _read(1, 'C')
        when :short
          _read(2, 'n')
        when :int
          _read(4, 'i')
        when :double
          _read(8, 'd')
        when :long
          _read(4, 'N')
        when :longlong
          upper, lower = _read(8, 'NN')
          upper << 32 | lower
        when :cstring
          str = @data.unpack("@#{@pos}Z*").first
          @data.slice!(@pos, str.size+1)
          str
        when :bson
          bson = {}
          data = Buffer.new _read(read(:int)-4)
          
          until data.empty?
            type = data.read(:byte)
            next if type == 0 # end of object

            key = data.read(:cstring).intern

            bson[key] = case type
                        when 1 # number
                          data.read(:double)
                        when 2 # string
                          data.read(:cstring)
                        when 3 # object
                          data.read(:bson)
                        when 4 # array
                          data.read(:bson).inject([]){ |a, (k,v)| a[k.to_s.to_i] = v; a }
                        when 8 # bool
                          data.read(:byte) == 1 ? true : false
                        when 10 # nil
                          nil
                        end
          end
          
          bson
        else
          raise InvalidType, "Cannot read data of type #{type}"
        end
      end
      
      types.size == 1 ? values.first : values
    end
    
    def write type, data
      case type
      when :byte
        _write(data, 'C')
      when :short
        _write(data, 'n')
      when :int
        _write(data, 'i')
      when :double
        _write(data, 'd')
      when :long
        _write(data, 'N')
      when :longlong
        lower =  data & 0xffffffff
        upper = (data & ~0xffffffff) >> 32
        _write([upper, lower], 'NN')
      when :cstring
        _write(data.to_s + "\0")
      when :bson
        buf = Buffer.new
        data.each do |key,value|
          case value
          when Numeric
            buf.write(:byte, 1)
            buf.write(:cstring, key)
            buf.write(:double, value)
          when String
            buf.write(:byte, 2)
            buf.write(:cstring, key)
            buf.write(:cstring, value)
          when Hash
            buf.write(:byte, 3)
            buf.write(:cstring, key)
            buf.write(:bson, value)
          when Array
            buf.write(:byte, 4)
            buf.write(:cstring, key)
            buf.write(:bson, value.enum_with_index.inject({}){ |h, (v, i)| h.update i => v })
          when TrueClass, FalseClass
            buf.write(:byte, 8)
            buf.write(:cstring, key)
            buf.write(:byte, value ? 1 : 0)
          when NilClass
            buf.write(:byte, 10)
            buf.write(:cstring, key)
          end
        end

        write(:int, buf.size+5)
        _write(buf.to_s)
        write(:byte, 0)
      else
        raise InvalidType, "Cannot write data of type #{type}"
      end
      
      self
    end

    def _read size, pack = nil
      if @pos + size > length
        raise Overflow
      else
        data = @data[@pos,size]
        @data[@pos,size] = ''
        if pack
          data = data.unpack(pack)
          data = data.pop if data.size == 1
        end
        data
      end
    end
    
    def _write data, pack = nil
      data = [*data].pack(pack) if pack
      @data[@pos,0] = data
      @pos += data.length
    end

    def extract
      begin
        cur_data, cur_pos = @data.clone, @pos
        yield self
      rescue Overflow
        @data, @pos = cur_data, cur_pos
        nil
      end
    end
  end
end

if $0 =~ /bacon/ or $0 == __FILE__
  require 'bacon'
  include Mongo

  describe Buffer do
    before do
      @buf = Buffer.new
    end

    should 'have contents' do
      @buf.contents.should == ''
    end

    should 'initialize with data' do
      @buf = Buffer.new('abc')
      @buf.contents.should == 'abc'
    end

    should 'append raw data' do
      @buf << 'abc'
      @buf << 'def'
      @buf.contents.should == 'abcdef'
    end

    should 'append other buffers' do
      @buf << Buffer.new('abc')
      @buf.data.should == 'abc'
    end

    should 'have a position' do
      @buf.pos.should == 0
    end

    should 'have a length' do
      @buf.length.should == 0
      @buf << 'abc'
      @buf.length.should == 3
    end

    should 'know the end' do
      @buf.empty?.should == true
    end

    should 'read and write data' do
      @buf._write('abc')
      @buf.rewind
      @buf._read(2).should == 'ab'
      @buf._read(1).should == 'c'
    end

    should 'raise on overflow' do
      lambda{ @buf._read(1) }.should.raise Buffer::Overflow
    end

    should 'raise on invalid types' do
      lambda{ @buf.read(:junk) }.should.raise Buffer::InvalidType
      lambda{ @buf.write(:junk, 1) }.should.raise Buffer::InvalidType
    end
  
    { :byte => 0b10101010,
      :short => 100,
      :int => 65536,
      :double => 123.456,
      :long => 100_000_000,
      :longlong => 666_555_444_333_222_111,
      :cstring => 'hello',
    }.each do |type, value|

      should "read and write a #{type}" do
        @buf.write(type, value)
        @buf.rewind
        @buf.read(type).should == value
        @buf.should.be.empty
      end

    end
    
    [
      { :num => 1                       },
      # { :symbol => :abc                 },
      { :object => {}                   },
      { :array => [1, 2, 3]             },
      { :string => 'abcdefg'            },
      # { :oid => { :_id => 'uuid' }      },
      # { :ref => { :_ns => 'namespace',
      #             :_id => 'uuid' }      },
      { :boolean => true                },
      # { :time => Time.at(Time.now.to_i) },
      { :null => nil                    },
      # { :regex => /^.*?/                }
    ]. each do |bson|

      should "read and write bson with #{bson.keys.first}s" do
        @buf.write(:bson, bson)
        @buf.rewind
        @buf.read(:bson).should == bson
        @buf.should.be.empty
      end

    end

    should 'do transactional reads with #extract' do
      @buf.write :byte, 8
      orig = @buf.to_s

      @buf.rewind
      @buf.extract do |b|
        b.read :byte
        b.read :short
      end

      @buf.pos.should == 0
      @buf.data.should == orig
    end
  end
end