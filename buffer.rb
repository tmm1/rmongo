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
        when :long
          _read(4, 'N')
        when :longlong
          upper, lower = _read(8, 'NN')
          upper << 32 | lower
        when :cstring
          str = @data.unpack("@#{@pos}Z*").first
          @data.slice!(@pos, str.size+1)
          str
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
      when :long
        _write(data, 'N')
      when :longlong
        lower =  data & 0xffffffff
        upper = (data & ~0xffffffff) >> 32
        _write([upper, lower], 'NN')
      when :cstring
        _write(data)
        _write("\0")
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
      :int => 65536,0
      :long => 100_000_000,
      :longlong => 666_555_444_333_222_111,
      :cstring => 'hello',
      :bson => [
        { :num => 1                           },
        { :symbol => :abc                     },
        { :object => {}                       },
        { :array => [1, 2, 3]                 },
        { :string => 'abcdefg'                },
        { :_id => 'uuid'                      },
        { :_ns => 'namespace', :_id => 'uuid' },
        { :boolean => true                    },
        { :time => Time.at(Time.now.to_i)     },
        { :null => nil                        },
        { :regex => /^.*?/                    }
      ]
    }.each do |type, values|

      should "read and write a #{type}" do
        [*values].each do |value|
          @buf.write(type, value)
          @buf.rewind
          @buf.read(type).should == value
          @buf.should.be.empty
        end

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