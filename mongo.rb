require 'pp'
require 'java'

Dir['/Users/aman/code/10gen/appserver/{build,include/*.jar}'].each{ |cp| $CLASSPATH << cp }
import 'ed.db.DBProvider'
import 'ed.js.JSObjectBase'

class Mongo
  class Results
    include Enumerable

    def initialize results
      @results = results
    end
    
    def each
      @results.each do |obj|
        yield(obj.keySet.inject({}) do |hash, key|
          hash.update key.to_sym => case val = obj.get(key)
                                    when Java::EdDb::ObjectId, Java::EdJs::JSString
                                      val.toString
                                    else
                                      val
                                    end
        end)
      end
    end
  end

  class Collection
    def initialize db, name
      @collection = db.getCollection(name.to_s)
    end
    attr_reader :collection

    def [] obj
      find(obj, nil, 0, 1).first
    end

    def method_missing meth, obj, *args
      case meth
      when :find, :save, :remove
        jobj = JSObjectBase.new
        obj.each do |key,val|
          jobj.set(key.to_s, val)
        end
        obj = jobj
      when :limit, :skip
      else
        super
      end

      ret = @collection.__send__(meth, obj, *args)

      if meth == :find
        Results.new(ret)
      else
        ret
      end
    end
  end
  
  def initialize name = 'default', opts = {}
    opts = { :host => 'localhost', :port => 27017 }.merge(opts)
    @db = DBProvider.get(name)
  end

  def method_missing meth
    @collections ||= {}
    @collections[meth] ||= Collection.new(@db, meth)
  end
end

db = Mongo.new
test = db.test

test.remove({})
test.save(:name => 'mongo',
          :time => Time.now.to_i,
          :from => 'ruby',
          :num => 123)
test.save(:from => 'ruby',
          :name => 'something')

pp test[:from => 'ruby']
pp test.find(:from => 'ruby').map

__END__

{:_id=>"488d23ab771f9a4600723d6c",
 :name=>"mongo",
 :time=>1217209259.0,
 :from=>"ruby",
 :num=>123.0,
 :_ns=>"test"}

[{:_id=>"488d23ab771f9a4600723d6c",
  :name=>"mongo",
  :time=>1217209259.0,
  :from=>"ruby",
  :num=>123.0,
  :_ns=>"test"},
 {:_id=>"488d23ab771f9a4600723d6d",
  :from=>"ruby",
  :name=>"something",
  :_ns=>"test"}]
