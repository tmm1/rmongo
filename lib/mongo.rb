require 'pp'
require 'ext/em'
require 'ext/fiber18'
require 'ext/emspec'

module Mongo
  class Error < Exception; end

  DIR = File.dirname File.expand_path(__FILE__)

  %w[ buffer symbol client collection ].each do |file|
    require DIR + '/mongo/' + file
  end

  def self.connect opts = {}
    Mongo::Client.connect opts
  end
end

def Mongo namespace, client = nil
  Mongo::Collection.new(namespace, client)
end

EM.describe Mongo do

  @mongo = Mongo('test')
  @db = @mongo.db
  
  should 'remove all objects' do
    @db.remove({})

    @db.find({}) do |res|
      res.should == []
      done
    end
  end

  should 'insert new objects' do
    obj = @db.insert :hello => 'world'

    obj.keys.should.include? :_id
    obj[:_id].should.be.kind_of? String
    obj[:_id].length.should == 24

    @db.first({}) do |ret|
      ret.should == obj
      done
    end
  end

  should 'insert complex objects' do
    obj = {
      :array => [1,2,3],
      :float => 123.456,
      :hash => {:boolean => true},
      :nil => nil,
      :symbol => :name,
      :string => 'hello world',
      :time => Time.at(Time.now.to_i),
      :regex => /abc$/ix
    }
    
    obj = @db.insert obj
    
    @db.find(:_id => obj[:_id]) do |ret|
      ret.should == [ obj ]
      done
    end
  end

  should 'find objects using nested properties' do
    @db.insert :name => 'Google',
               :address => {
                 :city => 'Mountain View',
                 :state => 'California'
               }

    @db.first(:'address.city' => 'Mountain View') do |res|
      res[:name].should == 'Google'
      done
    end
  end
  
  @numbers = @mongo.numbers
  @numbers.remove({})

  { 1 => 'one',
    2 => 'two',
    3 => 'three',
    4 => 'four',
    5 => 'five',
    6 => 'six',
    7 => 'seven',
    8 => 'eight',
    9 => 'nine' }.each do |num, word|
                    @numbers.insert :num => num, :word => word
                  end
  
  should 'find objects with specific values' do
    @numbers.find :num.in([1,3,5]) do |res|
      res.size.should == 3
      res.map{|r| r[:num] }.sort.should == [1,3,5]
      done
    end
  end

  should 'add indexes' do
    @numbers.index :num
    @numbers.indexes do |res|
      res.first[:name].should == 'num'
      res.size.should == 1
      done
    end
  end
  
  should 'return sorted results' do
    @numbers.find({}, :num.desc) do |res|
      res.first[:num].should == 9
      res.last[:num].should  == 1
      res.size.should == 9
      done
    end
  end

  should 'limit returned results' do
    @numbers.find({}, :num.asc, :limit => 1) do |res|
      res.first[:num].should == 1
      res.size.should == 1
      done
    end
  end

  should 'skip rows with limit' do
    @numbers.find({}, :num.asc, :limit => 1, :skip => 1) do |res|
      res.first[:num].should == 2
      res.size.should == 1
      done
    end
  end

  should 'find ranges' do
    @numbers.find(:num > 2, :num < 4) do |res|
      res.first[:num].should == 3
      res.size.should == 1
      done
    end
  end
  
  @editors = @mongo.editors
  @editors.remove({})
  @editors.index :platform

  [ { :name => :textmate, :platform => [:osx] },
    { :name => :vim,      :platform => [:osx, :linux] },
    { :name => :eclipse,  :platform => [:osx, :linux, :windows] },
    { :name => :notepad,  :platform => [:windows] }
  ].each do |obj|
    @editors.insert obj
  end

  should 'find objects with given tag' do
    @editors.find(:platform => :osx) do |res|
      res.size.should == 3
      res.map{|r| r[:name].to_s }.sort.should == %w[ eclipse textmate vim ]
      done
    end
  end
  
end

__END__

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

  # in queries
  mongo.find :n.in([ 1,3,5 ]) do |results|
    
  end

  # sorting (sorting is faster with an index)
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

  mongo.find(:platform.in [:linux, :osx]) do |results|
    
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

FIND {:n=>{:$in=>[1, 3, 5]}} =>

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