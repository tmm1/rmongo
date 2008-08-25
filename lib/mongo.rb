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

  should 'close the connection' do
    @mongo.client.close{
      @mongo.client.should.not.be.connected?
      done
    }
  end
  
end if __FILE__ == $0