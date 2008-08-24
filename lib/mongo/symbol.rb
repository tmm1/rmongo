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