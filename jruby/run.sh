#!/bin/sh
export TENGEN_APPSERVER=`expr "$(cat 10gen.properties)" : 'BASE=\(.*\)\/libraries'`
echo TENGEN_APPSERVER=$TENGEN_APPSERVER

TEMP_CLASSPATH=./bin/:$TENGEN_APPSERVER/build/
for i in $TENGEN_APPSERVER/include/*.jar; do
  export TEMP_CLASSPATH=${TEMP_CLASSPATH}:${i}
done
echo TEMP_CLASSPATH=$TEMP_CLASSPATH

export ED_HOME=$TENGEN_APPSERVER
echo ED_HOME=$TENGEN_APPSERVER

echo Ready to run mongo.rb
#javac -d bin src/mongo/ruby/Mongo.java
#java mongo.ruby.Mongo
CLASSPATH=$TEMP_CLASSPATH jruby mongo.rb

echo Done.
