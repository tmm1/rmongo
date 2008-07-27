#!/bin/sh
CLASSPATH=./bin/:~/code/10gen/appserver/build/; for i in ~/code/10gen/appserver/include/*.jar; do CLASSPATH=$CLASSPATH:$i; done; ED_HOME=~/code/10gen/appserver java -cp $CLASSPATH mongo.ruby.Mongo
