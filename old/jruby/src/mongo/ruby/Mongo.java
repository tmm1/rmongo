package mongo.ruby;

import java.util.*;
import ed.js.*;
import ed.db.*;

public class Mongo {
  public Mongo(){
    DBApiLayer db = DBProvider.get("mongo");
    DBCollection test = db.getCollection("test");

    JSObjectBase remove = new JSObjectBase();
    test.remove(remove);
    
    JSObjectBase obj = new JSObjectBase();
    obj.set("name", "mongo");
    obj.set("value", 123);
    test.save(obj);

    JSObjectBase query = new JSObjectBase();
    query.set("name", "mongo");
    for (Iterator<JSObject> it = test.find(query); it.hasNext(); ){
      JSObject o = it.next();
      System.out.println("got result _id: "   + o.get("_id").toString());
      System.out.println("got result _ns: "   + o.get("_ns").toString());
      System.out.println("got result name: "  + o.get("name").toString());
      System.out.println("got result value: " + o.get("value").toString());
    }
  }

  public static void main(String[] args) {
    System.out.println("starting..");
    new Mongo();
    System.out.println("done.");
  }

}

/*

  $ cat 10gen.properties 
  BASE=~/code/10gen/appserver/libraries

  $ cat run.sh 
  #!/bin/sh
  CLASSPATH=./bin/:~/code/10gen/appserver/build/; for i in ~/code/10gen/appserver/include/*.jar; do CLASSPATH=$CLASSPATH:$i; done; ED_HOME=~/code/10gen/appserver java -cp $CLASSPATH mongo.ruby.Mongo

  $ ./run.sh
  starting..
  loading config file from [./10gen.properties]
  DBApiLayer : DBTCP : 127.0.0.1:27017/mongo
  Warning : can't find core appserver js sources : no harm, but js will be recompiled on appserver startup
  got result _id: 488c4d40771f9ab200645c9d
  got result name: mongo
  got result _id: 488c4d4f771f9af10025c852
  got result name: mongo
  got result _id: 488c4fd4771f9ac9006002b5
  got result name: mongo
  got result _id: 488c4fdd771f9aee00d0fe71
  got result name: mongo
  done.

*/