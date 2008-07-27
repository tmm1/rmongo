package mongo.ruby;

import java.util.*;

import ed.js.*;
import ed.js.func.*;
import ed.js.engine.*;
import ed.db.*;

public class Mongo extends DBBase {

  private Map<String,DBCollection> _collections = new TreeMap<String,DBCollection>();

  public Mongo(){
    super("Mongo");
  }
  
  protected DBCollection doGetCollection( String name ){
    DBCollection c = _collections.get( name );
    if ( c == null ){
      c = new MongoCollection( this, name );
      _collections.put( name, c );
    }
    return c;
  }

  public DBCollection getCollectionFromFull( String fullNameSpace ){
    throw new RuntimeException( "not implemented" );
  }

  public Collection<String> getCollectionNames(){
    return Collections.unmodifiableSet( _collections.keySet() );
  }

  public String getConnectPoint(){
    return null;
  }
  
  class MongoCollection extends DBCollection {
    protected MongoCollection(DBBase base, String name) {
      super(base, name);
    }

    protected JSObject doSave(JSObject arg0) {
      return null;
    }

    protected void doapply(JSObject arg0) {
    }

    protected JSObject dofind(ObjectId arg0) {
      return null;
    }

    public void ensureIndex(JSObject arg0, String arg1) {
    }

    public Iterator<JSObject> find(JSObject arg0, JSObject arg1, int arg2, int arg3) {
      return null;
    }

    public int remove(JSObject arg0) {
      return 0;
    }

    public JSObject update(JSObject arg0, JSObject arg1, boolean arg2, boolean arg3) {
      return null;
    }
  }
  
  public static void main(String[] args) {
    Mongo db = new Mongo();
    DBCollection test = db.getCollection("test");
    test.
    System.out.println("done.");
  }

}