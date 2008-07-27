package mongo.ruby;

import ed.db.*;
import ed.js.engine.Scope;

public class RMongo {

	public RMongo() {
	}

	private static DBBase db;

	public static void main(String[] args) {
    Scope s = Scope.getThreadLocal();
    Object dbo = s.get("db");

    if(! (dbo instanceof DBBase)) {
      throw new RuntimeException("your database is not a database");
    }

    db = (DBBase)dbo;
		System.out.println("done.");
	}

}
