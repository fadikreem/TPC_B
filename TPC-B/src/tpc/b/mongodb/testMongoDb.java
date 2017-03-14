package tpc.b.mongodb;
import java.sql.SQLException;

public class testMongoDb {
	
	/** benchmark time in seconds. According to TPC-B 2.0 rules it must be
	 * 15 minutes - 1 hour, but systems must be configured to store at least
	 * 8 hours of logs.
	 */
	final static int BENCHTIME=60*5;  //default 15*60
	final static int THREADS=5; // count of workers
	// Um die Anzahl der Test-Branches zu erh�hen ( 1 bedeutet 1 * 100000 Branches)
	final static int SCALE=1; // default 50 => 5 Mio Branches
	final static String schema = null;
	/**
	 * Benchmark entry point. You need to edit source code and choose driver and
	 * database JDBC URL.
	 */
	public static void main(String[] args) throws ClassNotFoundException, SQLException, InterruptedException {

		MongodbDriver mongoDriver;
		MongoDbBench mongoBench;
		MongoDbIntegrityTest t;
		int scale;
		boolean consistency=false;

		//Verbindung mit der Datenbank erstellen
		mongoDriver = new MongodbDriver();
		
		mongoDriver.setJDBCURL("localhost:27017");
		/* Configuration: Set username and password for connecting into database */
		//d.setUserCredentials("root", "1987");
		/* Configuration: Set database schema. null for default schema */
		mongoDriver.init(null);
		// Erstellung von leeren Mongo-Kollektionen
		mongoDriver.createEmptyTables(schema,true);
		t = new MongoDbIntegrityTest(mongoDriver);

		System.out.print("Old scale factor is ");
		System.out.flush();
		//siehe 3.2
		scale = t.verifyRecordLayoutsAndSizing(schema);
		System.out.println(scale+", wanted "+SCALE);
		if (scale > 0 && scale == SCALE) {
			System.out.print("Data consistent before test: ");
			System.out.flush();
			// �berpr�fung der Datenkonsistenz6
			consistency = t.checkDataConsistency(schema);
			System.out.println("Consistency: " + consistency);
			System.out.flush();
			/*
			 * we do not need to re - create inconsistent data because
			 * we are zeroing history and balances before test.
			 */
			/*
			if ( consistency == false )
				scale = 0;
			*/
		}
		//Fuegt Testdaten hinzu
		mongoDriver.prepareTestData(schema, scale, SCALE);
		
		//TPC-B Benchmark: Startet mehrere Worker gleichzeitig
		mongoBench = new MongoDbBench(mongoDriver, schema);
		// transactions = 0 : d.h. es wird Integer.MAX_VALUE Transaktionen pro Worker ausgefuehrt
		mongoBench.init(THREADS, 0, 1000L*BENCHTIME);
		System.out.println("Benchmarking "+mongoDriver.getName());
		mongoBench.run();
		System.out.println(mongoBench.gettps()+" tps, "+mongoBench.getGoodTransactions()+" good, "+mongoBench.getFailedTransactions()+" failed, 90 percentile "+mongoBench.get90percentile()+" s. tpsB="+mongoBench.gettpsB());

		// Nochmal wird die Datenkonsistenz nach dem Benchmark �berpr�ft
		System.out.println("\nTPC-B consistency tests:");
		System.out.println("Data scale factor is: " +t.verifyRecordLayoutsAndSizing(schema));
		System.out.println("Data are consistent: "+t.checkDataConsistency(schema));
		System.out.println("Data are still consistent after test transactions: "+t.checkDataConsistencyAfterTransactions(schema));
		
		// F�hrt die Integration-Tests aus : Transaktionsatomarit�t + Isolierung
		System.out.println("Transactions are atomic: "+t.checkTransactionAtomicity(schema));
		System.out.println("Transactions are isolated: "+ t.isolationTests(schema));
	}
}
