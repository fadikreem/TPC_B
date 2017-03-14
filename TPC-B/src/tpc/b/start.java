package tpc.b;
/*
Copyright (c) 2010, Radim Kolar
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

 * Redistributions of source code must retain the above copyright notice,
   this list of conditions and the following disclaimer.
 * Redistributions in binary form must reproduce the above copyright
   notice, this list of conditions and the following disclaimer in the
   documentation and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE AUTHOR AND CONTRIBUTORS ``AS IS'' AND ANY
EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE AUTHOR OR CONTRIBUTORS BE LIABLE FOR ANY
DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH
DAMAGE.
*/

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;


public class start {

	static {
		List <String> l = new ArrayList<String>();
//		l.add("com.ibm.db2.jcc.DB2Driver");
//		l.add("org.postgresql.Driver");
//		l.add("com.mysql.jdbc.Driver");
//		l.add("oracle.jdbc.driver.OracleDriver");
//		l.add("org.apache.derby.jdbc.EmbeddedDriver");
		l.add("jdbc:mysql://localhost/tpc_b_test");
		for(String c:l)
			try {
				Class.forName(c);
			}
			catch (ClassNotFoundException e) {	}
	}

	/** benchmark time in seconds. According to TPC-B 2.0 rules it must be
	 * 15 minutes - 1 hour, but systems must be configured to store at least
	 * 8 hours of logs.
	 */
	final static int BENCHTIME=60;  //default 15*60
	final static int THREADS=5;
	final static int SCALE=1; // default 50

	/**
	 * Benchmark entry point. You need to edit source code and choose driver and
	 * database JDBC URL.
	 */
	public static void main(String[] args) throws ClassNotFoundException, SQLException, InterruptedException {

		driver d;
		bench b;
		integrityTests t;
		int scale;
		boolean consistency=false;

		/* Configuration: Choose your JDBC driver */
		// Generic JDBC driver
		d = new driver();

		// IBM DB2 LUW driver
		// d = new db2driver();

		// PostgreSQL driver
		//d = new pgsqldriver();

		// MySQL driver
		 d = new mysqldriver();

		// Oracle driver
		// d = new oracledriver();

		// Apache Derby driver
		// d = new derbydriver();
		
		// MySQL driver
		//d = new raimadriver();

		/* Configuration: Select JDBC driver URL */
		// d.setJDBCURL("jdbc:db2:sample");
		//d.setJDBCURL("jdbc:postgresql:postgres?prepareThreshold=1");
		 d.setJDBCURL("jdbc:mysql://localhost/tpc_b_test");
		// d.setJDBCURL("jdbc:oracle:thin:@///XE");
		// d.setJDBCURL("jdbc:derby:/derby/tpcb;create=true");
		 //d.setJDBCURL("jdbc:raima:rdm://local");

		/* Configuration: Set username and password for connecting into database */
		d.setUserCredentials("root", "1987");
		/* Configuration: Set database schema. null for default schema */
		d.init(null);

		d.createEmptyTables(null,true);
		t = new integrityTests(d);

		System.out.print("Old scale factor is ");
		System.out.flush();
		scale = t.verifyRecordLayoutsAndSizing(null);
		System.out.println(scale+", wanted "+SCALE);
		if (scale > 0 && scale == SCALE) {
			System.out.print("Data consistent before test: ");
			System.out.flush();
			consistency = t.checkDataConsistency(null);
			System.out.println(consistency);
			/*
			 * we do not need to re - create inconsistent data because
			 * we are zeroing history and balances before test.
			 */
			/*
			if ( consistency == false )
				scale = 0;
			*/
		}
		d.prepareTestData(null, scale, SCALE);

		b = new bench(d, null);
		b.init(THREADS, 0, 1000L*BENCHTIME);
		System.out.println("Benchmarking "+d.getName());
		b.run();
		System.out.println(b.gettps()+" tps, "+b.getGoodTransactions()+" good, "+b.getFailedTransactions()+" failed, 90 percentile "+b.get90percentile()+" s. tpsB="+b.gettpsB());

		/* Required TPC-B consistency tests. All tests must pass */
		System.out.println("\nTPC-B consistency tests:");
		System.out.println("Data scale factor is: " +t.verifyRecordLayoutsAndSizing(null));
		System.out.println("Data are consistent: "+t.checkDataConsistency(null));
		System.out.println("Data are still consistent after test transactions: "+t.checkDataConsistencyAfterTransactions(null));
		System.out.println("Transactions are atomic: "+t.checkTransactionAtomicity(null));
		System.out.println("Transactions are isolated: "+ t.isolationTests(null));
	}
}
