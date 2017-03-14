package tpc.b.mongodb;
import tpc.b.bench;
import tpc.b.integrityTests;
import tpc.b.worker;

import java.sql.SQLException;
import java.util.ArrayList;


public class MongoDbBench extends bench {
	
	public MongoDbBench(MongodbDriver d, String schema) {
		super(d, schema);
	}
	
	@Override
	public MongodbDriver getDriver() {
		return (MongodbDriver) super.getDriver();
	}
	
	/**
	 * @param threads: anzahl der Workers, die gleichzeitig ausgefürd werden
	 * @param transactions : anzahl die Transktionen pro Worker
	 */
	@Override
	public void init(int threads, int transactions, long testtime) throws SQLException {
		integrityTests it = new MongoDbIntegrityTest(getDriver());
		scale = it.verifyRecordLayoutsAndSizing(getSchema());
		tobestarted = new ArrayList <worker>();

		long stop;
		if (testtime > 0) {
			this.testtime = testtime;
			stop = testtime + System.currentTimeMillis();
		}
		else
			stop = 0L;
		for(int i = 0; i < threads; i++) {
			worker w;
			w = new MongoDbWorker(workers,this);
			w.setTranscations(transactions);
			w.setStopTime(stop);
			tobestarted.add(w);
		}
	}

	
}
