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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class bench implements Runnable {

	/**
	 * 90% of transactions must complete within this time limit (sec).
	 */
	public final static double MAX_RESIDENCE_TIME=2.0;

	private driver driver;
	protected ThreadGroup workers;
	protected List <worker> tobestarted;
	private String schema;
	private AtomicInteger good_transactions;
	private AtomicInteger failed_transactions;
	private ArrayList <Float> txtimes;
	protected int scale;
	protected long testtime;

	public bench(driver d, String schema) {
		this.driver = d;
		workers = new ThreadGroup("workers");
		this.schema = schema;
		good_transactions = new AtomicInteger();
		failed_transactions = new AtomicInteger();
		txtimes = new ArrayList<Float>(200000);
	}

	/**
	 * creates pool of test threads
	 * @param threads	number of threads to use in test
	 * @param transactions maximum number of transactions to run per thread
	 * @param testtime maximum test duration wall clock time in milliseconds
	 * @throws SQLException
	 */
	public void init(int threads, int transactions, long testtime) throws SQLException {
		integrityTests it = new integrityTests(driver);
		scale = it.verifyRecordLayoutsAndSizing(schema);
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
			w = new worker(workers,this);
			w.setTranscations(transactions);
			w.setStopTime(stop);
			tobestarted.add(w);
		}
	}

	/**
	 * Get database driver
	 * @return database driver
	 */
	public driver getDriver() {
		return driver;
	}

	/**
	 * Get database schema used by benchmark
	 */
	public String getSchema() {
		return schema;
	}

	/**
	 * Report successful transaction
	 * @param eta time elapsed to complete transaction
	 */
	public void reportTransaction(long eta) {
		int curt = good_transactions.incrementAndGet();
		synchronized(txtimes) {
			if(curt % 50000 == 0) {
				txtimes.ensureCapacity(txtimes.size()+50001);
			}
			txtimes.add((float)(eta/1000.0f));
		}
	}

	/**
	 * Report failed transaction
	 */
	public void reportFailedTransaction() {
		failed_transactions.incrementAndGet();
	}

	/**
	 * Get scale factor
	 */
	public int getScaleFactor() {
		return scale;
	}

	/**
	 * Get test time
	 * @returns test time in ms or zero
	 */
	public long gettestTime() {
		return testtime;
	}

	/**
	 * Runs TPC-B benchmark
	 */
	public void run() {
		for(worker w:tobestarted) {
			w.start();
		}
		/* wait until workers finish */
		while( workers.activeCount() > 0)
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				break;
			}
	}

	public int getGoodTransactions() {
		return good_transactions.get();
	}

	public int getFailedTransactions() {
		return failed_transactions.get();
	}

	/**
	 * Get tpsB rating.
	 *
	 * This procedure gets real tpsB rating if all required rules passed.
	 * @return tpsB or negative number indicating that tpsB condition were not met.
	 *        -1 time limit was incorrect,
	 *        -2 90percentile condition < 2sec was not met,
	 *        -3 more than 1% of failed transactions
	 */
	public double gettpsB() {
		double tpsB;
		if (testtime<15*60*1000L || testtime>60*60*1000L)
			return -1;
		if (failed_transactions.get()>good_transactions.get()*0.01)
			return -3;
		if (get90percentile()>MAX_RESIDENCE_TIME)
			return -2;
		tpsB = gettps();
		/* is database big enough? */
		if ( tpsB > scale )
			tpsB = scale;
		return tpsB;
	}

	public double gettps() {
		double tps=getGoodTransactions()/(gettestTime()/1000.0);
		return Math.round(100.0*tps)/100.0;
	}
	/**
	 * Get 90% percentile txtime
	 */
	public float get90percentile() {
		Collections.sort(txtimes);
		if(txtimes.size() == 0) return Float.MAX_VALUE;
		return txtimes.get((int)(txtimes.size()*0.9));
	}
	

}
