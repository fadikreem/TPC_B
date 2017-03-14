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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * TPC-B Integrity tests
 *
 *
 */
public class integrityTests {

	private driver driver;

	public integrityTests(driver d) {
		driver = d;
	}

	/**
	 * Checks records layout and returns scale factor if all checks passed
	 * @return scale factor or 0 if records are incorrectly sized
	 * or missing
	 */
	public int verifyRecordLayoutsAndSizing(String schema) throws SQLException {
		String s=driver.schemaToTablePrefix(schema);
		Connection c=driver.getNewConnection();
		Statement st=c.createStatement();
		ResultSet rs;

		/* check rule 3.1.2 - All branches must have the same number of tellers */
		rs=st.executeQuery("select TELLERS.bid,count(*) from "+s+"TELLERS inner join "+s+"BRANCHES on TELLERS.bid=BRANCHES.bid group by TELLERS.bid");
		if ( !sameRows(rs, 2) ) {
			c.close();
			return 0;
		}
		/* check rule 3.1.2 - All branches must have the same number of accounts. */
		rs=st.executeQuery("select ACCOUNTS.bid,count(*) from "+s+"ACCOUNTS inner join "+s+"BRANCHES on ACCOUNTS.bid=BRANCHES.bid group by ACCOUNTS.bid");
		if ( !sameRows(rs, 2) ) {
			c.close();
			return 0;
		}

		c.close();
		/* we do not check for orphan records in accounts or tellers tables because database
		 * ref. integrity rules should enforce that */

		/* 4.2 - count lines */
		int accounts=countRows(schema, "ACCOUNTS");
		int tellers =countRows(schema, "TELLERS");
		int branches=countRows(schema, "BRANCHES");

		if ( (accounts != branches * 100000) ||
			 (tellers != branches * 10 ) ) {
			return 0;
		}

		/* check if there are no holes in numbering */
		if ( !checkMinMax(schema, "BRANCHES", "bid", 1, branches) ||
			 !checkMinMax(schema, "TELLERS",  "tid", 1, tellers)   ||
			 !checkMinMax(schema, "ACCOUNTS", "aid", 1, accounts)
			)
			return 0;
		return branches;
	}

	/**
	 * Returns number of rows in table
	 * @param schema
	 * @param table
	 * @return number of rows
	 * @throws SQLException
	 */
	private int countRows(String schema, String table) throws SQLException {
		Connection c=driver.getNewConnection();
		Statement st=c.createStatement();
		ResultSet rs=st.executeQuery("SELECT COUNT(*) FROM "+driver.schemaToTablePrefix(schema)+table);
		rs.next();
		int rc=rs.getInt(1);
		c.close();
		return rc;
	}

	/**
	 * Check if all rows have same numeric value
	 * @param rs ResultSet containing rows
	 * @param column column to check
	 * @return true if all rows are same
	 * @throws SQLException
	 */
	private boolean sameRows(ResultSet rs, int column) throws SQLException {
		boolean valueset=false;
		int value=Integer.MIN_VALUE;

		while(rs.next()) {
			int tel;
			tel=rs.getInt(column);
			if ( valueset ) {
				if (tel != value ) {
					rs.close();
					return false;
				}
			} else {
				valueset = true;
				value = tel;
			}
		}
		rs.close();
		return true;
	}

	/**
	 * Check minimum and maximum column values
	 * @param schema table schema
	 * @param table table name
	 * @param column column name to check
	 * @param expected_min minimum value expected
	 * @param expected_max maximum value expected
	 * @return true if expected values match table data
	 * @throws SQLException
	 */
	private boolean checkMinMax(String schema, String table, String column, int expected_min, int expected_max) throws SQLException {

		Connection c=driver.getNewConnection();
		Statement st=c.createStatement();
		boolean rc;

		ResultSet rs=st.executeQuery("SELECT MIN("+column+"), MAX("+column+") FROM "+driver.schemaToTablePrefix(schema)+table);
		rs.next();
		if( (rs.getInt(1) == expected_min) && (rs.getInt(2) == expected_max))
			rc=true;
		else
			rc=false;

		c.close();
		return rc;
	}

	/**
	 * Checks
	 * @param schema database schema for testing
	 * @return
	 * @throws SQLException
	 */
	public boolean checkTransactionAtomicity(String schema) throws SQLException {
		SUT sut;
		ResultSet rs;
		Statement st;
		Connection con;

		sut = driver.getNewSUT();
		sut.init(schema);
		con = driver.getNewConnection();
		con.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
		con.setAutoCommit(false);
		con.commit();

		final int aid = 4031;
		final int bid = 1;
		final int tid = 6;

		/* 2.2.2.1 */
		long oldaccount;
		long oldteller;
		long oldbranch;
		int oldhistory;

		oldaccount = getAccountBalance(con, schema, aid);
		oldteller = getTellerBalance(con, schema, tid);
		oldbranch = getBranchBalance(con, schema, bid);
		oldhistory = getHistoryCount(con, schema, bid, tid, aid);
		con.commit();

		sut.executeProfile(bid, tid, aid, 234);
		sut.commit();

		if ( (oldaccount == getAccountBalance(con, schema, aid)) ||
			 (oldteller == getTellerBalance(con, schema, tid)) ||
			 (oldbranch == getBranchBalance(con, schema, bid)) ||
			 (oldhistory == getHistoryCount(con, schema, bid, tid, aid))
				)
			return false;

		/* 2.2.2.2 */
		oldaccount = getAccountBalance(con, schema, aid);
		oldteller = getTellerBalance(con, schema, tid);
		oldbranch = getBranchBalance(con, schema, bid);
		oldhistory = getHistoryCount(con, schema, bid, tid, aid);
		con.commit();

		sut.executeProfile(bid, tid, aid, 234);
		sut.rollback();

		if ( (oldaccount != getAccountBalance(con, schema, aid)) ||
			 (oldteller != getTellerBalance(con, schema, tid)) ||
			 (oldbranch != getBranchBalance(con, schema, bid)) ||
			 (oldhistory != getHistoryCount(con, schema, bid, tid, aid))
				)
			return false;

		sut.dispose();
		con.commit();
		con.close();
		return true;
	}

	/**
	 * return account balance
	 */
	private long getAccountBalance(Connection con, String schema, int aid) throws SQLException {
		PreparedStatement st;
		ResultSet rs;
		st = con.prepareStatement("SELECT ABALANCE FROM "+driver.schemaToTablePrefix(schema)+"ACCOUNTS WHERE AID=?");
		st.setInt(1,aid);
		rs= st.executeQuery();
		if (rs.next())
			return rs.getLong(1);
		throw new IllegalStateException("account not found");
	}

	/**
	 *  return teller balance
	 */
	private long getTellerBalance(Connection con, String schema, int tid) throws SQLException {
		PreparedStatement st;
		ResultSet rs;
		st = con.prepareStatement("SELECT TBALANCE FROM "+driver.schemaToTablePrefix(schema)+"TELLERS WHERE TID=?");
		st.setInt(1,tid);

		rs= st.executeQuery();
		if (rs.next())
			return rs.getLong(1);
		throw new IllegalStateException("teller not found");
	}

	/**
	 * return branch balance
	 */
	private long getBranchBalance(Connection con, String schema, int bid) throws SQLException {
		PreparedStatement st;
		ResultSet rs;
		st = con.prepareStatement("SELECT BBALANCE FROM "+driver.schemaToTablePrefix(schema)+"BRANCHES WHERE BID=?");
		st.setInt(1,bid);

		rs= st.executeQuery();
		if (rs.next())
			return rs.getLong(1);
		throw new IllegalStateException("branch not found");
	}

	/**
	 * return history record counts
	 */
	private int getHistoryCount(Connection con, String schema, int bid, int tid, int aid) throws SQLException {
		PreparedStatement st;
		ResultSet rs;
		st = con.prepareStatement("SELECT COUNT(*) FROM "+driver.schemaToTablePrefix(schema)+"HISTORY WHERE BID=? AND TID=? AND AID=?");
		st.setInt(1,bid);
		st.setInt(2,tid);
		st.setInt(3,aid);

		rs= st.executeQuery();
		rs.next();
		return rs.getInt(1);
	}

	/**
	 * check dataset consistency
	 * @throws SQLException
	 */
	public boolean checkDataConsistency(String schema) throws SQLException {
		Connection con=driver.getNewConnection();
		con.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
		con.setAutoCommit(false);
		con.commit();

		/* test 2.3.3.1 */
		List l1, l2, l3, l4;
		l1 = loadBranchBallance(con, schema);
		l2 = computeBranchBallanceFromTeller(con, schema);
		l3 = computeBranchBallanceFromAccount(con, schema);
		l4 = computeBranchBallanceFromHistory(con, schema);

		if (l4.size()==0)
			l4 = l3;

		con.commit();
		con.close();

		return l1.equals(l2) && l1.equals(l4);
	}

	private List<Long> loadBranchBallance(Connection con, String schema) throws SQLException {
		List <Long> rc;
		rc = new ArrayList<Long>();
		ResultSet rs;
		Statement st = con.createStatement();

		rs = st.executeQuery("SELECT BBALANCE FROM "+driver.schemaToTablePrefix(schema)+"BRANCHES ORDER BY BID");
		while(rs.next())
			rc.add(rs.getLong(1));

		st.close();
		return rc;
	}

	private List<Long> computeBranchBallanceFromTeller(Connection con, String schema) throws SQLException {
		List <Long> rc;
		rc = new ArrayList<Long>();
		ResultSet rs;
		Statement st = con.createStatement();

		rs = st.executeQuery("SELECT SUM(TBALANCE) FROM "+driver.schemaToTablePrefix(schema)+"TELLERS JOIN BRANCHES ON BRANCHES.BID=TELLERS.BID GROUP BY TELLERS.BID ORDER BY TELLERS.BID");
		while(rs.next())
			rc.add(rs.getLong(1));

		st.close();
		return rc;
	}

	private List<Long> computeBranchBallanceFromAccount(Connection con, String schema) throws SQLException {
		List <Long> rc;
		rc = new ArrayList<Long>();
		ResultSet rs;
		Statement st = con.createStatement();

		rs = st.executeQuery("SELECT SUM(ABALANCE) FROM "+driver.schemaToTablePrefix(schema)+"ACCOUNTS JOIN BRANCHES ON BRANCHES.BID=ACCOUNTS.BID GROUP BY ACCOUNTS.BID ORDER BY ACCOUNTS.BID");
		while(rs.next())
			rc.add(rs.getLong(1));

		st.close();
		return rc;
	}

	/**
	 * This function works correctly only if history was not deleted
	 * without zeroing all accounts
	 */
	private List<Long> computeBranchBallanceFromHistory(Connection con, String schema) throws SQLException {
		List <Long> rc;
		rc = new ArrayList<Long>();
		ResultSet rs;
		Statement st = con.createStatement();

		rs = st.executeQuery("select sum(delta) from "+driver.schemaToTablePrefix(schema)+"HISTORY join "+driver.schemaToTablePrefix(schema)+"BRANCHES ON BRANCHES.BID=HISTORY.BID group by HISTORY.bid order by HISTORY.bid");
		while(rs.next())
			rc.add(rs.getLong(1));

		st.close();
		return rc;
	}


	public boolean checkDataConsistencyAfterTransactions(String schema) throws SQLException {
		int scale = verifyRecordLayoutsAndSizing(schema);

		if ( scale == 0)
			return false;


		/* test 2.3.3.2 */

		/* zero tables before benchmark */
		driver.zeroBalance(schema);
		driver.emptyHistoryTable(schema);

		/* run benchmark */
		bench b;
		b = new bench(driver, schema);
		b.init(1, 100 * scale, 0);
		b.run();
		if ( b.getGoodTransactions() != 100*scale )
			return false;

		if (!checkDataConsistency(schema))
			return false;

		Connection con=driver.getNewConnection();
		Statement st;
		ResultSet rs;
		con.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
		con.setAutoCommit(false);
		con.commit();
		st = con.createStatement();
		rs = st.executeQuery("SELECT SUM(BBALANCE) FROM "+driver.schemaToTablePrefix(schema)+"BRANCHES");
		rs.next();
		long bbalance = rs.getLong(1);
		rs = st.executeQuery("SELECT COUNT(*), SUM(DELTA) FROM "+driver.schemaToTablePrefix(schema)+"HISTORY");
		rs.next();
		if (b.getGoodTransactions() != rs.getInt(1))
			return false;
		if (bbalance != rs.getLong(2))
			return false;
		con.commit();
		con.close();

		return true;
	}

	public boolean isolationTests(String schema) throws SQLException, InterruptedException {

		class executor implements Runnable {
			public transient boolean started;
			public transient boolean finished;
			public transient int balance;
			SUT s;
			int aid;
			int delta;

			executor(SUT s, int aid, int delta) {
				this.s=s;
				started = false;
				finished = false;
				this.aid = aid;
				this.delta = delta;
				balance = 0;
			}

			public void run() {
				started = true;
				try {
					balance = s.executeProfile(1, 1, aid, delta);
				} catch (SQLException e) { }
				finished = true;
			}
		}
		SUT s1,s2;
		s1 = driver.getNewSUT();
		s2 = driver.getNewSUT();
		s1.init(schema);
		s2.init(schema);
		driver.zeroBalance(schema);
		/* 2.4.2.1 */
		executor ex;
		s1.executeProfile(1, 1, 1, 500);
		ex = new executor(s2, 1, 300);
		new Thread(ex).start();
		while(!ex.started);
		Thread.sleep(1000);
		if (ex.finished == true )
			return false;
		s1.commit();
		Thread.sleep(1000);
		if (ex.finished == false)
			return false;
		if (ex.balance != 800)
			return false;
		s2.commit();

		/* 2.4.2.2 */
		s1.executeProfile(1, 1, 1, -100);
		ex = new executor(s2, 1, -200);
		new Thread(ex).start();
		while(!ex.started);
		Thread.sleep(1000);
		if (ex.finished == true )
			return false;
		s1.rollback();
		Thread.sleep(1000);
		if (ex.finished == false)
			return false;
		if (ex.balance != 600)
			return false;
		s2.commit();

		s1.dispose();
		s2.dispose();

		return true;
	}
	
	public driver getDriver() {
		return driver;
	}
}
