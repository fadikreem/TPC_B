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

/**
 * class executing transaction profile as defined in section 1.2
 */
public class SUT {

	protected PreparedStatement updateAccount;
	protected PreparedStatement writeToHistory;
	protected PreparedStatement updateTeller;
	protected PreparedStatement updateBranch;
	protected PreparedStatement returnBallance;
	protected Connection con;

	public SUT(Connection con) {
		this.con = con;
	}

	/**
	 *  prepares prepared statements and other database dependent stuff
	 * @param schema
	 * @throws SQLException
	 */
	public void init(String schema) throws SQLException {
		updateAccount  = con.prepareStatement("UPDATE "+driver.schemaToTablePrefix(schema)+"ACCOUNTS SET ABALANCE=ABALANCE+? WHERE AID=?");
		writeToHistory = con.prepareStatement("INSERT INTO "+driver.schemaToTablePrefix(schema)+"HISTORY (BID,TID,AID,DELTA,TIME,FILL) VALUES(?,?,?,?,CURRENT_TIMESTAMP,' ')");
		updateTeller   = con.prepareStatement("UPDATE "+driver.schemaToTablePrefix(schema)+"TELLERS SET TBALANCE=TBALANCE+? WHERE TID=?");
		updateBranch   = con.prepareStatement("UPDATE "+driver.schemaToTablePrefix(schema)+"BRANCHES SET BBALANCE=BBALANCE+? WHERE BID=?");
		returnBallance = con.prepareStatement("SELECT ABALANCE FROM "+driver.schemaToTablePrefix(schema)+"ACCOUNTS WHERE AID=?");
		con.setAutoCommit(false);
	}

	/**
	 * Executes transaction profile as defined in section 1.2
	 * Transaction is not commited
	 * @return account balance
	 * @throws SQLException
	 */
	public int executeProfile(int bid, int tid, int aid, int delta) throws SQLException {
		ResultSet rs;

		updateBranch.setInt(1, delta);
		updateBranch.setInt(2, bid);
		updateBranch.executeUpdate();

		updateTeller.setInt(1, delta);
		updateTeller.setInt(2, tid);
		updateTeller.executeUpdate();

		updateAccount.setInt(1, delta);
		updateAccount.setInt(2, aid);
		updateAccount.executeUpdate();

		writeToHistory.setInt(1, bid);
		writeToHistory.setInt(2, tid);
		writeToHistory.setInt(3, aid);
		writeToHistory.setInt(4, delta);
		writeToHistory.executeUpdate();

		returnBallance.setInt(1, aid);
		rs = returnBallance.executeQuery();
		if (rs.next())
			return rs.getInt(1);
		else
			return Integer.MIN_VALUE;
	}

	/**
	 * release allocated resources. Called at end of test.
	 */
	public void dispose() {
		try {
			con.rollback();
			con.close();
		} catch (SQLException e) { }
		updateAccount = null;
		writeToHistory = null;
		updateTeller = null;
		updateBranch = null;
		returnBallance = null;
		con = null;
	}

	/**
	 * Commits executed profile
	 * @throws SQLException
	 */
	public void commit() throws SQLException {
		con.commit();
	}

	/**
	 * Rollback executed profile. Mainly used for integrity testing
	 */
	public void rollback() {
		try {
			con.rollback();
		}
		catch (Exception e) {}
	}
}
