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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


public class oracledriver extends driver {

	@Override
	public void emptyHistoryTable(String schema) throws SQLException {
		Connection c=getNewConnection();
		Statement st=c.createStatement();
		st.executeUpdate("TRUNCATE TABLE "+schemaToTablePrefix(schema)+"HISTORY");
		c.close();
	}

	@Override
	public void emptyTestTables(String schema) throws SQLException {
		emptyHistoryTable(schema);
		/* we can not use TRUNCATE TABLE because of referential integrity */
		super.emptyTestTables(schema);
	}

	@Override
	public void createEmptyTables(String schema, boolean reservespace) throws SQLException {
		Connection conn;
		Statement st;
		ResultSet res;

		schema = schemaToTablePrefix(schema);
		int fillsizes[] = { 1,1,1,1};
		if (reservespace) fillsizes=getfillsizes();

		conn = getNewConnection();
		st = conn.createStatement();
		try {
			res = st.executeQuery("SELECT * FROM "+schema+"BRANCHES WHERE BID=1");
			res.close();
		}
		catch (SQLException e) {
			/* create table branches */
			st.executeUpdate("CREATE TABLE "+schema+"BRANCHES (BID NUMBER(9) NOT NULL PRIMARY KEY, BBALANCE NUMERIC(10) NOT NULL, FILL CHARACTER("+fillsizes[0]+") NOT NULL)");
		}
		try {
			res = st.executeQuery("SELECT * FROM "+schema+"TELLERS WHERE TID=1");
			res.close();
		}
		catch (SQLException e) {
			/* create table tellers */
			st.executeUpdate("CREATE TABLE "+schema+"TELLERS (TID NUMBER(9) NOT NULL PRIMARY KEY, BID NUMBER(9) NOT NULL REFERENCES BRANCHES, TBALANCE NUMERIC(10) NOT NULL, FILL CHARACTER("+fillsizes[1]+") NOT NULL)");
		}
		try {
			res = st.executeQuery("SELECT * FROM "+schema+"ACCOUNTS WHERE AID=1");
			res.close();
		}
		catch (SQLException e) {
			/* create table accounts */
			st.executeUpdate("CREATE TABLE "+schema+"ACCOUNTS (AID NUMBER(9) NOT NULL PRIMARY KEY, BID NUMBER(9) NOT NULL REFERENCES BRANCHES, ABALANCE NUMERIC(10) NOT NULL, FILL CHARACTER("+fillsizes[2]+") NOT NULL)");
		}
		try {
			res = st.executeQuery("SELECT * FROM "+schema+"HISTORY WHERE TID=0");
			res.close();
		}
		catch (SQLException e) {
			/* create table history */
			st.executeUpdate("CREATE TABLE "+schema+"HISTORY(BID NUMBER(9) NOT NULL REFERENCES BRANCHES, TID NUMBER(9) NOT NULL REFERENCES TELLERS, AID NUMBER(9) NOT NULL REFERENCES ACCOUNTS, DELTA NUMERIC(10) NOT NULL, TIME TIMESTAMP NOT NULL, FILL CHARACTER("+fillsizes[3]+") NOT NULL)");
			st.executeUpdate("CREATE INDEX "+schema+"HISTORY_ACC ON "+schema+"HISTORY(AID)");
		}

		st.executeUpdate(
				"CREATE OR REPLACE PROCEDURE "+schema+"tpcbprofile(v_bid IN NUMBER, v_tid IN NUMBER, v_aid IN NUMBER, v_delta IN OUT NUMBER) "+
				"IS "+
				"  NEW_BALANCE NUMBER;"+
				"BEGIN"+
				" UPDATE BRANCHES SET BBALANCE=BBALANCE+V_DELTA WHERE BID=V_BID;"+
				" UPDATE TELLERS SET TBALANCE=TBALANCE+V_DELTA WHERE TID=V_TID;"+
				" UPDATE ACCOUNTS SET ABALANCE=ABALANCE+V_DELTA WHERE AID=V_AID RETURNING ABALANCE INTO NEW_BALANCE;"+
				" INSERT INTO HISTORY (BID,TID,AID,DELTA,TIME,FILL) VALUES(V_BID,V_TID,V_AID,V_DELTA,CURRENT_TIMESTAMP,' ');"+
				" V_DELTA := NEW_BALANCE;"+
				"END;"
		);
		conn.close();
	}

	@Override
	public SUT getNewSUT() throws SQLException {
		return new SUTpgsql(getNewConnection());
	}

	@Override
	public String getName() {
		return "Oracle 10g";
	}

	@Override
	public int[] getfillsizes() {
		int fillsizes[] = { 75, 71, 71, 7 };

		return fillsizes;
	}

}
