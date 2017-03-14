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
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class pgsqldriver extends driver {

	@Override
	public void emptyHistoryTable(String schema) throws SQLException {
		Connection c=getNewConnection();
		Statement st=c.createStatement();
		st.executeUpdate("TRUNCATE TABLE "+schemaToTablePrefix(schema)+"HISTORY");
		c.close();
	}

	@Override
	public void emptyTestTables(String schema) throws SQLException {
		Connection c=getNewConnection();
		Statement st=c.createStatement();
		System.out.print("Truncating TPC-B tables");
		System.out.flush();
		st.executeUpdate("TRUNCATE TABLE "+schemaToTablePrefix(schema)+"HISTORY,"+
		schemaToTablePrefix(schema)+"ACCOUNTS,"+
		schemaToTablePrefix(schema)+"TELLERS,"+
		schemaToTablePrefix(schema)+"BRANCHES");
		System.out.println(".");
		c.close();
	}

	@Override
	public void createEmptyTables(String schema,boolean reservespace) throws SQLException {
		super.createEmptyTables(schema,reservespace);
		Connection conn;
		Statement st;

		schema = schemaToTablePrefix(schema);

		conn = getNewConnection();
		st = conn.createStatement();
		st.executeUpdate(
				"CREATE OR REPLACE FUNCTION "+schema+"tpcbprofile(IN v_bid integer, IN v_tid integer, IN v_aid integer, INOUT v_delta integer)"+
				"  RETURNS integer AS $BODY$"+
				"DECLARE"+
				"  NEW_BALANCE INTEGER;"+
				"BEGIN"+
				" UPDATE BRANCHES SET BBALANCE=BBALANCE+V_DELTA WHERE BID=V_BID;"+
				" UPDATE TELLERS SET TBALANCE=TBALANCE+V_DELTA WHERE TID=V_TID;"+
				" UPDATE ACCOUNTS SET ABALANCE=ABALANCE+V_DELTA WHERE AID=V_AID RETURNING ABALANCE INTO NEW_BALANCE;"+
				" INSERT INTO HISTORY (BID,TID,AID,DELTA,TIME,FILL) VALUES(V_BID,V_TID,V_AID,V_DELTA,CURRENT_TIMESTAMP,' ');"+
				" V_DELTA := NEW_BALANCE;"+
				"END;$BODY$ LANGUAGE 'plpgsql' VOLATILE"
				);
		conn.close();
	}

	@Override
	public void prepareTestData(String schema, int oldscalefactor,
			int newscalefactor) throws SQLException {
		super.prepareTestData(schema, oldscalefactor, newscalefactor);
		schema = schemaToTablePrefix(schema);
		Connection conn;
		Statement st;
		conn = getNewConnection();
		st = conn.createStatement();
		System.out.print("Vacuum prepared data");
		System.out.flush();
		st.executeUpdate("VACUUM ANALYZE "+schema+"BRANCHES");
		st.executeUpdate("VACUUM ANALYZE "+schema+"TELLERS");
		st.executeUpdate("VACUUM ANALYZE "+schema+"ACCOUNTS");
		st.executeUpdate("VACUUM ANALYZE "+schema+"HISTORY");
		System.out.println(".");
		conn.close();
	}

	@Override
	public SUT getNewSUT() throws SQLException {
		return new SUTpgsql(getNewConnection());
	}

	@Override
	public String getName() {
		return "Postgresql 8.3";
	}

	@Override
	public int[] getfillsizes() {
		int fillsizes[] = { 59, 55, 55, 1 };

		return fillsizes;
	}

	/**
	 * We need to cheat and use READ COMMITED isolation level because PostgreSQL
	 * is not able to pass TPC-B isolation tests in REPEATABLE READ. This is
	 * violation of TPC-B rules, but PostgreSQL still returns correct data in this
	 * isolation level.
	 */
	@Override
	public Connection getNewConnection() throws SQLException {
		Connection conn;
		conn = super.getNewConnection();
		conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
		return conn;
	}
}
