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
import java.sql.SQLException;
import java.sql.Statement;

/** DB2 database driver */

public class db2driver extends driver {


	@Override
	public void emptyHistoryTable(String schema) throws SQLException {
		Connection c=getNewConnection();
		Statement st=c.createStatement();
		st.executeUpdate("TRUNCATE TABLE "+schemaToTablePrefix(schema)+"HISTORY REUSE STORAGE IMMEDIATE");
		c.close();
	}

	@Override
	public void emptyTestTables(String schema) throws SQLException {
		emptyHistoryTable(schema);
		/* we can not use TRUNCATE TABLE because of referential integrity */
		super.emptyTestTables(schema);
	}

	@Override
	protected void loadTestData(String schema, int oldscalefactor,
			int newscalefactor) throws SQLException {
		Connection c=getNewConnection();
		Statement st=c.createStatement();

		st.executeUpdate(
				"BEGIN ATOMIC "+
				"DECLARE FACTOR INTEGER DEFAULT "+oldscalefactor+";"+
				"DECLARE I INTEGER;"+
				"WHILE FACTOR < "+newscalefactor+" DO "+
				"INSERT INTO "+schemaToTablePrefix(schema)+"BRANCHES (bid,bbalance,fill) VALUES(FACTOR + 1,0,' ');"+
				"SET I = 1;"+
				"WHILE I<=10 DO "+
				"INSERT INTO "+schemaToTablePrefix(schema)+"TELLERS (TID,BID,TBALANCE,FILL) VALUES( FACTOR * 10 + I, FACTOR + 1, 0, ' ');"+
				"SET I = I + 1;" +
				"END WHILE;" +
				"SET FACTOR = FACTOR + 1;" +
				"END WHILE;END"
				);

		/* insert into accounts */
		for(int i=oldscalefactor; i < newscalefactor;i++) {
			st.executeUpdate(
					"BEGIN ATOMIC " +
					"DECLARE I INTEGER DEFAULT "+(i*100000+1)+";"+
					"WHILE I <="+(i+1)*100000+" DO "+
					"INSERT INTO "+schemaToTablePrefix(schema)+"ACCOUNTS (BID,AID,ABALANCE,FILL) VALUES(1+(i-1)/100000,i,0,' ');" +
					"SET I = I + 1;" +
					"END WHILE;" +
					"END"
					);
		}
		c.close();
	}

	@Override
	public void createEmptyTables(String schema, boolean reservespace) throws SQLException {
		super.createEmptyTables(schema,reservespace);
		Connection conn;
		Statement st;

		schema = schemaToTablePrefix(schema);

		conn = getNewConnection();
		st = conn.createStatement();
		st.executeUpdate(
				"CREATE OR REPLACE PROCEDURE "+schema+"TPCBPROFILE( IN V_BID INTEGER,"+
				"IN V_TID INTEGER, IN V_AID INTEGER, INOUT V_DELTA INTEGER )"+
				"LANGUAGE SQL DYNAMIC RESULT SETS 0 MODIFIES SQL DATA " +
				"NOT DETERMINISTIC COMMIT ON RETURN NO NO EXTERNAL ACTION "+
				"BEGIN "+
				"DECLARE NEW_BALANCE INTEGER;"+

				"UPDATE BRANCHES SET BBALANCE=BBALANCE+V_DELTA WHERE BID=V_BID;"+
				"UPDATE TELLERS SET TBALANCE=TBALANCE+V_DELTA WHERE TID=V_TID;"+
				"SELECT ABALANCE INTO NEW_BALANCE FROM FINAL TABLE(UPDATE ACCOUNTS SET ABALANCE=ABALANCE+V_DELTA WHERE AID=V_AID);"+
				"INSERT INTO HISTORY (BID,TID,AID,DELTA,TIMER,FILL) VALUES(V_BID,V_TID,V_AID,V_DELTA,CURRENT_TIMESTAMP,' ');"+
				"SET V_DELTA = NEW_BALANCE;"+
				"END"
				);
		conn.close();
	}

	@Override
	public SUT getNewSUT() throws SQLException {
		return new SUTDB2(getNewConnection());
	}

	@Override
	public String getName() {
		return "DB2 9 LUW";
	}

	/*
	 * row overhead is 10 bytes.
	 * timestamp 10 bytes
	 * integer 4 bytes
	 * numeric trunc(p/2)+1
	 */
	@Override
	public int[] getfillsizes() {
		int fillsizes[] = { 80, 76, 76, 12 };

		return fillsizes;
	}
}
