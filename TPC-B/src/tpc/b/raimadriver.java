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

public class raimadriver extends driver {

	@Override
	public void createEmptyTables(String schema, boolean reservespace) throws SQLException {
		super.createEmptyTables(schema,reservespace);

		Connection conn;
		Statement st;
		ResultSet res;
		schema = schemaToTablePrefix(schema);

		conn = getNewConnection();
		st = conn.createStatement();

		// open db
		// create database 
		try {
				res = st.executeQuery("use tpcb;");
				res.close();
			}
			catch (SQLException e) {
				/* open db */
			}
		
		
		st.executeUpdate("DROP procedure IF EXISTS "+schema+"tpcbprofile");

		
		st.executeUpdate(
				"CREATE PROCEDURE "+schema+"tpcbprofile(IN v_bid INT,IN v_tid INT, IN v_aid INT,INOUT  v_delta INT) "+
				" modifies sql data" +
				" BEGIN"+
				//" DECLARE NEW_BALANCE INT;"+
				" UPDATE BRANCHES SET BBALANCE=BBALANCE+V_DELTA WHERE BID=V_BID;"+
				" UPDATE TELLERS SET TBALANCE=TBALANCE+V_DELTA WHERE TID=V_TID;"+
				" UPDATE ACCOUNTS SET ABALANCE=ABALANCE+V_DELTA WHERE AID=V_AID;"+
				" INSERT INTO HISTORY (BID,TID,AID,DELTA,TIMER,FILL) VALUES(V_BID,V_TID,V_AID,V_DELTA,CURRENT_TIMESTAMP,' ');"+
				" SELECT ABALANCE INTO V_DELTA FROM ACCOUNTS WHERE AID=V_AID;"+
				"END;"
		);
		
	/*	st.executeUpdate(
				"CREATE PROCEDURE "+schema+"TPCBPROFILE(IN v_bid integer, IN v_tid integer, IN v_aid integer, INOUT v_delta integer)\n"+
				"BEGIN "+
				"DECLARE NEW_BALANCE INTEGER;"+
				"UPDATE BRANCHES SET BBALANCE=BBALANCE+V_DELTA WHERE BID=V_BID;"+
				"UPDATE TELLERS SET TBALANCE=TBALANCE+V_DELTA WHERE TID=V_TID;"+
				"UPDATE ACCOUNTS SET ABALANCE=ABALANCE+V_DELTA WHERE AID=V_AID;"+
				"INSERT INTO HISTORY (BID,TID,AID,DELTA,TIMER,FILL) VALUES(V_BID,V_TID,V_AID,V_DELTA,CURRENT_TIMESTAMP,' ');"+
				"SELECT ABALANCE AS V_DELTA FROM ACCOUNTS WHERE AID=V_AID;"+
				"END;"
				); */
		conn.close();
	}

	@Override
	public SUT getNewSUT() throws SQLException {
		return new SUTpgsql(getNewConnection());
	}

	@Override
	public String getName() {
		return "Raima 14";
	}

	/* (non-Javadoc)
	 * @see driver#getfillsizes()
	 */
	@Override
	public int[] getfillsizes() {

			int fillsizes[] = { 64, 60, 60, 1 };
			return fillsizes;
		}
}
