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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;

/**
 * TPC-B
 * Database dependent driver
 *
 * @author Radim Kolar
 */

public class driver {

	protected String username,password;
	protected String JDBCURL;


	/**
	 * Default constructor
	 */
	public driver() { }

	/**
	 * Initializes database driver, for example loads JDBC driver class and
	 * checks if database connection can be made.
	 *
	 * @return true if initialization was successful
	 */
	public boolean init(Properties args) {
		if (JDBCURL == null )
			return false;
		else
			return true;
	}

	/**
	 * Get list of driver supported properties with description
	 */
	public List <driverPropertyHelp> help() {
		return new ArrayList<driverPropertyHelp>(0);
	}

	/**
	 * Sets username/password. it may be used by database dependent driver
	 *
	 * @param username	test user username
	 * @param password  test user password
	 */
	public void setUserCredentials(String username, String password) {
		this.username=username;
		this.password=password;
	}

	/**
	 * Sets JDBC url for used by this driver
	 */
	public void setJDBCURL(String url) {
		JDBCURL = url;
	}

	/**
	 * Get new database connection
	 */
	public Connection getNewConnection() throws SQLException {
		Connection conn;
		conn = DriverManager.getConnection(JDBCURL, username, password);
		conn.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
		
		
		if(this.getName() == "Raima 14")
		{
	        try { 
	     
	        Statement Stmt =  conn.createStatement();
	        Stmt.execute("use tpcb");
	        
	        } catch (SQLException ex) {
	            System.out.println("catched use tpcb exception");
	        }
		}
        // END for Raima
        
		return conn;
	}

	/**
	 * Create TPC-B tables and fills them with data.
	 * No existing table data reused.
	 * @throws SQLException
	 */
	public void createTables(int scalefactor,boolean reservespace) throws SQLException {
		if (scalefactor < 1) throw new IllegalArgumentException("scalefactor must be > 0");

		createEmptyTables(null,reservespace);
		prepareTestData(null, 0, scalefactor);
	}

	/**
	 * Create TPC-B tables if they do not exists
	 * @param schema schema to create tables
	 * @param reservespace reserve storage for TPC-B storage rules compliance
	 * @throws SQLException
	 */
	public void createEmptyTables(String schema, boolean reservespace) throws SQLException {
		Connection conn;
		Statement st;
		ResultSet res;

		schema = schemaToTablePrefix(schema);
		int fillsizes[] = { 1, 1, 1, 1};
		if (reservespace) fillsizes=getfillsizes();

		conn = getNewConnection();
		st = conn.createStatement();
		
		if (this.getName() == "Raima 14")
		{
			// create database 
			try {
				res = st.executeQuery("CREATE DATABASE tpcb;");
				res = st.executeQuery("use tpcb;");
				res.close();
			}
			catch (SQLException e) {
				/* create db */
			}
		}
		
		try {
			res = st.executeQuery("SELECT * FROM "+schema+"BRANCHES WHERE BID=1");
			res.close();
		}
		catch (SQLException e) {
			/* create table branches */
			st.executeUpdate("CREATE TABLE "+schema+"BRANCHES (BID INTEGER NOT NULL PRIMARY KEY, BBALANCE INTEGER NOT NULL, FILL CHAR("+fillsizes[0]+") NOT NULL)");
		}
		try {
			res = st.executeQuery("SELECT * FROM "+schema+"TELLERS WHERE TID=1");
			res.close();
		}
		catch (SQLException e) {
			/* create table tellers */
			st.executeUpdate("CREATE TABLE "+schema+"TELLERS (TID INTEGER NOT NULL PRIMARY KEY, BID INTEGER NOT NULL REFERENCES BRANCHES, TBALANCE INTEGER NOT NULL, FILL CHAR("+fillsizes[1]+") NOT NULL)");
		}
		try {
			res = st.executeQuery("SELECT * FROM "+schema+"ACCOUNTS WHERE AID=1");
			res.close();
		}
		catch (SQLException e) {
			/* create table accounts */
			st.executeUpdate("CREATE TABLE "+schema+"ACCOUNTS (AID INTEGER NOT NULL PRIMARY KEY, BID INTEGER NOT NULL REFERENCES BRANCHES, ABALANCE INTEGER NOT NULL, FILL CHAR("+fillsizes[2]+") NOT NULL)");
		}
		try {
			res = st.executeQuery("SELECT * FROM "+schema+"HISTORY WHERE TID=0");
			res.close();
		}
		catch (SQLException e) {
			/* create table history */
			st.executeUpdate("CREATE TABLE "+schema+"HISTORY(BID INTEGER NOT NULL REFERENCES BRANCHES, TID INTEGER NOT NULL REFERENCES TELLERS, AID INTEGER NOT NULL REFERENCES ACCOUNTS, DELTA INTEGER NOT NULL, TIMER TIMESTAMP NOT NULL, FILL CHAR("+fillsizes[3]+") NOT NULL)");
			st.executeUpdate("CREATE INDEX "+schema+"HISTORY_ACC ON "+schema+"HISTORY(AID)");
		}
		st.close();
		conn.close();
	}

	/**
	 * Converts schema name to table name prefix
	 * @param schema schema name, may be null
	 * @return non null table prefix
	 */
	public static String schemaToTablePrefix(String schema) {
		if (schema == null)
			schema = "";
		else
			if (!schema.endsWith("."))
				schema += '.';
		return schema;
	}

	/**
	 * Inserts test data.
	 * History table is emptied and balances zeroed.
	 * @throws SQLException
	 */
	public void prepareTestData(String schema, int oldscalefactor, int newscalefactor) throws SQLException {

		if (oldscalefactor < 0 || newscalefactor < 1)
			throw new IllegalArgumentException("invalid scale factors");
		if (oldscalefactor == 0 || newscalefactor < oldscalefactor ) {
			emptyTestTables(schema);
			oldscalefactor = 0;
		}
		else {
			System.out.print("Cleaning history ");
			System.out.flush();
			emptyHistoryTable(schema);
			System.out.print("and account balances");
			System.out.flush();
			zeroBalance(schema);
			System.out.println(".");
		}

		if ( oldscalefactor == newscalefactor )
			return;

		System.out.println("start load "+(newscalefactor-oldscalefactor)*100000+" test rows "+new Date());
		loadTestData(schema, oldscalefactor, newscalefactor);
		System.out.println("done loading test data "+new Date());
	}

	protected void loadTestData(String schema, int oldscalefactor, int newscalefactor) throws SQLException {
		Connection c=getNewConnection();
		PreparedStatement ps;

		c.setAutoCommit(false);

		/* insert new data into branches */
		ps = c.prepareStatement("INSERT INTO "+schemaToTablePrefix(schema)+"BRANCHES (bid,bbalance,fill) VALUES(?,0,' ')");
		for(int i=oldscalefactor+1;i<=newscalefactor;i++) {
			ps.setInt(1, i);
			ps.executeUpdate();
		}

		/* insert into tellers */
		ps = c.prepareStatement("INSERT INTO "+schemaToTablePrefix(schema)+"TELLERS (TID,BID,TBALANCE,FILL) VALUES(?,?,0,' ')");
		for(int i=oldscalefactor*10+1;i<=newscalefactor*10;i++) {
			ps.setInt(1, i);
			ps.setInt(2, 1+(i-1)/10);
			ps.addBatch();
		}
		ps.executeBatch();
		c.commit();
		ps.clearBatch();

		/* insert into accounts */
		ps = c.prepareStatement("INSERT INTO "+schemaToTablePrefix(schema)+"ACCOUNTS (BID,AID,ABALANCE,FILL) VALUES(?,?,0,' ')");
		for(int i=oldscalefactor*100000+1;i<=newscalefactor*100000;) {
			for(int j=0;j<1000;j++,i++)
			{
				ps.setInt(1, 1+(i-1)/100000);
				ps.setInt(2, i);
				ps.addBatch();
			}
			ps.executeBatch();
			c.commit();
			ps.clearBatch();
		}

		c.close();
	}

	/**
	 * Deletes all records from test tables.
	 *
	 * Databases drivers should do additional
	 * clean processing such as shrink empty tables.
	 * @param schema
	 * @throws SQLException
	 */
	public void emptyTestTables(String schema) throws SQLException {
		Connection c=getNewConnection();
		Statement st=c.createStatement();
		List <String> tables = new ArrayList <String>();
		tables.add(schemaToTablePrefix(schema)+"HISTORY");
		tables.add(schemaToTablePrefix(schema)+"ACCOUNTS");
		tables.add(schemaToTablePrefix(schema)+"TELLERS");
		tables.add(schemaToTablePrefix(schema)+"BRANCHES");

		System.out.print("Removing all data from");

		for(String table:tables) {
			System.out.print(" "+table);
			System.out.flush();
			st.executeUpdate("DELETE FROM "+table);
		}
		System.out.println(".");

		c.close();
	}

	/**
	 * Get new SUT instance related to database driver
	 *
	 * @return SUT
	 */
	public SUT getNewSUT() throws SQLException {
		return new SUT(getNewConnection());
	}

	/**
	 * Deletes all records from history table.
	 *
	 * Databases drivers should do additional
	 * clean processing such as shrink empty table.
	 * @param schema
	 * @throws SQLException
	 */
	public void emptyHistoryTable(String schema) throws SQLException {
		Connection c=getNewConnection();
		Statement st=c.createStatement();
		st.executeUpdate("DELETE FROM "+schemaToTablePrefix(schema)+"HISTORY");
		c.close();
	}

	/**
	 * Sets all balances to zero
	 */
	public void zeroBalance(String schema) throws SQLException {
		Connection c=getNewConnection();
		Statement st=c.createStatement();
		c.setAutoCommit(false);
		st.executeUpdate("UPDATE "+schemaToTablePrefix(schema)+"BRANCHES SET BBALANCE=0");
		st.executeUpdate("UPDATE "+schemaToTablePrefix(schema)+"TELLERS  SET TBALANCE=0");
		st.executeUpdate("UPDATE "+schemaToTablePrefix(schema)+"ACCOUNTS SET ABALANCE=0");
		c.commit();
		c.close();
	}

	/**
	 * get driver name
	 */
	public String getName() {
		return "Generic JDBC";
	}

	/**
	 * size of filling char field to get required TPC-B record size
	 * 100 bytes, except for history record which is 50 bytes minimum.
	 * @return fillsizes for TPC-B tables in order branches, tellers, accounts, history.
	 */
	public int[] getfillsizes() {

		int fillsizes[] = { 80, 76, 76, 12 };
		return fillsizes;
	}

}
