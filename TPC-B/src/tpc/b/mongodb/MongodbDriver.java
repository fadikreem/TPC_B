package tpc.b.mongodb;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.Updates;

import tpc.b.driver;

import static tpc.b.mongodb.DbConstants.*;

public class MongodbDriver extends driver {

	@Override
	public void createEmptyTables(String schema, boolean reservespace)
			throws SQLException {
		try (MongoDbConnection mongo = getMongoConnection(schema)) {
			MongoDatabase mongoDb = mongo.getDb();
			if (!MongoDbUtils.collectionExists(mongoDb, ACCOUNTS)) {
				mongoDb.createCollection(ACCOUNTS);
			}
			if (!MongoDbUtils.collectionExists(mongoDb, BRANCHES)) {
				mongoDb.createCollection(BRANCHES);
			}
			if (!MongoDbUtils.collectionExists(mongoDb, TELLERS)) {
				mongoDb.createCollection(TELLERS);
			}
			if (!MongoDbUtils.collectionExists(mongoDb, HISTORY)) {
				mongoDb.createCollection(HISTORY);
				mongoDb.getCollection(HISTORY).createIndex(Indexes.ascending("AID"));
			} 
			// Transaction ist eine Zwischenspeicher-Tabelle 
			if (!MongoDbUtils.collectionExists(mongoDb, TRANSACTIONS)) {
				mongoDb.createCollection(TRANSACTIONS);
				mongoDb.getCollection(TRANSACTIONS).createIndex(Indexes.hashed("SUT_ID"));
			}
		} catch (Exception ex) {
			throw new SQLException(ex);
		}
	}

	@Override
	public void prepareTestData(String schema, int oldscalefactor,
			int newscalefactor) throws SQLException {
		if (oldscalefactor < 0 || newscalefactor < 1)
			throw new IllegalArgumentException("invalid scale factors");
		if (oldscalefactor == 0 || newscalefactor < oldscalefactor) {
			emptyTestTables(schema);
			oldscalefactor = 0;
		} else {
			System.out.print("Cleaning history ");
			System.out.flush();
			emptyHistoryTable(schema);
			System.out.print("and account balances");
			System.out.flush();
			zeroBalance(schema);
			System.out.println(".");
		}

		if (oldscalefactor == newscalefactor)
			return;

		System.out.println("start load " + (newscalefactor - oldscalefactor)
				* 100000 + " test rows " + new Date());
		loadTestData(schema, oldscalefactor, newscalefactor);
		System.out.println("done loading test data " + new Date());
	}

	@Override
	protected void loadTestData(String schema, int oldscalefactor,
			int newscalefactor) throws SQLException {
		try (MongoDbConnection mongo = getMongoConnection(schema)) {
			MongoDatabase mongoDb = mongo.getDb();
			List<Document> toInsert = new ArrayList<Document>();
			//long currentTimeMillis = System.currentTimeMillis();
			for (int i = oldscalefactor + 1; i <= newscalefactor; i++) {
				Map<String, Object> documentDetail = new HashMap<String, Object>();
				documentDetail.put(BID, i);
				documentDetail.put(BBALANCE, 0);
				documentDetail.put(FILL, " ");
				documentDetail.put(_LOCK, "");
				toInsert.add(new Document(documentDetail));
			}
			mongoDb.getCollection(BRANCHES).insertMany(toInsert);
			toInsert.clear();

			for (int i = oldscalefactor * 10 + 1; i <= newscalefactor * 10; i++) {
				Map<String, Object> documentDetail = new HashMap<String, Object>();
				documentDetail.put(TID, i);
				documentDetail.put(BID, 1 + (i - 1) / 10);
				documentDetail.put(TBALANCE, 0);
				documentDetail.put(FILL, " ");
				toInsert.add(new Document(documentDetail));
			}
			mongoDb.getCollection(TELLERS).insertMany(toInsert);
			toInsert.clear();
			/* insert into accounts */
			for (int i = oldscalefactor * 100000 + 1; i <= newscalefactor * 100000;) {
				for (int j = 0; j < 1000; j++, i++) {
					Map<String, Object> documentDetail = new HashMap<String, Object>();
					documentDetail.put(BID, 1 + (i - 1) / 100000);
					documentDetail.put(AID, i);
					documentDetail.put(ABALANCE, 0);
					documentDetail.put(FILL, " ");
					toInsert.add(new Document(documentDetail));
				}
				mongoDb.getCollection(ACCOUNTS).insertMany(toInsert);
				toInsert.clear();
			}
		}

	}

	@Override
	public void zeroBalance(String schema) throws SQLException {
		try (MongoDbConnection mongo = getMongoConnection(schema)) {
			MongoDatabase mongoDb = mongo.getDb();
			//lockt die Tabelle bis die Transaktion committed oder zuruckgesetzt wird (rollback) ist 
			Bson updates = Updates.combine(Updates.set(BBALANCE, 0), 
					Updates.set(_LOCK, ""));
			mongoDb.getCollection(BRANCHES).updateMany(new BasicDBObject(),
					updates);

			updates = Updates.set(ABALANCE, 0);
			mongoDb.getCollection(ACCOUNTS).updateMany(new BasicDBObject(),
					updates);

			updates = Updates.set(TBALANCE, 0);
			mongoDb.getCollection(TELLERS).updateMany(new BasicDBObject(),
					updates);

		}
	}

	@Override
	public void emptyTestTables(String schema) throws SQLException {
		try (MongoDbConnection mongo = getMongoConnection(schema)) {
			mongo.getDb().getCollection(ACCOUNTS)
					.deleteMany(new BasicDBObject());
			mongo.getDb().getCollection(BRANCHES)
					.deleteMany(new BasicDBObject());
			mongo.getDb().getCollection(TELLERS)
					.deleteMany(new BasicDBObject());
			mongo.getDb().getCollection(HISTORY)
					.deleteMany(new BasicDBObject());
			mongo.getDb().getCollection(TRANSACTIONS).deleteMany(new BasicDBObject());
		}
	}

	@Override
	public void emptyHistoryTable(String schema) throws SQLException {
		try (MongoDbConnection mongo = getMongoConnection(schema)) {
			mongo.getDb().getCollection(HISTORY)
					.deleteMany(new BasicDBObject());
		}
	}

	public static String getSchemaName(String schema) {
		if (schema == null) {
			return "tpc_b_test";
		}
		return schema;
	}

	public MongoDbConnection getMongoConnection(String schema) {
		MongoClient mongo = new MongoClient(JDBCURL);
		MongoDatabase mongoDb = mongo.getDatabase(getSchemaName(schema));
		return new MongoDbConnection(mongo, mongoDb);
	}
	
}
