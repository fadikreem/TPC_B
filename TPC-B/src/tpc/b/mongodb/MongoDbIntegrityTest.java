package tpc.b.mongodb;
import static com.mongodb.client.model.Accumulators.sum;
import static com.mongodb.client.model.Aggregates.group;
import static com.mongodb.client.model.Aggregates.match;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import tpc.b.bench;
import tpc.b.integrityTests;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;

import static tpc.b.mongodb.DbConstants.*;
import static tpc.b.mongodb.MongoDbUtils.*;

public class MongoDbIntegrityTest extends integrityTests {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(MongoDbIntegrityTest.class);
	
	public MongoDbIntegrityTest(MongodbDriver d) {
		super(d);
	}

	/**
	 * check dataset consistency
	 * 
	 * @throws SQLException
	 */
	public boolean checkDataConsistency(String schema) throws SQLException {
		MongodbDriver driver = getDriver();
		try(MongoDbConnection mongo = driver.getMongoConnection(schema)) {
			LOGGER.debug("checkDataConsistency for schema : " + mongo.getDb().getName());
	;
	
			/* test 2.3.3.1 */
			List<Long> l1, l2, l3, l4;
			MongoDatabase mongoDb = mongo.getDb();
			l1 = loadBranchBallance(mongoDb);
			l2 = computeBranchBallanceFromTeller(mongoDb);
			l3 = computeBranchBallanceFromAccount(mongoDb);
			l4 = computeBranchBallanceFromHistory(mongoDb);

			if (l4.size()==0)
				l4 = l3;
			
			return l1.equals(l2) && l1.equals(l4);
		}
	}

	@Override
	public boolean checkDataConsistencyAfterTransactions(String schema)
			throws SQLException {
		LOGGER.debug("checkDataConsistencyAfterTransactions for schema : " + MongodbDriver.getSchemaName(schema));
		int scale = verifyRecordLayoutsAndSizing(schema);

		if ( scale == 0)
			return false;


		/* test 2.3.3.2 */
		
		/* zero tables before benchmark */
		getDriver().zeroBalance(schema);
		getDriver().emptyHistoryTable(schema);

		/* run benchmark */
		bench b;
		b = new MongoDbBench(getDriver(), schema);
		b.init(1, 100 * scale, 0);
		b.run();
		if ( b.getGoodTransactions() != 100*scale )
			return false;

		if (!checkDataConsistency(schema))
			return false;


		
		try (MongoDbConnection mongo = getDriver().getMongoConnection(schema)) {
			MongoDatabase mongoDb = mongo.getDb();
			AggregateIterable<Document> iter = mongoDb.getCollection(BRANCHES).aggregate(Arrays.asList(group(null, sum("total", fieldAsParam(BBALANCE)))));
			Document doc = iter.first();
			long bbalance = MongoDbUtils.objectToLong(doc.get("total"));
			
			iter = mongoDb.getCollection(HISTORY).aggregate(Arrays.asList(group(null, sum("count", 1), sum("total", fieldAsParam(DELTA)))));
			doc = iter.first();
			if (doc == null) {
				return false;
			}
			long count = MongoDbUtils.objectToLong(doc.get("count"));
			long delta = MongoDbUtils.objectToLong(doc.get("total"));
		
			if (b.getGoodTransactions() != count) return false;
			if (bbalance != delta) 	return false;
		}

		return true;
	}

	@Override
	public boolean checkTransactionAtomicity(String schema) throws SQLException {
		SUTMongoDb sut;
		try (MongoDbConnection mongo = getDriver().getMongoConnection(schema)) {
			LOGGER.debug("checkTransactionAtomicity for schema : " + MongodbDriver.getSchemaName(schema));
			MongoDatabase mongoDb = mongo.getDb();
			sut = new SUTMongoDb(mongoDb);
			sut.init();
		
	
			final int aid = 4031;
			final int bid = 1;
			final int tid = 6;
	
			/* 2.2.2.1 */
			long oldaccount;
			long oldteller;
			long oldbranch;
			int oldhistory;
	
			oldaccount = getAccountBalance(mongoDb, aid);
			oldteller = getTellerBalance(mongoDb, tid);
			oldbranch = getBranchBalance(mongoDb, bid);
			oldhistory = getHistoryCount(mongoDb, bid, tid, aid);
	
			sut.executeProfile(bid, tid, aid, 234);
			sut.commit();
	
			if ( (oldaccount == getAccountBalance(mongoDb, aid)) ||
				 (oldteller == getTellerBalance(mongoDb, tid)) ||
				 (oldbranch == getBranchBalance(mongoDb, bid)) ||
				 (oldhistory == getHistoryCount(mongoDb, bid, tid, aid))
					)
				return false;
	
			/* 2.2.2.2 */
			oldaccount = getAccountBalance(mongoDb, aid);
			oldteller = getTellerBalance(mongoDb, tid);
			oldbranch = getBranchBalance(mongoDb, bid);
			oldhistory = getHistoryCount(mongoDb, bid, tid, aid);
			sut.commit();
	
			sut.executeProfile(bid, tid, aid, 234);
			sut.rollback();
	
			if ( (oldaccount != getAccountBalance(mongoDb, aid)) ||
				 (oldteller != getTellerBalance(mongoDb, tid)) ||
				 (oldbranch != getBranchBalance(mongoDb, bid)) ||
				 (oldhistory != getHistoryCount(mongoDb, bid, tid, aid))
					)
				return false;
	
			sut.dispose();
			return true;
		}
	}
	
	@Override
	public MongodbDriver getDriver() {
		return (MongodbDriver) super.getDriver();
	}
	
	@Override
	public boolean isolationTests(String schema) throws SQLException,
			InterruptedException {
		class executor implements Runnable {
			public transient boolean started;
			public transient boolean finished;
			public transient int balance;
			SUTMongoDb s;
			int aid;
			int delta;

			executor(SUTMongoDb s, int aid, int delta) {
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
		LOGGER.debug("isolationTests for schema : " + MongodbDriver.getSchemaName(schema));
		getDriver().zeroBalance(schema);
		SUTMongoDb s1 = null,s2 = null;
		try (MongoDbConnection mongo1 = getDriver().getMongoConnection(schema);MongoDbConnection mongo2 = getDriver().getMongoConnection(schema)) {
			s1 = new SUTMongoDb(mongo1.getDb());
			s2 = new SUTMongoDb(mongo2.getDb());
			s1.init();
			s2.init();
			/* 2.4.2.1 */
			executor ex;
			s1.executeProfile(1, 1, 1, 500);
			ex = new executor(s2, 1, 300);
			new Thread(ex).start();
			while(!ex.started);
			Thread.sleep(1000);
			if (ex.finished == true ) {
				s1.dispose();
				s2.dispose();
				return false;
			}
			s1.commit();
			Thread.sleep(1000);
			if (ex.finished == false) {
				s1.dispose();
				s2.dispose();
				return false;
			}
			if (ex.balance != 800) {
				s1.dispose();
				s2.dispose();
				return false;
			}
			s2.commit();

			/* 2.4.2.2 */
			s1.executeProfile(1, 1, 1, -100);
			ex = new executor(s2, 1, -200);
			new Thread(ex).start();
			while(!ex.started);
			Thread.sleep(1000);
			if (ex.finished == true ) {
				s1.dispose();
				s2.dispose();
				return false;
			}
			s1.rollback();
			Thread.sleep(1000);
			if (ex.finished == false) {
				s1.dispose();
				s2.dispose();
				return false;
			}
			if (ex.balance != 600) {
				s1.dispose();
				s2.dispose();
				return false;
			}
			s2.commit();
			s1.dispose();
			s2.dispose();
			return true;
		}catch(Exception ex) {
			if (s1 != null) s1.dispose();
			if (s2 != null) s2.dispose();
			return false;
		}
	}

	/**
	 * Checks records layout and returns scale factor if all checks passed
	 * 
	 * @return scale factor or 0 if records are incorrectly sized or missing
	 */
	public int verifyRecordLayoutsAndSizing(String schema) throws SQLException {
		LOGGER.debug("verifyRecordLayoutsAndSizing for schema : " + MongodbDriver.getSchemaName(schema));
		MongodbDriver driver = getDriver();
		schema = MongodbDriver.getSchemaName(schema);
		try (MongoDbConnection connection = driver.getMongoConnection(schema)) {
			MongoDatabase mongoDb = connection.getDb();

			// getCollection

			MongoCollection<Document> tellersColl = mongoDb
					.getCollection(TELLERS);
			MongoCollection<Document> branchesColl = mongoDb
					.getCollection(BRANCHES);
			MongoCollection<Document> accountsColl = mongoDb
					.getCollection(ACCOUNTS);
			Set<Integer> bids = getBIDs(branchesColl);
			/*
			 * check rule 3.1.2 - All branches must have the same number of
			 * tellers
			 */
			AggregateIterable<Document> output = tellersColl.aggregate(Arrays
					.asList(group(fieldAsParam(BID), sum("count", 1))));
			List<Document> rs = new LinkedList<Document>();
			for (Document doc : output) {
				int bid = doc.getInteger("_id");
				if (!bids.contains(bid)) {
					continue;
				}
				rs.add(doc);
			}
			if (!sameRows(rs, "count")) {
				return 0;
			}

			/*
			 * check rule 3.1.2 - All branches must have the same number of
			 * accounts.
			 */
			rs.clear();
			output = accountsColl.aggregate(Arrays.asList(group(fieldAsParam(BID),
					sum("count", 1))));
			for (Document doc : output) {
				int bid = doc.getInteger(_ID);
				if (!bids.contains(bid)) {
					continue;
				}
				rs.add(doc);
			}
			if (!sameRows(rs, "count")) {
				return 0;
			}

			/*
			 * we do not check for orphan records in accounts or tellers tables
			 * because database ref. integrity rules should enforce that
			 */

			/* 4.2 - count lines */
			int accounts = (int) accountsColl.count();
			int tellers = (int) tellersColl.count();
			int branches = (int) branchesColl.count();

			if ((accounts != branches * 100000) || (tellers != branches * 10)) {
				return 0;
			}
			
			/* check if there are no holes in numbering */
			if (!checkMinMax(branchesColl, BID, 1, branches)
					|| !checkMinMax(tellersColl, TID, 1, tellers)
					|| !checkMinMax(accountsColl, AID, 1, accounts))
				return 0;
			return (int)branches;
		}
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
	private boolean checkMinMax(MongoCollection<Document> collection, String column, int expected_min, int expected_max) throws SQLException {
		FindIterable<Document> minIter = collection.find().sort(Sorts.ascending(column)).limit(1);
		FindIterable<Document> maxIter =collection.find().sort(Sorts.descending(column)).limit(1);
		Document minDoc = minIter.first();
		Document maxDoc = maxIter.first();
		int minVal = minDoc == null ? Integer.MIN_VALUE : minDoc.getInteger(column);
		int maxVal = maxDoc == null ? Integer.MAX_VALUE : maxDoc.getInteger(column);
		if ((minVal == expected_min) && (maxVal == expected_max))
			return true;
		else
			return false;
	}

	private List<Long> computeBranchBallanceFromAccount(MongoDatabase mongoDb) {
		List <Long> rc = new LinkedList<Long>();
		Set<Integer> bids = getBIDs(mongoDb.getCollection(BRANCHES));
		Bson matchAgg = match(Filters.in(BID, bids.toArray(new Integer[0])));
		Bson sumAgg = group(fieldAsParam(BID), sum("total", fieldAsParam(ABALANCE)));
		Bson sortAgg = Aggregates.sort(Sorts.ascending(_ID));
		AggregateIterable<Document> iter = mongoDb.getCollection(ACCOUNTS).aggregate(Arrays.asList(matchAgg, sumAgg, sortAgg));
		for (Document document : iter) {
			long balance = MongoDbUtils.objectToLong(document.get("total"));
			rc.add(balance);
		}
		return rc;
			
	}
	
	private List<Long> computeBranchBallanceFromHistory(MongoDatabase mongoDb) {
		List <Long> rc = new LinkedList<Long>();
		Set<Integer> bids = getBIDs(mongoDb.getCollection(BRANCHES));
		Bson matchAgg = match(Filters.in(BID, bids.toArray(new Integer[0])));
		Bson sumAgg = group(fieldAsParam(BID), sum("total", fieldAsParam(DELTA)));
		Bson sortAgg = Aggregates.sort(Sorts.ascending(_ID));
		AggregateIterable<Document> iter = mongoDb.getCollection(HISTORY).aggregate(Arrays.asList(matchAgg, sumAgg, sortAgg));
		for (Document document : iter) {
			long balance = MongoDbUtils.objectToLong(document.get("total"));
			rc.add(balance);
		}
		return rc;
	}

	private List<Long> computeBranchBallanceFromTeller(MongoDatabase mongoDb) {
		List <Long> rc = new LinkedList<Long>();
		Set<Integer> bids = getBIDs(mongoDb.getCollection(BRANCHES));
		Bson matchAgg = match(Filters.in(BID, bids.toArray(new Integer[0])));
		Bson sumAgg = group(fieldAsParam(BID), sum("total", fieldAsParam(TBALANCE)));
		Bson sortAgg = Aggregates.sort(Sorts.ascending(_ID));
		AggregateIterable<Document> iter = mongoDb.getCollection(TELLERS).aggregate(Arrays.asList(matchAgg, sumAgg, sortAgg));
		for (Document document : iter) {
			long balance = MongoDbUtils.objectToLong(document.get("total"));
			rc.add(balance);
		}
		return rc;
	}

	/**
	 * return account balance
	 */
	private long getAccountBalance(MongoDatabase mongoDb, int aid) throws SQLException {
		long result = MongoDbUtils.getLongFieldValueById(mongoDb, ACCOUNTS, ABALANCE, AID, aid);
		if (result != Long.MIN_VALUE) {
			return result;
		}
		throw new IllegalStateException("account not found (AID = " + aid +")");
	}

	private Set<Integer> getBIDs(MongoCollection<Document> branchesColl) {
		Set<Integer> bids = new HashSet<Integer>();
		for (Document doc : branchesColl.find()) {
			int bid = doc.getInteger(BID);
			bids.add(bid);
		}
		return bids;
	}
	
	
	/**
	 * return branch balance
	 */
	private long getBranchBalance(MongoDatabase mongoDb, int bid) throws SQLException {
		long result = MongoDbUtils.getLongFieldValueById(mongoDb, BRANCHES, BBALANCE, BID, bid);
		if (result != Long.MIN_VALUE) {
			return result;
		}
		throw new IllegalStateException("branch not found");
	}
	
	
	/**
	 * return history record counts
	 */
	private int getHistoryCount(MongoDatabase mongoDb, int bid, int tid, int aid) throws SQLException {
		FindIterable<Document> iter = mongoDb.getCollection(HISTORY).find(Filters.and(Filters.eq(BID, bid), Filters.eq(TID, tid), Filters.eq(AID, aid)));
		int result = 0;
		for (@SuppressWarnings("unused") Document doc : iter) {
			result++;
		}
		return result;
	}
	
	/**
	 *  return teller balance
	 */
	private long getTellerBalance(MongoDatabase mongoDb, int tid) throws SQLException {
		long result = MongoDbUtils.getLongFieldValueById(mongoDb, TELLERS, TBALANCE, TID, tid);
		if (result != Long.MIN_VALUE) {
			return result;
		}
		throw new IllegalStateException("teller not found");
	}

	private List<Long> loadBranchBallance(MongoDatabase mongoDb) {
		List <Long> rc = new LinkedList<Long>();
		FindIterable<Document> iter = mongoDb.getCollection(BRANCHES).find().sort(Sorts.ascending(BID)).projection(Projections.include("BBALANCE"));
		for (Document doc : iter) {
		   long balance = MongoDbUtils.objectToLong(doc.get(BBALANCE));
		   rc.add(balance);
		}
		return rc;
	}

	/**
	 * Check if all rows have same numeric value
	 * 
	 * @param rs
	 *            ResultSet containing rows
	 * @param col
	 *            column to check
	 * @return true if all rows are same
	 * @throws SQLException
	 */
	private boolean sameRows(List<Document> rs, String col) {
		boolean valueset = false;
		int value = Integer.MIN_VALUE;
		Iterator<Document> itr = rs.iterator();
		while (itr.hasNext()) {
			int tel;
			Document doc = itr.next();
			tel = doc.getInteger(col);
			if (valueset) {
				if (tel != value) {
					return false;
				}
			} else {
				valueset = true;
				value = tel;
			}
		}
		return true;
	}
}
