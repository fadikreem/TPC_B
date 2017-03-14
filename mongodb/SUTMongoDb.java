package tpc.b.mongodb;

import static tpc.b.mongodb.DbConstants.*;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;

import org.bson.BsonTimestamp;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
/**
 * Do Transaction
 * @author Fadi
 *
 */
public class SUTMongoDb {
	private static final Semaphore semaphore = new Semaphore(1);
	private MongoDatabase mongoDb;
	/**
	 * eindeutige Transkation-ID
	 */
	public String randomTempId;
	private MongoCollection<Document> accounts;
	private MongoCollection<Document> tellers;
	private MongoCollection<Document> branches;
	private MongoCollection<Document> transactions;
	private MongoCollection<Document> history;

	private static final Logger LOGGER = LoggerFactory.getLogger(SUTMongoDb.class);

	public SUTMongoDb(MongoDatabase mongoDb) {
		this.mongoDb = mongoDb;
		this.randomTempId = MongoDbUtils.generateRandomString();
	}

	/**
	 * prepares prepared statements and other database dependent stuff
	 * 
	 * @param schema
	 */
	public void init() {
		LOGGER.debug("Initialize SUTMongoDb wit ID: " + randomTempId);
		this.branches = mongoDb.getCollection(BRANCHES);
		this.tellers = mongoDb.getCollection(TELLERS);
		this.accounts = mongoDb.getCollection(ACCOUNTS);
		this.transactions = mongoDb.getCollection(TRANSACTIONS);
		this.history = mongoDb.getCollection(HISTORY);
	}

	/**
	 * Executes transaction profile as defined in section 1.2 Transaction is not
	 * commited
	 * 
	 * @return account balance
	 * @throws SQLException
	 */
	public synchronized int executeProfile(int bid, int tid, int aid, int delta) throws SQLException {
		//sperrt die Branches Tabelle für diese Transaktion 
		// Falls die Tablle von einer anderen Tansaktion gesperrt ist, wartet bis die Transktion abgeschlossen ist
		lockProfile(bid);
		LOGGER.debug(String.format(
				"Execute Profile SUTMongoDb wit ID: %s - params : (AID -> %d, BID -> %d, TID -> %d, Delta -> %d)",
				randomTempId, aid, bid, tid, delta));
		doTransaction(bid, tid, aid, delta);
		Document doc = MongoDbUtils.findOne(transactions,
				Filters.and(Filters.eq(SUT_ID, randomTempId), Filters.eq(_TYPE, ACCOUNTS), Filters.eq(AID, aid)));
		return MongoDbUtils.objectToInteger(doc.get(ABALANCE));
	}

	private void unlockAllProfiles() {
		if (!SUTMongoDbConfiguration.getInstance().isLockBeforeTransaction()) {
			return;
		}
		branches.updateMany(Filters.eq(_LOCK, randomTempId), Updates.set(_LOCK, ""));
	}

	private void lockProfile(int bid) {
		// überprüft falls die Lock-Fuktionalität aktiviert ist
		// siehe : mongo_sut.properties -> Tpcb.Mongo.LockBeforeTransaction 
		if (!SUTMongoDbConfiguration.getInstance().isLockBeforeTransaction()) {
			return;
		}
		try {
			if (SUTMongoDbConfiguration.getInstance().isLockReadUsingSemaphore()) {
				// sicherstellen, dass nur ein Thread die Daten vor dem Sperren lesen kann
				// zum Deaktivieren dieser Option: mongo_sut.properties > Tpcb.Mongo.LockReadUsingSemaphore
				semaphore.acquire();
			}
			Document branch = MongoDbUtils.findOne(branches, Filters.eq(BID, bid));
			
			if (branch != null) {
				Object lock = branch.get(_LOCK);
				if (MongoDbUtils.isNullOrEmpty(lock)) {
					branches.updateOne(Filters.eq(BID, bid), Updates.set(_LOCK, randomTempId));
				} else if (!randomTempId.equals(lock)) {
					// Die Tablle ist von einer anderen Transaktion gelockt. Es muss gewartet werden
					while (true) {
						MongoDbUtils.milliSleep(100);
						branch = MongoDbUtils.findOne(branches, Filters.eq(BID, bid));
						lock = branch.get(_LOCK);
						if (MongoDbUtils.isNullOrEmpty(lock)) {
							MongoDbUtils.milliSleep(100);
							branches.updateOne(Filters.eq(BID, bid), Updates.set(_LOCK, randomTempId));
							break;
						} else if (randomTempId.equals(lock)) {
							break;
						}
					}
				}
				return;
			} else {
				throw new IllegalArgumentException("No branch found with id : " + bid);
			}
		} catch (InterruptedException ex) {
			LOGGER.error(ex.getMessage(), ex);
		} finally {
			if (SUTMongoDbConfiguration.getInstance().isLockReadUsingSemaphore()) {
				semaphore.release();
			}
		}
	}

	public synchronized void doTransaction(int bid, int tid, int aid, int delta) {
		//sucht ob diese Transaktion vorher die Tabellen geändert hat
		Document branch = transactions.findOneAndUpdate(
				Filters.and(Filters.eq(SUT_ID, randomTempId), Filters.eq(_TYPE, BRANCHES), Filters.eq(BID, bid)),
				Updates.combine(Updates.inc(BBALANCE, delta), Updates.set(_TIMESTAMP, System.currentTimeMillis())));
		if (branch == null) {
			// Falls keine Änderungen vorhander, wird ein neues Dokument erstellt
			branch = MongoDbUtils.findOne(branches, Filters.eq(BID, bid));
			if (branch != null) {
				ObjectId id = (ObjectId) branch.get(_ID);
				Long bbalance = MongoDbUtils.objectToLong(branch.get(BBALANCE));
				Document newDocument = branch;
				newDocument.put(_ID, ObjectId.get());
				newDocument.put(_OLDID, id);
				newDocument.put(BBALANCE, bbalance + delta);
				newDocument.put(_TYPE, BRANCHES);
				newDocument.put(_TIMESTAMP, System.currentTimeMillis());
				newDocument.put(SUT_ID, randomTempId);
				transactions.insertOne(newDocument);
			}
		}

		Document teller = transactions.findOneAndUpdate(
				Filters.and(Filters.eq(SUT_ID, randomTempId), Filters.eq(_TYPE, TELLERS), Filters.eq(TID, tid)),
				Updates.combine(Updates.inc(TBALANCE, delta), Updates.set(_TIMESTAMP, System.currentTimeMillis())));
		if (teller == null) {
			teller = MongoDbUtils.findOne(tellers, Filters.eq(TID, tid));
			if (teller != null) {
				ObjectId id = (ObjectId) teller.get(_ID);
				Long tbalance = MongoDbUtils.objectToLong(teller.get(TBALANCE));
				Document newDocument = teller;
				newDocument.put(_ID, ObjectId.get());
				newDocument.put(_OLDID, id);
				newDocument.put(TBALANCE, tbalance + delta);
				newDocument.put(_TYPE, TELLERS);
				newDocument.put(_TIMESTAMP, System.currentTimeMillis());
				newDocument.put(SUT_ID, randomTempId);
				transactions.insertOne(newDocument);
			}
		}

		Document account = transactions.findOneAndUpdate(
				Filters.and(Filters.eq(SUT_ID, randomTempId), Filters.eq(_TYPE, ACCOUNTS), Filters.eq(AID, aid)),
				Updates.combine(Updates.inc(ABALANCE, delta), Updates.set(_TIMESTAMP, System.currentTimeMillis())));
		if (account == null) {
			account = MongoDbUtils.findOne(accounts, Filters.eq(AID, aid));
			if (account != null) {
				ObjectId id = (ObjectId) account.get(_ID);
				Long abalance = MongoDbUtils.objectToLong(account.get(ABALANCE));
				Document newDocument = account;
				newDocument.put(_ID, ObjectId.get());
				newDocument.put(_OLDID, id);
				newDocument.put(ABALANCE, abalance + delta);
				newDocument.put(_TYPE, ACCOUNTS);
				newDocument.put(_TIMESTAMP, System.currentTimeMillis());
				newDocument.put(SUT_ID, randomTempId);
				transactions.insertOne(newDocument);
			}
		}

		Map<String, Object> docObj = new HashMap<String, Object>();
		docObj.put(BID, bid);
		docObj.put(TID, tid);
		docObj.put(AID, aid);
		docObj.put(DELTA, delta);
		docObj.put(TIMER, new BsonTimestamp());
		docObj.put(SUT_ID, randomTempId);
		docObj.put(_TYPE, HISTORY);
		docObj.put(FILL, " ");

		transactions.insertOne(new Document(docObj));
	}

	/**
	 * release allocated resources. Called at end of test.
	 */
	public void dispose() {
		LOGGER.debug("dispose SUTMongoDb wit ID: " + randomTempId);
		rollback();
	}

	public synchronized void commit() {
		LOGGER.debug("commit SUTMongoDb wit ID: " + randomTempId);
		FindIterable<Document> branchesIter = transactions
				.find(Filters.and(Filters.eq(SUT_ID, randomTempId), Filters.eq(_TYPE, BRANCHES)));
		for (Document document : branchesIter) {
			branches.updateOne(Filters.eq(_ID, document.get(_OLDID)), Updates.set(BBALANCE, document.get(BBALANCE)));
		}

		FindIterable<Document> tellersIter = transactions
				.find(Filters.and(Filters.eq(SUT_ID, randomTempId), Filters.eq(_TYPE, TELLERS)));
		for (Document document : tellersIter) {
			tellers.updateOne(Filters.eq(_ID, document.get(_OLDID)), Updates.set(TBALANCE, document.get(TBALANCE)));
		}

		FindIterable<Document> accountsIter = transactions
				.find(Filters.and(Filters.eq(SUT_ID, randomTempId), Filters.eq(_TYPE, ACCOUNTS)));
		for (Document document : accountsIter) {
			accounts.updateOne(Filters.eq(_ID, document.get(_OLDID)), Updates.set(ABALANCE, document.get(ABALANCE)));
		}

		FindIterable<Document> historyIter = transactions
				.find(Filters.and(Filters.eq(SUT_ID, randomTempId), Filters.eq(_TYPE, HISTORY)));
		List<Document> toAdd = new ArrayList<>();
		for (Document document : historyIter) {
			document.remove(SUT_ID);
			document.remove(_TIMESTAMP);
			document.remove(_TYPE);
			toAdd.add(document);
		}
		if (!toAdd.isEmpty()) {
			history.insertMany(toAdd);
		}
		unlockAllProfiles();
		transactions.deleteMany(Filters.eq(SUT_ID, randomTempId));
	}

	/**
	 * Rollback executed profile. Mainly used for integrity testing
	 */
	public synchronized void rollback() {
		LOGGER.debug("rollback SUTMongoDb wit ID: " + randomTempId);
		unlockAllProfiles();
		transactions.deleteMany(Filters.eq(SUT_ID, randomTempId));
	}

}
