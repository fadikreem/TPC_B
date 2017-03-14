package tpc.b.mongodb;
import java.security.SecureRandom;
import java.util.LinkedList;
import java.util.List;

import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.BasicDBObject;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;

public class MongoDbUtils {

	static final String AB = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
	static SecureRandom rnd = new SecureRandom();

	public static boolean collectionExists(final MongoDatabase mongoDb,
			final String collectionName) {
		MongoIterable<String> collectionNames = mongoDb.listCollectionNames();
		for (final String name : collectionNames) {
			if (name.equalsIgnoreCase(collectionName)) {
				return true;
			}
		}
		return false;
	}
	
	public static Document findOne(MongoCollection<Document> collection , Bson filter) {
		return collection.find(filter).first();
	}
	
	public static Long objectToLong(Object obj) {
		if (obj instanceof Long) {
			return (Long) obj;
		}
		if (obj instanceof Integer) {
			return new Long((int) obj);
		}
		return Long.valueOf(String.valueOf(obj));
	}
	
	public static Integer objectToInteger(Object obj) {
		if (obj instanceof Long) {
			return ((Long) obj).intValue();
		}
		if (obj instanceof Integer) {
			return (Integer) obj;
		}
		return Integer.valueOf(String.valueOf(obj));
	}

	/**
	 * makes a backup copy of the given {@code srcCollection}
	 * 
	 * @param mongoDb
	 * @param srcCollection
	 * @param destCollection
	 */
	public static void copyCollection(MongoDatabase mongoDb,
			String srcCollection, String destCollection) {
		if (collectionExists(mongoDb, destCollection)) {
			throw new IllegalArgumentException("Collection already exists : "
					+ destCollection);
		}
		if (!collectionExists(mongoDb, srcCollection)) {
			throw new IllegalArgumentException("Collection doesn't exist : "
					+ srcCollection);
		}
		mongoDb.createCollection(destCollection);
		List<Document> src = new LinkedList<Document>();
		int maxInserts = 5000;
		int counter = 0;
		for (Document doc : mongoDb.getCollection(srcCollection).find()) {
			if (counter < maxInserts) {
				src.add(doc);
				counter ++;
			} else {
				mongoDb.getCollection(destCollection).insertMany(src);
				src.clear();
				counter = 0;
			}
		}
		if (src.isEmpty()) {
			return;
		}
		mongoDb.getCollection(destCollection).insertMany(src);
	}

	/**
	 * 
	 * 
	 * @param mongoDb
	 * @param srcCollection
	 * @param destCollection
	 */
	public static void replaceCollection(MongoDatabase mongoDb,
			String srcCollection, String destCollection) {
		if (collectionExists(mongoDb, destCollection)) {
			mongoDb.getCollection(destCollection).deleteMany(
					new BasicDBObject());
		} else {
			mongoDb.createCollection(destCollection);
		}
		if (!collectionExists(mongoDb, srcCollection)) {
			return;
		}
		List<Document> src = new LinkedList<Document>();
		for (Document doc : mongoDb.getCollection(srcCollection).find()) {
			src.add(doc);
		}
		if (!src.isEmpty()) {
			mongoDb.getCollection(destCollection).insertMany(src);
		}
		mongoDb.getCollection(srcCollection).drop();
	}

	public static void dropCollection(MongoDatabase mongoDb, String coll) {
		if (collectionExists(mongoDb, coll)) {
			mongoDb.getCollection(coll).drop();
		}
	}

	public static String generateRandomString() {
		int len = 10;
		StringBuilder sb = new StringBuilder(len + 1);
		sb.append("_");
		for (int i = 0; i < len; i++)
			sb.append(AB.charAt(rnd.nextInt(AB.length())));
		return sb.toString();
	}

	public static int getIntFieldValueById(MongoDatabase mongoDb, String coll,
			String outputField, String idField, int id) {
		FindIterable<Document> iter = mongoDb.getCollection(coll)
				.find(Filters.eq(idField, id))
				.projection(Projections.include(outputField));
		Document doc = iter.first();
		if (doc != null) {
			return MongoDbUtils.objectToLong(doc.get(outputField)).intValue();
		}
		return Integer.MIN_VALUE;
	}

	public static long getLongFieldValueById(MongoDatabase mongoDb,
			String coll, String outputField, String idField, long id) {
		FindIterable<Document> iter = mongoDb.getCollection(coll)
				.find(Filters.eq(idField, id))
				.projection(Projections.include(outputField));
		Document doc = iter.first();
		if (doc != null) {
			return MongoDbUtils.objectToLong(doc.get(outputField));
		}
		return Long.MIN_VALUE;
	}
	
	public static String fieldAsParam(String param) {
		return "$" + param;
	}
	
	public static boolean isNullOrEmpty(Object obj) {
		return obj == null || (obj instanceof String && ((String)obj).isEmpty());
	}
	
	public static boolean objectToBoolean(Object obj) {
		if (obj == null) {
			return false;
		}
		return Boolean.valueOf(String.valueOf(obj).trim());
	}
	
	public static void milliSleep(int millis) {
		try {
			Thread.sleep(millis);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
