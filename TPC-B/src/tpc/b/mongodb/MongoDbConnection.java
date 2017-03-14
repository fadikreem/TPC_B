package tpc.b.mongodb;
import java.io.Closeable;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;


public class MongoDbConnection implements Closeable{
	private MongoClient client;
	private MongoDatabase db;
	
	public MongoDbConnection(MongoClient client, MongoDatabase db) {
		this.client = client;
		this.db = db;
	}
	
	public MongoClient getClient() {
		return client;
	}
	
	public MongoDatabase getDb() {
		return db;
	}
	
	@Override
	public void close()  {
		if (client != null) {
			client.close();
		}
	}
}
