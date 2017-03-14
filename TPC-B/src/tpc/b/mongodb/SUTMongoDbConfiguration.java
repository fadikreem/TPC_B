package tpc.b.mongodb;

import java.net.URL;
import java.util.Properties;
/**
 * liest die mongo sut properties 
 * @author Fadi
 *
 */
public class SUTMongoDbConfiguration {
	
	private static SUTMongoDbConfiguration instance;
	
	private boolean lockReadUsingSemaphore = true;
	
	private boolean lockBeforeTransaction = true;
	
	private SUTMongoDbConfiguration() {
		try {
			URL url = ClassLoader.getSystemClassLoader().getResource("mongo_sut.properties");
			Properties p = new Properties();
			p.load(url.openStream());
			lockReadUsingSemaphore = MongoDbUtils.objectToBoolean(p.getProperty("Tpcb.Mongo.LockReadUsingSemaphore"));
			lockBeforeTransaction = MongoDbUtils.objectToBoolean(p.getProperty("Tpcb.Mongo.LockBeforeTransaction"));
			return;
		} catch(Exception ex) {
			
		}
	}
	
	/**
	 * sperrt die Filiale (Branch) bis eine commit oder rollback operation durchgeführt wird
	 * @return
	 */
	public boolean isLockBeforeTransaction() {
		return lockBeforeTransaction;
	}
	
	/**
	 * stellt sicher, dass nur einen bestimmten SUT worker das {@code branches._LOCK} lesen kann
	 * @return true 
	 */
	public boolean isLockReadUsingSemaphore() {
		return lockReadUsingSemaphore;
	}
	
	public static SUTMongoDbConfiguration getInstance() {
		if (null == instance) {
			instance = new SUTMongoDbConfiguration();
		}
		return instance;
	}

	@Override
	public String toString() {
		return "SUTMongoDbConfiguration [lockReadUsingSemaphore=" + lockReadUsingSemaphore + ", lockBeforeTransaction="
				+ lockBeforeTransaction + "]";
	}
	
	
}
