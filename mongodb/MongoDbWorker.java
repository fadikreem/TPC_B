package tpc.b.mongodb;

import java.util.Random;

import tpc.b.worker;
/**
 * Die Transktion Thread
 * @author Fadi
 *
 */
public class MongoDbWorker extends worker {

	public MongoDbWorker(ThreadGroup group, MongoDbBench b) {
		super(group, b);
	}

	@Override
	public MongoDbBench getBench() {
		return (MongoDbBench) super.getBench();
	}

	@Override
	public void run() {
		SUTMongoDb sut = null;
		long start;
		int counter;
		MongodbDriver driver = getBench().getDriver();
		try (MongoDbConnection mongo = driver.getMongoConnection(getBench().getSchema())) {
			sut = new SUTMongoDb(mongo.getDb());
			sut.init();

			start = System.currentTimeMillis();
			int scale = b.getScaleFactor();
			Random rand = new Random();
			counter = 0;

			while (counter++ < transactions) {
				long now;
				boolean failed = false;
				try {
					int aid, bid;
					bid = rand.nextInt(scale);
					if (rand.nextFloat() < 0.85f)
						aid = rand.nextInt(100000) + bid * 100000 + 1;
					else
						aid = rand.nextInt(100000 * scale) + 1;
					sut.executeProfile(bid + 1, rand.nextInt(10) + bid * 10 + 1, aid,
							rand.nextInt(999999 * 2) - 999999);
					sut.commit();
				} catch (Exception err) {
					failed = true;
					try {
						sut.rollback();
					} catch (Exception ignored) {
					}
				}

				now = System.currentTimeMillis();
				if (now > stoptime)
					break;
				if (!failed)
					b.reportTransaction(now - start);
				else
					b.reportFailedTransaction();
				start = now;
			}
			sut.dispose();
		} catch (Exception ex) {
			if (sut != null)
				sut.dispose();
		} finally {

		}
	}
}
