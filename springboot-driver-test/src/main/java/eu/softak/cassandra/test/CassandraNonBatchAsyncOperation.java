package eu.softak.cassandra.test;

import static eu.softak.cassandra.test.CassandraCommon.INSERT_STATEMENT;
import static eu.softak.cassandra.test.CassandraCommon.TRUNCATE_STATEMENT;
import static eu.softak.cassandra.test.CassandraCommon.UPDATE_STATEMENT;
import static eu.softak.cassandra.test.CassandraCommon.getDate;
import static eu.softak.cassandra.test.CassandraCommon.getStatus;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Service
public class CassandraNonBatchAsyncOperation extends AbstractCassandraOperation {

	private static final Logger LOG = LoggerFactory.getLogger(CassandraNonBatchAsyncOperation.class);

	Executor executor = Executors.newFixedThreadPool(3 * 4);

	protected void updateRows(List<Row> result) throws InterruptedException {

		CountDownLatch latch = new CountDownLatch(result.size() / 20);
		LatchCallback callback = new LatchCallback(latch);

		long start = System.currentTimeMillis();
		int index = 1;
		int howMany = 0;
		BoundStatement boundStatement;
		for (Row row : result) {
			if (index++ % 20 != 0) {
				boundStatement = updateStatement.bind(100, row.get(0, String.class), row.get(1, String.class), row.get(2, Integer.class));
				ResultSetFuture resultSet = session.executeAsync(boundStatement);
				Futures.addCallback(resultSet, callback, executor);
				howMany++;
			}

			if (index % 1000 == 0) {
				LOG.info("processed {}", index);
			}
		}

		latch.await(1, TimeUnit.MINUTES);

		LOG.info("update time {} for {} rows", System.currentTimeMillis() - start, howMany);
	}

	public void importData(int howManyDays, int howManyData) throws InterruptedException {

		session.execute(TRUNCATE_STATEMENT);

		BoundStatement boundStatement;

		CountDownLatch latch = new CountDownLatch(howManyDays * howManyData);
		LatchCallback callback = new LatchCallback(latch);

		long start = System.currentTimeMillis();
		String id;

		for (int i = 0; i < howManyDays; i++) {
			for (int j = 0; j < howManyData; j++) {

				id = UUID.randomUUID().toString();
				boundStatement = insertStatement
						.bind(getDate(i + 1), id, j, id, getStatus(j), RandomStringUtils.random(40, 'a', 'z'), RandomStringUtils.random(250, 'a', 'z'));
				ResultSetFuture resultSet = session.executeAsync(boundStatement);
				Futures.addCallback(resultSet, callback, executor);

			}
			LOG.info("inserted {} of {} day", i, howManyDays);
		}

		latch.await(1, TimeUnit.MINUTES);
		LOG.info("insert time {} for {} rows", System.currentTimeMillis() - start, howManyDays * howManyData);
	}

	private static class LatchCallback implements FutureCallback<ResultSet> {

		private final CountDownLatch latch;

		public LatchCallback(CountDownLatch latch) {
			this.latch = latch;
		}

		@Override
		public void onSuccess(ResultSet result) {
			latch.countDown();
		}

		@Override
		public void onFailure(Throwable t) {
			LOG.error("callback error" + t.getMessage());
			latch.countDown();
		}
	}
}
