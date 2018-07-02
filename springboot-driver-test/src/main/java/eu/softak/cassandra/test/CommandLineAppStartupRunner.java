package eu.softak.cassandra.test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

@Component
public class CommandLineAppStartupRunner implements CommandLineRunner {

	private static final Logger LOG = LoggerFactory.getLogger(CommandLineAppStartupRunner.class);

	@Autowired
	private CassandraBatchOperation batchOperation;

	@Autowired
	private CassandraNonBatchOperation nonBatchOperation;

	@Autowired
	private CassandraNonBatchAsyncOperation asyncOperation;

	@Override
	public void run(String... args) throws Exception {

		int howManyDays = 20;
		int howManyRowPerDay = 3_000;

		executeNonBatch(howManyDays, howManyRowPerDay);

		executeBatch(howManyDays, howManyRowPerDay);

		executeNonBatchAsync(howManyDays, howManyRowPerDay);

	}

	private void executeNonBatchAsync(int howManyDays, int howManyRowsPerDay) {
		LOG.info("\n\n ==== start simple processing ===== \n");

		try {
			asyncOperation.importData(howManyDays, howManyRowsPerDay);
			asyncOperation.testSelectAndBatchUpdate();
		} catch (InterruptedException e) {
			LOG.error("", e);
		}

		LOG.info("\n\n ==== finished simple processing ===== \n");
	}

	private void executeNonBatch(int howManyDays, int howManyRowsPerDay) throws InterruptedException {
		LOG.info("\n\n ==== start simple processing ===== \n");

		nonBatchOperation.importData(howManyDays, howManyRowsPerDay);
		nonBatchOperation.testSelectAndBatchUpdate();

		LOG.info("\n\n ==== finished simple processing ===== \n");
	}

	private void executeBatch(int howManyDays, int howManyRowsPerDay) throws InterruptedException {
		LOG.info("\n\n ==== start batch processing ===== \n");

		batchOperation.importData(howManyDays, howManyRowsPerDay);
		batchOperation.testSelectAndBatchUpdate();

		LOG.info("\n\n ==== finished batch processing ===== \n");
	}

}