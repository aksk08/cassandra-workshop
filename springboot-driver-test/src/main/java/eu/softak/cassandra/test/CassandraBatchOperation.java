package eu.softak.cassandra.test;

import static eu.softak.cassandra.test.CassandraCommon.INSERT_STATEMENT;
import static eu.softak.cassandra.test.CassandraCommon.TRUNCATE_STATEMENT;
import static eu.softak.cassandra.test.CassandraCommon.UPDATE_STATEMENT;
import static eu.softak.cassandra.test.CassandraCommon.getDate;
import static eu.softak.cassandra.test.CassandraCommon.getStatus;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import org.apache.commons.lang3.RandomStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Service
public class CassandraBatchOperation extends AbstractCassandraOperation {

	private static final Logger LOG = LoggerFactory.getLogger(CassandraBatchOperation.class);


	protected void updateRows(List<Row> result) {

		long start = System.currentTimeMillis();
		BatchStatement batchStmt;
		List<BoundStatement> statements = new ArrayList<>();
		int index = 1;
		int howMany = 0;
		for (Row row : result) {
			if (index++ % 20 != 0) {
				statements.add(updateStatement.bind(100, row.get(0, String.class), row.get(1, String.class), row.get(2, Integer.class)));
				howMany++;
			}

			if (index % 100 == 0) {
				batchStmt = new BatchStatement();
				batchStmt.addAll(statements);
				session.execute(batchStmt);
				statements.clear();

			}

			if (index % 1000 == 0) {
				LOG.info("processed {}", index);
			}
		}

		if (!statements.isEmpty()) {
			batchStmt = new BatchStatement();
			batchStmt.addAll(statements);
			session.execute(batchStmt);
		}

		LOG.info("update time {} for {} rows", System.currentTimeMillis() - start, howMany);
	}

	public void importData(int howManyDays, int howManyData) {

		session.execute(TRUNCATE_STATEMENT);

		BoundStatement boundStatement;

		long start = System.currentTimeMillis();
		String id;
		List<BoundStatement> statements = new ArrayList<>();
		for (int i = 0; i < howManyDays; i++) {
			for (int j = 0; j < howManyData; j++) {

				id = UUID.randomUUID().toString();
				boundStatement = new BoundStatement(insertStatement);
				statements
						.add(boundStatement.bind(getDate(i + 1), id, j, id, getStatus(j), RandomStringUtils.random(40, 'a', 'z'), RandomStringUtils.random(250, 'a', 'z')));

				if (j > 0 && j % 20 == 0) {
					executeBatch(statements);
				}

			}
			if (!statements.isEmpty()) {
				executeBatch(statements);
			}
			LOG.info("inserted {} of {} day", i, howManyDays);
		}

		LOG.info("insert time {} for {} rows", System.currentTimeMillis() - start, howManyDays * howManyData);
	}

	private void executeBatch(List<BoundStatement> statements) {
		BatchStatement batchStmt;
		batchStmt = new BatchStatement();
		batchStmt.addAll(statements);
		session.execute(batchStmt);
		statements.clear();
	}
}
