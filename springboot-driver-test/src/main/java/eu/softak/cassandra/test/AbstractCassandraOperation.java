package eu.softak.cassandra.test;

import static eu.softak.cassandra.test.CassandraCommon.INSERT_STATEMENT;
import static eu.softak.cassandra.test.CassandraCommon.SELECT_STATEMENT;
import static eu.softak.cassandra.test.CassandraCommon.UPDATE_STATEMENT;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.annotation.PostConstruct;

public abstract class AbstractCassandraOperation {

	private static final Logger LOG = LoggerFactory.getLogger(AbstractCassandraOperation.class);

	@Autowired
	protected Session session;

	protected PreparedStatement updateStatement;
	protected PreparedStatement insertStatement;
	protected PreparedStatement selectStatement;

	@PostConstruct
	public void initStatements() {
		updateStatement = session.prepare(UPDATE_STATEMENT);
		insertStatement = session.prepare(INSERT_STATEMENT);
		selectStatement = session.prepare(SELECT_STATEMENT);
	}

	public void testSelectAndBatchUpdate() throws InterruptedException {

		BoundStatement boundStatement;

		List<Integer> range = IntStream.range(20, 850).boxed().collect(Collectors.toList());
		boundStatement = selectStatement.bind(2, Arrays.asList(1, 4, 950));
		long start = System.currentTimeMillis();
		List<Row> result = session.execute(boundStatement).all();
		LOG.info("select time {} for {} rows", System.currentTimeMillis() - start, result.size());

		boundStatement = selectStatement.bind(2, range);
		start = System.currentTimeMillis();
		result = session.execute(boundStatement).all();
		LOG.info("select time {} for {} rows", System.currentTimeMillis() - start, result.size());

		updateRows(result);

		boundStatement = selectStatement.bind(2, range);
		start = System.currentTimeMillis();
		result = session.execute(boundStatement).all();
		LOG.info("select time {} for {} rows", System.currentTimeMillis() - start, result.size());
	}

	protected abstract void updateRows(List<Row> result) throws InterruptedException;

}
