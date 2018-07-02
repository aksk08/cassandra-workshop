package eu.softak.cassandra.test;

public class CassandraCommon {

	public static final String TRUNCATE_STATEMENT = "truncate table test.perf_test;";

	public static final String INSERT_STATEMENT =
			"INSERT INTO test.perf_test (pk_date, id , sec_id ,process_id, status  ,comment , add_text, created_dt ) VALUES (?,?,?,?,?,?,?,?)";

	public static final String SELECT_STATEMENT =
			"select pk_date, id , sec_id ,process_id, status  ,comment , add_text, created_dt from test.test_view where status=? and sec_id in ?";

	public static final String UPDATE_STATEMENT =
			"update test.perf_test set status = ? where  pk_date = ? and  id = ? and  sec_id = ?";

	public static int getStatus(int j) {
		if (j % 20 == 0) {
			return 4;
		}
		return 2;
	}

	public static String getDate(int i) {
		return String.format("%s-JUN-2018", i < 10 ? "0" + i : "" + i);

	}
}
