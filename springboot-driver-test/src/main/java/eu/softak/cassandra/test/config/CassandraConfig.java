package eu.softak.cassandra.test.config;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Session;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class CassandraConfig {

	@Bean
	public Cluster initCluster() {
		PoolingOptions poolingOptions = new PoolingOptions();
		poolingOptions.setMaxRequestsPerConnection(HostDistance.LOCAL, 32768).setMaxRequestsPerConnection(HostDistance.REMOTE, 2000);

		return Cluster.builder().addContactPoint("192.168.238.128").withProtocolVersion(ProtocolVersion.V4).withCredentials("cassandra", "cassandra")
				.withPoolingOptions(poolingOptions).build();
	}

	@Bean
	public Session initSession() {

		return initCluster().connect();
	}

}
