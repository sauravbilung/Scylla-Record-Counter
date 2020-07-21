package com.counter.DataSource;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.policies.DCAwareRoundRobinPolicy;
import com.datastax.driver.core.policies.DefaultRetryPolicy;
import com.datastax.driver.core.policies.TokenAwarePolicy;

public class DataSource {

	static String[] contactPoints;
	static String keyspace;
	static Cluster cluster;
	static Session session;
	static PoolingOptions poolingOptions = new PoolingOptions();
	static QueryOptions queryOptions = new QueryOptions().setConsistencyLevel(ConsistencyLevel.LOCAL_ONE);
	static SocketOptions socketOptions = new SocketOptions().setConnectTimeoutMillis(15000);

	public DataSource(String[] contactPoints, String keyspace) {
		super();
		DataSource.contactPoints = contactPoints;
		DataSource.keyspace = keyspace;
		createConnection();
	}

	public void createConnection() {

		cluster = Cluster.builder().addContactPoints(contactPoints).withQueryOptions(queryOptions)
				.withRetryPolicy(DefaultRetryPolicy.INSTANCE)
				.withLoadBalancingPolicy(new TokenAwarePolicy(DCAwareRoundRobinPolicy.builder().build()))
				.withPoolingOptions(poolingOptions).withSocketOptions(socketOptions).build();

		session = cluster.connect(keyspace);

	}

	public static Session getSession() {
		return session;
	}

	public void closeConnection() {
		session.close();
		cluster.close();
	}
}
