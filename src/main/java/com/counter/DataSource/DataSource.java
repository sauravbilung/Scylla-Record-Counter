package com.counter.DataSource;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.Session;

public class DataSource {

	static String[] contactPoints;
	static String keyspace;
	static Cluster cluster;
	static Session session;
	static PoolingOptions poolingOptions = new PoolingOptions();

	public DataSource(String[] contactPoints, String keyspace) {
		super();
		DataSource.contactPoints = contactPoints;
		DataSource.keyspace = keyspace;
		createConnection();
	}

	public void createConnection() {
		cluster = Cluster.builder().addContactPoints(contactPoints).withPoolingOptions(poolingOptions).build();
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
