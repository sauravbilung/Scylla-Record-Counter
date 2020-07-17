package com.counter.DataSource;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

public class DataSource {

	String[] contactPoints;
	String keyspace;
	Cluster cluster;
	Session session;

	public DataSource(String[] contactPoints, String keyspace) {
		super();
		this.contactPoints = contactPoints;
		this.keyspace = keyspace;
		createConnection();
	}

	public void createConnection() {
		cluster = Cluster.builder().addContactPoints(contactPoints).build();
		session = cluster.connect(keyspace);
	}

	public Session getSession() {
		return session;
	}

	public void closeConnection() {
		session.close();
		cluster.close();
	}
}
