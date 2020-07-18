package com.counter.Worker;

import java.math.BigInteger;
import java.util.concurrent.Callable;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class WorkerService implements Callable<Long> {

	Session session;
	String partitionKeys;
	BigInteger token;
	BigInteger sizeOfEachQueryRange;
	String table;

	public WorkerService(Session session, String partitionKeys, BigInteger token, BigInteger sizeOfEachQueryRange,
			String table) {
		super();
		this.session = session;
		this.partitionKeys = partitionKeys;
		this.token = token;
		this.sizeOfEachQueryRange = sizeOfEachQueryRange;
		this.table = table;
	}

	@Override
	public Long call() throws Exception {

		PreparedStatement get = session.prepare("SELECT count(1) FROM " + table + " WHERE token(" + partitionKeys
				+ ") >= token(?) AND token(" + partitionKeys + ") <= token(?)");
		String lowerLimitOfQuery = token.toString();
		String upperLimitOfQuery = token.add(sizeOfEachQueryRange).subtract(BigInteger.valueOf(1)).toString();

		// # Thread and task information
		System.out.println("Thread " + Thread.currentThread().getId() + " is executing for token ranges from "
				+ lowerLimitOfQuery + " to " + upperLimitOfQuery);

		Row row = session.execute(get.bind(lowerLimitOfQuery, upperLimitOfQuery)).one();
		return row.getLong(0);

	}

}
