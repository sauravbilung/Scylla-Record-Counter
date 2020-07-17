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

	public WorkerService(Session session,String partitionKeys,BigInteger token, BigInteger sizeOfEachQueryRange,String table) {
		super();
		this.session = session;
		this.partitionKeys=partitionKeys;
		this.token = token;
		this.sizeOfEachQueryRange = sizeOfEachQueryRange;
		this.table=table;
	}

	@Override
	public Long call() throws Exception {
		
		PreparedStatement get = session.prepare("SELECT count(*) FROM "+table+" WHERE token(" + partitionKeys
				+ ") >= ? AND token(" + partitionKeys + ") <= ?");
		String upperLimit = token.toString();
		String lowerLimit = token.add(sizeOfEachQueryRange).toString();
		Row row = session.execute(get.bind(upperLimit, lowerLimit)).one();
		return row.getLong(0);
	}

}
