package com.counter;

import java.math.BigInteger;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.counter.DataSource.DataSource;
import com.counter.Worker.WorkerService;
import com.datastax.driver.core.Session;

public class Coordinator {

	public static void main(String[] args) {

		// Node information
		int numberOfCores = 2;
		int numberOfNodes = 1;

		// Number of worker nodes to spawn (parallel queries)
		int N = numberOfCores * numberOfNodes * 3;

		// record counter
		long totalRecords = 0;

		// token ranges in ScyllaDB
		BigInteger min = new BigInteger("-9223372036854775807");
		BigInteger max = new BigInteger("9223372036854775807");

		// Number of query ranges
		long M = N * 100;
		BigInteger sizeOfEachQueryRange = new BigInteger("9223372036854775807").divide(BigInteger.valueOf(M));

		// Connection properties
		String[] contactPoints = { "172.17.0.2","172.17.0.3","172.17.0.4" };
		String keyspace = "catalog";
		String tableName = "superheroes";
		String partitionKeys = "first_name";

		// Beginning execution
		ExecutorService executorService = Executors.newFixedThreadPool(N);

		// Checker variable
		// if unequal distribution happens then check notifies for that.
		// and the values are adjusted.
		BigInteger check = min;

		// Creating connection
		DataSource datasource = new DataSource(contactPoints, keyspace);
		datasource.createConnection();
		Session session = DataSource.getSession();

		try {
			while (min.compareTo(max) == -1) {

				check = min;
				check = check.add(sizeOfEachQueryRange);

				if (check.compareTo(max) == 1) {
					sizeOfEachQueryRange = max.subtract(min);
				}

				Callable<Long> callable = new WorkerService(session, partitionKeys, min, sizeOfEachQueryRange,
						tableName);

				Future<Long> future = executorService.submit(callable);

				totalRecords += future.get();

				min = min.add(sizeOfEachQueryRange);

			}
			
		} catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
		} finally {
			// Closing connection 
			datasource.closeConnection();
		}

		System.out.println("Total Records : " + totalRecords);
	}
}
