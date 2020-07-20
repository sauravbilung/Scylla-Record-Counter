package com.counter;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
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
		// int N=16;

		// record counter
		BigInteger totalRecords = new BigInteger("0");

		// token ranges in ScyllaDB
		BigInteger min = new BigInteger("-9223372036854775807");
		BigInteger max = new BigInteger("9223372036854775807");

		// Number of query ranges
		long M = N * 100;
		BigInteger sizeOfEachQueryRange = new BigInteger("9223372036854775807").divide(BigInteger.valueOf(M));

		// Connection properties
		String[] contactPoints = { "172.17.0.2", "172.17.0.3", "172.17.0.4" };
		String keyspace = "catalog";
		String tableName = "superheroes";
		String partitionKeys = "first_name";

		// Creating a thread pool
		ExecutorService executorService = Executors.newFixedThreadPool(N);

		// Checker variable
		// if unequal distribution happens then check notifies for that.
		// and the values are adjusted.
		BigInteger check = min;

		// Creating connection
		DataSource datasource = new DataSource(contactPoints, keyspace);
		datasource.createConnection();
		Session session = DataSource.getSession();

		// List of Callable Tasks
		List<Callable<Long>> callableTasks = new ArrayList<Callable<Long>>();

		// Creating a list of Callable tasks
		while (min.compareTo(max) == -1) {

			check = min;
			check = check.add(sizeOfEachQueryRange);

			if (check.compareTo(max) == 1) {
				sizeOfEachQueryRange = max.subtract(min);
			}

			Callable<Long> callable = new WorkerService(session, partitionKeys, min, sizeOfEachQueryRange, tableName);
			callableTasks.add(callable);

			// System.out.println("Lower Limit of Query : "+min.toString()+" Upper Limit of
			// Query : "+max.toString());

			// Increased query range by 1 from the expected. Still Results
			min = min.add(sizeOfEachQueryRange).add(BigInteger.valueOf(1));

		}

		// Executing the tasks
		List<Future<Long>> futures = null;

		try {

			futures = executorService.invokeAll(callableTasks);

			// Counting the records
			for (Future<Long> future : futures) {
				totalRecords = totalRecords.add(BigInteger.valueOf(future.get()));
				// totalRecords=totalRecords.add(new BigInteger(future.get().toString()));
			}

		} catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
		} finally {
			// Closing connection
			datasource.closeConnection();
		}

		// System.out.println("Total Callables : "+callableTasks.size());
		System.out.println("Total Records : " + totalRecords);

	}
}
