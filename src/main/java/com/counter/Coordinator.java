package com.counter;

import java.math.BigInteger;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import com.counter.DataSource.DataSource;
import com.counter.Worker.WorkerService;
import com.datastax.driver.core.Session;

public class Coordinator {

	// For tracking total queries created.
	public static AtomicLong queryCounter = new AtomicLong();

	public static void main(String[] args) {

		DateFormat formatter = new SimpleDateFormat("yyyy.MM.dd HH:mm:ss");
		Date date = new Date();

		// start time
		long startTime = System.currentTimeMillis();

		// Node information
		int numberOfCores = 4;
		int numberOfNodes = 1;

		// Number of worker nodes to spawn (parallel queries)
		int N = numberOfCores * numberOfNodes * 3;

		// record counter
		BigInteger totalRecords = new BigInteger("0");

		// token ranges in ScyllaDB
		BigInteger min = new BigInteger("-9223372036854775807");
		BigInteger max = new BigInteger("9223372036854775807");

		// Number of query ranges
		long M = N * 100;
		// Size of each query range. Total tokens in Scylla: 18446744073709551614
		BigInteger sizeOfEachQueryRange = new BigInteger("18446744073709551614").divide(BigInteger.valueOf(M))
				.subtract(BigInteger.valueOf(1));

		// Connection properties
		// Connection properties
		String[] contactPoints = { "172.17.0.2", "172.17.0.3", "172.17.0.4" };
		String keyspace = "catalog";
		String tableName = "Superheroes";
		String partitionKeys = "first_name";

		// Creating a thread pool
		ExecutorService executorService = Executors.newFixedThreadPool(N);

		// Checker variable. Explained below
		BigInteger check = min;

		// Creating connection
		DataSource datasource = new DataSource(contactPoints, keyspace);
		datasource.createConnection();
		Session session = DataSource.getSession();

		// List of Callable Tasks
		List<Callable<Long>> callableTasks = new ArrayList<Callable<Long>>();

		// Creating a list of Callable tasks
		while (min.compareTo(max) != 1) {

			// check is to identify the case when (min + sizeOfEachQueryRange)
			// becomes greater than max. In such case values are adjusted.
			check = min.add(sizeOfEachQueryRange);

			if (check.compareTo(max) == 1) {
				sizeOfEachQueryRange = max.subtract(min);
			}

			Callable<Long> callable = new WorkerService(session, partitionKeys, min, sizeOfEachQueryRange, tableName);
			callableTasks.add(callable);

			min = min.add(sizeOfEachQueryRange).add(BigInteger.valueOf(1));

		}

		// Initializing atomic long
		queryCounter.set(callableTasks.size());

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

		// Time calculation
		date.setTime(startTime);
		System.out.println("Execution start time : " + formatter.format(date));
		long endTime = System.currentTimeMillis();
		date.setTime(endTime);
		System.out.println("Execution end time : " + formatter.format(date));
		long totalExecutionTime = endTime - startTime;
		System.out.println("Total execution time :" + totalExecutionTime / 1000 + " seconds");

	}
}
