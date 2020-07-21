# Scylla-Record-Counter
An Asynchronous project which full scans a Scylla table and returns the total record count. This project scans the cluster parallelly with the token function. Token ranges in Scylla varies from -9223372036854775807 to 9223372036854775807. This range is divided into smaller sub ranges and is queried parallelly. The values returned are summed up to get the final result. Refer [here](https://www.scylladb.com/2017/02/13/efficient-full-table-scans-with-scylla-1-6/) for more information.

```com.counter.Coordinator``` is the driver class. Change the parameters here as per your cluster configurations.
