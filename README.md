# twitterPageRank
Loading 2010 Twitter Dataset to DSE Graph for Page Rank


## To Run: 

```sh
$ cd twitterPageRank/
$ mvn clean install
```

```sh
$ cd twitterPageRank/target
$ dse spark-submit --executor-memory=<SET MEM HERE> --driver-memory=<SET MEM HERE> --class com.twitterdata.pagerank.App runPageRank-1.0-SNAPSHOT.jar 
```

If you want to persist data into Cassandra, create the following Keyspace and Table

```sql
CREATE KEYSPACE twitter_results WITH replication = {'class': 'NetworkTopologyStrategy', '<INSERT DataCenter Name HERE': '3'}  AND durable_writes = true;

CREATE TABLE twitter_results.pagerank (
code text, 
norm_pg double, 
PRIMARY KEY (code)
);
```

## Data:

This project assumes that you are reading the data from DSE Graph. Use this repo to load data: 

[DSE Graph Twitter Load]

[DSE Graph Twitter Load]: <https://github.com/pmehra7/twitterGraphLoad/>
