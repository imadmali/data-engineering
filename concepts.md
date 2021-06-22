# Data Engineering Concepts

Working in industry as a data scientist I've found it useful to maintain some understanding of data engineering concepts, even if it's just at a high-level.
This facilitates communication with data engineers who form part of the bridge between data and data scientists in most tech companies.
Below is a non-exhaustive laundry list of concepts that I've found useful to know.

## Data Model

A data model organizes data assets and defines the relationship between them.
Typically you might see it as an outline of database/schema design, the available tables, what the primary/foreign keys are (i.e. how tables can be linked together in relational databases).

## Primary vs Foreign Keys

TBD

## Data Lineage

Data lineage describes what happens to the data as it moves from its origin to its final destination.
This destination could be another data repository or it could be a machine learning model.
As a data scientist it's helpful to understand data lineage since it help you determine whether the assumptions you're making in your modeling approach are appropriate.

## Partitioning

Partitioning is the process of splitting up a table to make it more efficient to query.
This can increase the speed at which you read in data.
You only read the partition(s) into memory that you're interested in, which is smaller than loading all the data.
By doing this you're offloading a part of your query (specifically the part that involves filtering the data) to the partitioning logic.

Partitioning by time is commonly used.
For example, we might find ourselves querying recent data compared more often than querying historical data.
It would probably make sense to split up the data between recent and historical so that, for most of our queries, we only hit the recent partition as opposed to all of the data (but we can still access the historical data if we need to).

However, suppose we have partitioned by time but want to query all of a give user's data.
The partition work that we have done is irrelevant.
We have to read all the data into memory since we can't be certain that there is no data for a given user in any of the partitions.
In this case it might be useful to partition by user instead.

In some cases it might be useful to partition by both time *and* user.
However, if we make the partition definition too granular then we could end up increasing our query time (i.e. we will be doing a lot of reads from disk).

[Sharding](https://www.digitalocean.com/community/tutorials/understanding-database-sharding#sharding-architectures) is a type of *horizontal* partitioning that separates the rows of a table into multiple tables (partitions).

The advantages of sharding are that it
1. Reduces query time
2. Mitigates the impact of outages
3. Facilitates horizontal scaling (i.e. the addition of new servers to grow out the database).

The disadvantages are that it,
1. May result in data loss since the process of sharding can be complex
2. Is diffcult to revert back to non-sharded architecture.
3. Might result in unbalanced shards (i.e. depending on the sharding logic and the data we might end up with more records in a given shard compared to other shards).

Three ways to implement sharding are,
1. **Key based**. This uses a hash function to determine which shard each record should go into.
This can be problematic when creating new shards since it requires updating the hash function and therefore updating all the old shards.
2. **Range based**. This uses contiguous non-overlapping splits of a range of a particular column to determine which shard each record should belong to.
3. **Directory based**.
This uses a lookup table (directory) to determine which determines that shard that each record should go into.
This can be problematic when sharding on columns that have high cardinality as there will be a lot of records to maintain in the lookup table.

## Columnar Data

To understand a column store database it's useful to understand its counterpart, a *row store database*.
Suppose you're querying a subset of columns of a data table.
With row storage you have to read all the rows into memory and then drop the columns that you're not interested in (even if the data is partitioned by a certain column).
With column storage you only read the columns that you're interested in.
This is more efficient, particularly for wide data.
In order to accomplish this type of querying you have to store the data in a column-wise format (e.g. parquet).

## Scheduled Date vs Execution Date

It's important to be able to have access to both the scheduled run date and the execution run date of a job.
The scheduled date is the date that the job was scheduled to run and the execution date is the date at which a job actually runs.

In most cases they will be the same.
They differ when you want to trigger a previous run of your job in such a way that your job operates as if it was running at a previous date (the scheduled run date) not today's date (the execution date).

## Idempotent Jobs

A job is idempotent if the output is the same even when you run it multiple times with the same parameters.
If you end up with different output then your job fails to be idempotent.
For example, if you trigger the same job multiple times then you shouldn't end up with duplicated data.

Here are some useful strategies to keep your job idempotent:
* **Scheduled Batch**: Read a reasonable amount of data in and perform transformations on it using the scheduled date instead of the execution date.
Write data using the scheduled run date.
* **Atomic**: Make your job atomic by allowing it to succeed completely, otherwise it should fail completely.
The entire job should fail if one part of the job does not complete successfully.
In these situations you should also clean up or undo any changes you've made to existing data. For example, in Python you can use try/except logic to cleanup a failed workflow.
* **Overwrite**: Delete before writing data produced by your job.
This will prevent jobs from duplicating data in situation where they need to be rerun.

## SQL and NoSQL Databases

A *SQL database* is your typical relational database.
It contains tables whose columns have some sort of relationship between one another.
For example, there might be one table that contains information about movies watched by users, where movies are identified by some movie ID.
This may be combined with another table that has movie IDs and the corresponding meta information about the movie (title, genre, runtime, etc).

A *NoSQL database* is sort of a catchall for everything that is not a SQL database.
There are reasons for not storing data in relational format.
For example, it might not be sensible to store images/audio/text files in key-value pairs instead of a relational table format (although meta information about these objects might be stored in such a way).
Or say we want to be able to find all the users that watched a movie that a given user watched.
It probably makes more sense to store this information in a graph database instead of a relational database.

## Data Warehouse

Hitting raw data files (parquet/json/csv/etc) is not an efficient way to do any sort of analytics or ad hoc queries with your data.
A data warehouse sits on top of your raw data and provides centralized access to all data (most likely cleaned and transformed) for users.
Typically a data warehouse is designed to allow access data with SQL queries and is optimized for read performance over write performance.

## Data Federation

In some cases you might have multiple databases responsible for storing different types of data or collecting data from different locations.
Data federation defines a common data model for these databases so that the application can query a single data source.

## Data Replication

In some cases you might want to,

* Increase the availability of your data so that your application is not bottlenecked by hitting a single data source.
* Ensure the reliability of data access so that your applications are not vulnerable if a data source goes down for some reason.
* Speed up query performance.

If you copy the original data (replication) into multiple servers then you're increasing the availability/reliability/speed through horizontal scaling.
You could do a **full replication** the data which would involve a complete copy of all the data into additional servers, or you could do a **partial replication** data which would only copy a subset of the data into additional servers.
While this comes with the advantages outlined above, there are storage costs to consider when duplicating data (particularly massive amounts of data).
There is also the additional overhead of monitoring and making sure that the replicates stay in sync with updates to the source data (including updates to historical data).

## Caching

This isn't strictly a data engineering concept, but it's a way to access data to ensure a streamline front end application experience (e.g. when you want to reduce the latency of a table/graphic that is presented to a user).
Typically it's faster to access data from memory than it is from disk.
Caching basically stores a portion of your data in memory. When a data request is made, you first check to see if it is in the cache.
If it's not available then you defer to the database.
Typically you'd want to cache data that's frequently requested by the application.
