# Data Engineering Homework

## Problem

### Context

We get 10 millions of transactions every day. Each transaction has
- an amount (int)
- a purchase date (date)
- a business id (string)
- a payment instrument id (string)

Other potentially interesting details
- Transactions arrive in real time
- The time difference between the reception of a transaction and the end of its
  processing should not exceed 10 minutes.
- We have already acquired 4 years of transactions.
- We have transactions for about 500000 different businesses already, which can vary
  greatly in size. Some are independent yoga teachers. A few are large supermarkets.

We need to store all transactions and enable the following interactive data accesses
- get a list of transaction for a combination of payment instrument and business
- For a business, graph the share of recurrent customers over time, month by month. A
  recurrent customer for month M is one that has made more than 1 transaction
  between M-12 (included) and M (excluded). The data point for each month is the
  ratio between the number of recurrent customers over the total number of customers.

### What you need to do

Outline a data pipeline architecture that will support the above use cases. Your outline will
include:
- data transformation stages. what goes in, what comes out.
- what data is stored and when (final and intermediate storage as applicable)
- what types of storage technologies we should use.
- how is data processing distributed

## Problem analysis

From the problem description, we know that a transaction (denoted `Trans` or `trans`) consists of:
- Amount [int] (denoted `Amt`)
- Purchase Date [date] (denoted `Tdate`)
- Business Id [string] (denoted `BuId`)
- Payment Instrument Id [string] (denoted `PiId`)

We also assume that an incoming transaction has an `Id` [string] column to identify the transaction itself. 

In order not to make problem more complex, we assume that each customer has only one payment instrument. Thus, our `PiId`
could be used as an **IDENTIFIER** for customers.

### Data volume

We can roughly estimate the data size as following:
- `Id[string]`: about 128 bytes (assume this field has a length of 128 characters)
- `Amt[int]`: about 4 bytes
- `Tdate[date]`: about 8 bytes (considered as `datetime` of MySQL)
- `BuId[string]`: about 128 bytes (same as `Id`)
- `PiId[string]`: about 128 bytes (same as `Id`)

By adding these fields together we can easily know:
- Size of a single `Trans`: about 268 bytes
- Size of 4-year history transaction data: 
  - 14.6 billion `Trans`
  - about 5.26 TB
- Size of stream data:
  - 10 million trans/day = 416.67K trans/hour = 116 trans/second
  - about 3.69 GB/day = 157.71 MB/hour = 44.86 KB/second

### Data accesses

From the problem description we know that 2 types of data accesses are required:
1. Given `PiId` and `BuId`, list all related `Trans`
2. Given `BuId`, graph recurrent customers over time month by month.

Note that both 2 data accesses are interactive, which raises performance requirements for solution architecture, data 
transformations, data fetching queries, etc. 

#### Access 1

For this access, assume given `PiId` is `Pi0` and given `BuId` is `Bu0`. 

The problem can be translated to a pseudo SQL statement:
```roomsql
SELECT * FROM Transactions
WHERE PiId = "Pi0" AND BuId = "Bu0"
```

#### Access 2

For this access, we need to know how a recurrent customer is defined which we can easily find in the problem description:
>A recurrent customer for month M is one that has made more than 1 transaction between M-12 (included) and M (excluded).

And this access requires us to calculate the recurrent customer ratio for each month:
>The data point for each month is the ratio between the number of recurrent customers over the total number of customers.

From the definition and requirement we can set up equation below for the calculation of recurrent customer ratio:
>![RecurRatio](https://latex.codecogs.com/svg.latex?RecurRatio_%7BBu%7D%28M%29%3D%5Cfrac%7BCount%28RecurPi_%7BBu%7D%28M%29%29%7D%7BCount%28AllPi_%7BBu%7D%28M%29%29%7D)

where:
- RecurRatio<sub>Bu</sub>(M): recurrent customer ratio for given month `M` and business `Bu`.
- RecurPi<sub>Bu</sub>(M): set of recurrent customers for given month `M` and business `Bu` (Note that payment 
  instrument `Pi` is used as customer identifier here).
- AllPi<sub>Bu</sub>(M): set of all customers for given month `M` and business `Bu`. 

We can describe AllPi<sub>Bu</sub>(M) and RecurPi<sub>Bu</sub>(M) by a pseudo SQL statement for each:

- AllPi<sub>Bu</sub>(M):
  ```roomsql
  SELECT DISTINCT PiId FROM Transactions
  WHERE BuId = "Bu"
    AND Tdate IN M
  ```

- RecurPi<sub>Bu</sub>(M):
  ```roomsql
  SELECT DISTINCT PiId FROM Transactions
  WHERE BuId = "Bu"
    AND Tdate IN M
    AND PiId IN (
        SELECT PiId FROM #Lpi_Bu_M
        )
  ```
  where `#Lpi_Bu_M`(denoted Lpi<sub>Bu</sub>(M) in the equations) stands for set of customers having purchased in the 
  past 12 months (from M-12 included to M excluded) for a given business `Bu`. Lpi<sub>Bu</sub>(M) can also be 
  described by a pseudo SQL statement:
  ```roomsql
  SELECT DISTINCT PiId FROM Transactions
  WHERE BuId = "Bu"
    AND Tdate BETWEEN M-12 AND M
  ```
  
To sum everything up for calculation of recurrent customer ratio:
1. Get AllPi<sub>Bu</sub>(M)
2. Get Lpi<sub>Bu</sub>(M)
3. Get RecurPi<sub>Bu</sub>(M) by using Lpi<sub>Bu</sub>(M)
4. Calculate RecurRatio<sub>Bu</sub>(M)

## Solution architecture

### Data storage

From problem analysis we know:
- The whole historical data volume is about 5.26 TB and all required data accesses will
  query these data. 
- Most of the access queries select `PiId`, and filter on `BuId` and `Tdate`

In this case, we could use Hadoop ecosystem and build a cluster:
- Data Storage: HDFS
  - Data can be distributed on HDFS throughout the whole cluster
  - Data are replicated to have a fault-tolerant feature
- SQL-like query engine and data warehouse: Impala
  - Table can be stored in Parquet format (configured via Impala) to achieve shorter query time and to have less disk 
    consumption
  - Unlike MapReduce which created huge amount of disk I/O, Impala benefits from the memory usage during calculation
  - By using a `PARTITIONED BY` clause on `BuId`, `year(Tdate)` and `month(Tdate)` while creating the `Transactions` 
    table, data are physically divided in directories on HDFS so that when we query the table with `WHERE` clause on 
    `BuId` column and/or on `Tdate` column, much fewer data will be loaded, thus a shorter response time.

The Hadoop ecosystem architecture will look like this:
![Hadoop_architecture](https://www.itbusinessedge.com/wp-content/uploads/2021/02/cloudera-delivers-on-impala-sql-promise-for-hadoop_6037f2345fe4a.png)

### Data streaming pipeline

To handle with the incoming transaction stream, we could combine:
- Kafka which plays a role of streaming source for Spark and ensures more-than-once message delivery
- Spark's Structured Streaming API 
  - which brings the Spark Core and SQL capabilities to stream data processing
  - which applies Spark's Dataframe/Dataset API to streaming data as each incoming transaction could be considered and
    processed as a row of `Dataframe` or `Dataset`
  - which could deliver end-to-end exactly-once semantics by combining a replayable streaming source (in this case, 
    Kafka) and an idempotent sink (in this case we have transaction id thus idempotent)

Once streaming data are processed, they will be stored in a cache layer and then flushed to the sink periodically.

The streaming pipeline will look like this:
![streaming_pipeline](https://databricks.com/wp-content/uploads/2017/04/structured-streaming-kafka-blog-image-1-overview.png)

### Caching

Caching is essential as it accelerates query time in general as well as reduces work on database or data storage side. 
We could use Redis as a cache layer since Redis is a matured, in-memory, scalable database, which is quite suitable for 
data caching.

1. From data access type 2 we know that a recurrent customer ratio for any previous month will remain unchanged.
Thus, all the recurrent customer ratios for previous N months could be pre-calculated and cached in Redis for a shorter 
query time.
2. Also, for data access type 2, we could cache several data for current month M: 
   1. Lpi<sub>Bu</sub>(M) because it's immutable in time
   2. AllPi<sub>Bu</sub>(M) in the form of Set and will be updated by streaming data
   3. RecurPi<sub>Bu</sub>(M) in the form of Set and will be updated by streaming data
3. For data access type 1 which lists all transactions given by a `BuId` and a `PiId`, we could set a query 
management mechanism so that most frequent query results will be cached.

>Note: For point 2, all these 3 data measures (Lpi<sub>Bu</sub>(M), AllPi<sub>Bu</sub>(M)) and RecurPi<sub>Bu</sub>(M)) 
>are recalculated in the beginning of every month.

## Data processing jobs

Several scripts are created for different purpose:
- [StructuredStreamingApp](src/main/java/StructuredStreamingApp.java):
  - Streaming App which:
    - reads streaming data from source (file in demo, Kafka in production) 
    - updates cache (AllPi<sub>Bu</sub>(M) and RecurPi<sub>Bu</sub>(M) in Redis)
    - saves to sink (Redis in demo, Impala in production)
- [RecurrentCustomerRatioCalculator](src/main/java/RecurrentCustomerRatioCalculator.java):
  - Batch processing for:
    - Calculating recurrent customer ratio for current month
    - Getting historical recurrent customer ratio data along with the one of current month
- [LpiCacheUpdater](src/main/java/LpiCacheUpdater.java):
  - Batch processing at beginning of every month for:
    - Recalculating Lpi<sub>Bu</sub>(M)
    - Clearing and recalculating AllPi<sub>Bu</sub>(M)
    - Clearing and recalculating RecurPi<sub>Bu</sub>(M)
- [CacheInitializer](src/main/java/CacheInitializer.java):
  - Batch processing job to initiate system state, especially foobar data and cache initialization.
