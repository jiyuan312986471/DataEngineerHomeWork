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
- Amount[int] (denoted `Amt`)
- PurchaseDate[date] (denoted `Tdate`)
- BusinessId[string] (denoted `BuId`)
- PaymentInstrumentId[string] (denoted `PiId`)

In order not to make problem more complex, we assume that each customer has only one payment instrument. Thus, our `PiId`
could be used as an **IDENTIFIER** for customers.

### Data volume

We can roughly estimate the data size as following: 
- `Amt[int]`: about 4 bytes
- `Tdate[date]`: about 8 bytes (considered as `datetime` of MySQL)
- `BuId[string]`: about 128 bytes (assume this field has a length of 128 characters)
- `PiId[string]`: about 128 bytes (same as `BuId`)

By adding these fields together we can easily know:
- Size of a single `Trans`: about 268 bytes
- Size of 4-year history transaction data: 
  - 14.6 billion `Trans`
  - about 3.56 TB
- Size of stream data:
  - 10 million trans/day = 416.67K trans/hour = 116 trans/second
  - about 2.50 GB/day = 106.73 MB/hour = 30.36 KB/second

### Data accesses

From the problem description we know that 2 types of data accesses are required:
1. Given `PiId` and `BuId`, list all related `Trans`
2. Given `BuId`, graph recurrent customers over time month by month.

Note that both 2 data accesses are interactive, which raises performance requirements for solution architecture, data 
transformations, data fetching queries, etc. 

#### Access 1

For this access, assume given `PiId` is `Pi0` and given `BuId` is `Bu0`. 

The problem can be translated to an SQL 
statement:
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
- RecurRatio<sub>Bu</sub>(M) stands for recurrent customer ratio for given month `M` and business `Bu`.
- RecurPi<sub>Bu</sub>(M) stands for set of recurrent customers for given month `M` and business `Bu` (Note that payment 
  instrument `Pi` is used as customer identifier here).
- AllPi<sub>Bu</sub>(M) stands for set of all customer for given month `M` and business `Bu`. 


