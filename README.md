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


