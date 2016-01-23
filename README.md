#Twitter Trends Hbase
*by Guilherme Dinis Jr and Philipp Eisen*

## Design Decisions for creating the Hbase Schema

We decided to have one row for each window and language.
Even though this might not yield the best for the best spread of load amongst the
when inserting, we decided to stick with this design, as it allows for range queries.

|                       | (CF) hashTag |     |     | (CF) meta |
|-----------------------|--------------|-----|-----|-----------|
| (rowKey)timestampLang | xxx          | yyy | zzz | lang      |
|                       | 3            | 2   | 1   | en        |
|                       |              |     |     |           |
