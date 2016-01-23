#Twitter Trends Hbase
*by Guilherme Dinis Jr and Philipp Eisen*

## Design Decisions for creating the Hbase Schema
###Row Key

When designing the schema for the HBase database of our application,
the most important decision was how we would compose our rowKey. We were thinking
about several possible designs, of which each had its up and down sides.
Finally, we decided to use the following design:

|                       | (CF) hashTag |     |     | (CF) meta |
|-----------------------|--------------|-----|-----|-----------|
| (rowKey)timestampLang | xxx          | yyy | zzz | lang      |
|                       | 3            | 2   | 1   | en        |
|                       |              |     |     |           |


In this schema the rowKey consists of a concat of timestamp and language.

This design has one major downside when used in an application that frequently
and in large volumes adds new data. New in this case is referred to more recent
data, thus data with a higher timestamp. In this scenario only the last table
region would receive insertions. In a distributed system this means uneven load
amongst the nodes of a cluster. Furthermore, it would require for frequent
rebalancing of the table regions amongst the nodes.

One of many possible solutions to go about this problem could be to use the concat
langTimestamp instead of timestampLang. However, this would make range queries by
ranges of timestamps impossible. The only possible range query would be to retrieve
all entries of one language.

This leads us to the upsides of using the chosen design. It allows for range queries
by timestamps. Since all three queries to be run require a scan by timestamp this
increases the performance of the queries. The three queries are the following:
query set is composed by 3 queries:

    1. Given a language (lang), do find the Top-N most used words for the given language in a time interval defined with a start and end timestamp. Start and end timestamp are in milliseconds.
    2. Do find the list of Top-N most used words for each language in a time interval defined with the provided start and end timestamp. Start and end timestamp are in milliseconds.
    3. Do find the Top-N most used words and the frequency of each word regardless the language in a time interval defined with the provided start and end timestamp. Start and end timestamp are in milliseconds.

Allowing for range queries by timestamp allows for good performance on all the three queries.
However, for query 1 and query 2 on the sever side we still scan more data then we need to process
the query. While this doesn't add to less performance on the application side it still is costly on
the server side, since more than necessary data is being loaded from HDFS. Still, this was theb best
solution we could think of. 

Finally, another reason why we chose to decide for timestampLang as rowKey was the fact that
in this specific application the data is only inserted once and therefore they positive effects of
more efficient queries outweigh the negative effect of writing the data.




###Subsequent design decisions

In order to perform the required filters for query 1 and 2 our design includes a column
``meta.lang`` that contains the language as value.

The hashtags are saved as columns with their count for the time window as value.
This allows for efficient handling on the application side once the required entries are
retrieved.




##Running App

First, build it with maven:

```
mvn clean compile package assembly:single
```
or create application script

```
mvn clean compile package appassembler:assemble
```

We need to insert data before running any queries:

```
java -jar {$PATHTO}/TwitterHBase-1.0-SNAPSHOT-jar-with-dependencies.jar 4 {PATH_TO_HUGE_LOG_FILE}
```

Insertion should take a little while, depending on your data volume. Once data is inserted, you can run the queries on the data:

  - Query 1: ${APP}/hbaseApp.sh 1 startTS endTS N language outputFolder
  - Query 2: ${APP}/hbaseApp.sh 2 startTS endTS N lang-1,lang-2,lang-3 outputFolder
  - Query 3: ${APP}/hbaseApp.sh 3 startTS endTS N outputFolder
