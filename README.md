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


##Running App

First, build it with maven:

```
mvn clean compile package assembly:single
```

We need to insert data before running any queries:

```
java -jar {$PATHTO}/TwitterHBase-1.0-SNAPSHOT-jar-with-dependencies.jar 4 {PATH_TO_HUGE_LOG_FILE}
```

Insertion should take a little while, depending on your data volume. Once data is inserted, you can run the queries on the data:

  - Query 1: ${APP}/hbaseApp.sh 1 startTS endTS N language outputFolder
  - Query 2: ${APP}/hbaseApp.sh 2 startTS endTS N lang-1,lang-2,lang-3 outputFolder
  - Query 3: ${APP}/hbaseApp.sh 3 startTS endTS N outputFolder

