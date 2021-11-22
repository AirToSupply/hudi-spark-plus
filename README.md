# hudi-spark-plus

A library based on Hudi for Spark.

## Requirements

The library currently supports the following versions of components：

（1）Scala：2.12.x

（2）Spark：3.1.x

（3）Hudi：0.9.0

## Features

In addition to the basic features of data lake through Hudi(https://github.com/apache/hudi), it has the ability to consume upstream CDC change logs, and customize the sink connector to synchronize multiple tables of RDBMS into data lake. It greatly improves the development cost of streaming CDC change logs into warehousing.

## How to do?

We use Spark CDC scheme to obtain upstream change log records. We need to thank allwefantasy for providing it with a dependency library spark-binlog(https://github.com/allwefantasy/spark-binlog). 

Unfortunately, the latest version of this dependency library is 1.0.4, and Spark 3.1.x is not supported. We have made a patch related to spark 3.1.x to recommend this work. This part of the code is not in the mavne public warehouse for the time being.

You need to introduce spark binlog and the current dependency library. Note that you should not forget to introduce your database connection driver. Currently, only supported MySQL.

The codes involved are as follows：

```scala
val spark = SparkSession.builder()
      .appName("spark-cdc-hudi-sync")
      .getOrCreate()

val df = spark.readStream.
  format("mysql-binlog").
  option("host", "127.0.0.1").
  option("port", "3306").
  option("userName", "root").
  option("password", "123456").
  option("databaseNamePattern", "db_issue_clear").
  option("tableNamePattern", "person|student").
  option("bingLogNamePrefix", "mysql-bin").
  option("binlogIndex", "10").
  option("binlogFileOffset", "90840").
  load()

val query = df.writeStream.
  format("binlog-hudi").
  outputMode("append").
  option("path", "").
  option("mode", "Append").
  option("option.hoodie.path", "/hudi/tmp/{db}/ods_{db}_{table}").
  option("checkpointLocation", "/hudi/tmp/spark-cdc-hudi-sync-suite/").
  // person
  option("db_issue_clear.person.hoodie.base.path", "/hudi/tmp/db_issue_clear/ods_db_issue_clear_person").
  option("db_issue_clear.person.hoodie.table.name", "ods_db_issue_clear_person").
  option("db_issue_clear.person.hoodie.datasource.write.recordkey.field", "id").
  option("db_issue_clear.person.hoodie.datasource.write.precombine.field", "id").
  option("db_issue_clear.person.hoodie.datasource.write.keygenerator.class", "org.apache.hudi.keygen.NonpartitionedKeyGenerator").
  // student
  option("db_issue_clear.student.hoodie.base.path", "/hudi/tmp/db_issue_clear/ods_db_issue_clear_student").
  option("db_issue_clear.student.hoodie.table.name", "ods_db_issue_clear_student").
  option("db_issue_clear.student.hoodie.datasource.write.recordkey.field", "id").
  option("db_issue_clear.student.hoodie.datasource.write.precombine.field", "id").
  option("db_issue_clear.student.hoodie.datasource.write.keygenerator.class", "org.apache.hudi.keygen.NonpartitionedKeyGenerator").
  trigger(Trigger.ProcessingTime("10 seconds")).
  start()

query.awaitTermination()
``` 

## Note

（1）For the configuration and usage of spark binlog, please refer to: https://github.com/allwefantasy/spark-binlog/blob/master/README.md .

（2）BinlogHoudieDataSource supports multi table synchronization into data lake, and the attribute behavior of each table is different, which requires users to configure different attributes of each data Lake table before it can be correctly consumed in real time.

（3）For Hudi related configuration, please refer to the official website of Hudi. Because there are many Hudi configuration items, these complex configuration items can be found in the Hudi spark datasource module of Hudi project, so users should abide by the following rules when specifying these parameters: ${db_name}.${table_name}.${hoodie_config_key} .
