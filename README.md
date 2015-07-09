# Spark-Mongodb

Spark-Mongodb is a library that allows the user to read/write data with [Spark SQL](http://spark.apache.org/docs/latest/sql-programming-guide.html)
from/into MongoDB collections.

[MongoDB](http://www.mongodb.org/ "MongoDB website") provides a documental data model
richer than typical key/value systems. [Apache Spark](http://spark.incubator.apache.org/ "Spark website") is a
fast and general-purpose cluster computing system that can run applications up to 100 times faster than Hadoop.

Integrating MongoDB and Apache Spark gives us a system that combines the best of both
worlds opening to MongoDB the possibility of solving a wide range of new use cases.

## Requirements

This library requires Apache Spark 1.4+, Scala 10.4+, casbah 2.8+

##Latest compatible versions

Spark-MongoDB | Apache Spark | Mongodb
 :--: | :--: | :--:
0.8.5 | 1.4.0 | 3.0.+
0.8.4 | 1.4.0 | 3.0.+
0.8.3 | 1.4.0 | 3.0.+
0.8.2 | 1.4.0 | 3.0.+
0.8.1 | 1.3.0 | 3.0.+
0.8.0 | 1.2.1 | 3.0.+

## Using the library

There are two ways of using Spark-Mongodb library:

You can link against this library by putting the following lines in your program:

```
<groupId>com.stratio</groupId>
<artifactId>spark-mongodb-core</artifactId>
<version>LATEST_VERSION</version>
```
There also exists the possibility of downloading the project by doing:

```
git clone https://github.com/Stratio/spark-mongodb.git
mvn clean install
```
In order to add the spark-mongodb jar file to Spark, you can use the --jars command line option.
For example, to include it when starting the spark-shell:

```
$ bin/spark-shell --jars <path-to>/spark-mongodb-core-<version>.jar,<path-to>/casbah-commons_2.10-2.8.0.jar,<path-to>/casbah-core_2.10-2.8.0.jar,
<path-to>/casbah-query_2.10-2.8.0.jar,<path-to>/mongo-java-driver-2.13.0.jar

Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 1.4.0
      /_/

Using Scala version 2.10.4 (Java HotSpot(TM) 64-Bit Server VM, Java 1.7.0_80)
Type in expressions to have them evaluated.
Type :help for more information.
Spark context available as sc.
SQL context available as sqlContext.

```

### Scala API

To read a DataFrame from a Mongo collection, you can use the library by loading the implicits from `com.stratio.provider.mongodb._`.

```
scala> import com.mongodb.casbah.{WriteConcern => MongodbWriteConcern}

scala> import com.stratio.provider._

scala> import com.stratio.provider.mongodb._

scala> import com.stratio.provider.mongodb.schema._

scala> import com.stratio.provider.mongodb.writer._

scala> import org.apache.spark.sql.SQLContext

scala> import DeepConfig._

scala> import MongodbConfig._

scala> val builder = MongodbConfigBuilder(Map(Host -> List("host:port"), Database -> "highschool", Collection -> "students", SamplingRatio -> 1.0, WriteConcern -> MongodbWriteConcern.Normal))

scala> val readConfig = builder.build()

scala> val mongoRDD = sqlContext.fromMongoDB(readConfig)

scala> mongoRDD.registerTempTable("students")

scala> sqlContext.sql("SELECT name, age FROM students")

```
In the example we can see how to use the fromMongoDB() function to read from MongoDB and transform it to a DataFrame.

To save a DataFrame in MongoDB you should use the saveToMongodb() function as follows:

```

scala> import org.apache.spark.sql._
scala> import sqlContext._

scala> case class Student(name: String, age: Int)
scala> val dataFrame: DataFrame = createDataFrame(sc.parallelize(List(Student("Torcuato", 27), Student("Rosalinda", 34))))

scala> import com.mongodb.casbah.{WriteConcern => MongodbWriteConcern}
scala> import com.stratio.provider.mongodb._
scala> import MongodbConfig._

scala> val saveConfig = MongodbConfigBuilder(Map(Host -> List("host:port"), Database -> "highschool", Collection -> "students", SamplingRatio -> 1.0, WriteConcern -> MongodbWriteConcern.Normal, SplitSize -> 8, SplitKey -> "_id", SplitSize -> 8, SplitKey -> "_id"))

scala> dataFrame.saveToMongodb(saveConfig.build)

```

### Python API

Mongo data can be queried from Python too:

```
from pyspark.sql import SQLContext

sqlContext = SQLContext(sc)

sqlContext.sql("CREATE TEMPORARY TABLE students_table USING com.stratio.provider.mongodb OPTIONS (host 'host:port', database 'highschool', collection 'students')")

sqlContext.sql("SELECT * FROM students_table").collect()

```
### SSL support

If you want to use a SSL connection, you need to add some options to the previous examples:

#### Scala API 

For both Scala examples you need to add this 'import', and add 'SSLOptions' to the MongodbConfigBuilder:

```
scala> import com.stratio.provider.mongodb.MongodbSSLOptions._
scala> val builder = MongodbConfigBuilder(Map(Host -> List("host:port"), Database -> "highschool", Collection -> "students", SamplingRatio -> 1.0, WriteConcern -> MongodbWriteConcern.Normal, SSLOptions -> MongodbSSLOptions("<path-to>/keyStoreFile.keystore","keyStorePassword","<path-to>/trustStoreFile.keystore","trustStorePassword")))

```

#### Python API 

In this case you only need to add SSL options when you create the temporary table in the specified format below:

```
sqlContext.sql("CREATE TEMPORARY TABLE students_table USING com.stratio.provider.mongodb OPTIONS (host 'host:port', database 'databaseName', collection 'collectionName', ssloptions '<path-to>/keyStoreFile.keystore,keyStorePassword,<path-to>/trustStoreFile.keystore,trustStorePassword')")

```


# License #

Licensed to STRATIO (C) under one or more contributor license agreements.
See the NOTICE file distributed with this work for additional information
regarding copyright ownership.  The STRATIO (C) licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.

