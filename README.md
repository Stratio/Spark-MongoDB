# Spark-Mongodb

Spark-Mongodb is a library that allows the user to read/write data with [Spark SQL](http://spark.apache.org/docs/latest/sql-programming-guide.html)
from/into MongoDB collections.

[MongoDB](http://www.mongodb.org/ "MongoDB website") provides a documental data model
richer than typical key/value systems. [Spark](http://spark.incubator.apache.org/ "Spark website") is a
fast and general-purpose cluster computing system that can run applications up to 100 times faster than Hadoop.

Integrating MongoDB and Spark gives us a system that combines the best of both
worlds opening to MongoDB the possibility of solving a wide range of new use cases.

## Requirements

This library requires Spark 1.2+

## Using the library

There are two ways of using Spark-Mongodb library:

You can link against this library by putting the following lines in your program:

```
<groupId>com.stratio</groupId>
<artifactId>spark-mongodb</artifactId>
<version>LATEST</version>
```
There also exists the possibility of downloading the project by doing:

```
git clone https://github.com/Stratio/spark-mongodb.git
mvn clean install
```
In order to add the spark-mongodb jar file to Spark, you can use the --jars command line option.
For example, to include it when starting the spark shell:

```
$ bin/spark-shell --jars <path-to>/spark-mongodb-<version>.jar,<path-to>/casbah-commons_2.10-2.8.0.jar,<path-to>/casbah-core_2.10-2.8.0.jar,
<path-to>/casbah-query_2.10-2.8.0.jar,<path-to>/mongo-java-driver-2.13.0.jar

Spark assembly has been built with Hive, including Datanucleus jars on classpath
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 1.2.0
      /_/

Using Scala version 2.10.4 (Java HotSpot(TM) 64-Bit Server VM, Java 1.7.0_76)
Type in expressions to have them evaluated.
Type :help for more information.
15/02/11 13:21:56 Utils: Set SPARK_LOCAL_IP if you need to bind to another address
15/02/11 13:21:56 NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Spark context available as sc.

```

### Scala API

To read a SchemaRDD from a Mongo collection, you can use the library by loading the implicits from `com.stratio.deep.mongodb._`.

```
scala> import com.mongodb.casbah.{WriteConcern => MongodbWriteConcern}

scala> import com.stratio.deep._

scala> import com.stratio.deep.mongodb._

scala> import com.stratio.deep.mongodb.schema._

scala> import com.stratio.deep.mongodb.writer._

scala> import org.apache.spark.sql.SQLContext

scala> import DeepConfig._

scala> import MongodbConfig._

scala> val sqlContext: SQLContext = new SQLContext(sc)

scala> val builder = MongodbConfigBuilder(Map(Host -> List("localhost:27017"), Database -> "highschool", Collection -> "students", SamplingRatio -> 1.0, WriteConcern -> MongodbWriteConcern.Normal))

scala> val readConfig = builder.build()

scala> val mongoRDD = sqlContext.fromMongoDB(readConfig)

scala> mongoRDD.registerTempTable("students")

scala> sqlContext.sql("SELECT name, enrolled FROM students")

scala> val writeConfig = builder.set(Collection,"students2").build()

scala> mongoRDD.saveToMongodb(writeConfig,batch=true)

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

