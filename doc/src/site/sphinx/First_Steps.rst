First steps
***********

We are going to introduce how to use our MongoDB datasource for Apache Spark.

Using the library
=================

There are two ways of using Spark-Mongodb library:

You can link against this library by putting the following lines in your program:

::

 <groupId>com.stratio.datasource</groupId>
 <artifactId>spark-mongodb_2.10</artifactId>
 <version>LATEST_VERSION</version>

There also exists the possibility of downloading the project by doing:

::

 git clone https://github.com/Stratio/spark-mongodb.git
 mvn clean install

In order to add the spark-mongodb jar file to Spark, you can use the --packages or --jars command line option.
For example, to include it when starting the spark shell:


Spark packages option:

::

 $ bin/spark-shell --packages com.stratio.datasource:spark-mongodb_2.10:<VERSION>


Jars option:

::

 $ bin/spark-shell --jars <path-to>/spark-mongodb_2.10-<version>.jar,<path-to>/casbah-commons_2.10-2.8.0.jar,<path-to>/casbah-core_2.10-2.8.0.jar,<path-to>/casbah-query_2.10-2.8.0.jar,<path-to>/mongo-java-driver-2.13.0.jar

::

 Welcome to
       ____              __
      / __/__  ___ _____/ /__
     _\ \/ _ \/ _ `/ __/  '_/
    /___/ .__/\_,_/_/ /_/\_\   version 1.4.0
       /_/
 
 Using Scala version 2.10.4 (OpenJDK 64-Bit Server VM, Java 1.7.0_79)
 Type in expressions to have them evaluated.
 Type :help for more information.
 Spark context available as sc.
 SQL context available as sqlContext.



It is the same in sparkR and pyspark shells.

* Note if you want to use Scala 2.11, you have to change Scala binary version ("2.10" for "2.11" ) in the artifactId.


Configuration parameters
========================

+-----------------------------------------------+--------------------------------------------------------------------------------+-------------------------+
|      Option                                   |    Format  example                                                             |      requested          |
+===============================================+================================================================================+=========================+
| host                                          | "host:port,host:port"                                                          | Yes                     |
+-----------------------------------------------+--------------------------------------------------------------------------------+-------------------------+
| database                                      | "databaseName"                                                                 | Yes                     |
+-----------------------------------------------+--------------------------------------------------------------------------------+-------------------------+
| collection                                    | "collectionName"                                                               | Yes                     |
+-----------------------------------------------+--------------------------------------------------------------------------------+-------------------------+
| schema_samplingRatio                          |      1.0                                                                       | No                      |
+-----------------------------------------------+--------------------------------------------------------------------------------+-------------------------+
| writeConcern                                  | mongodb.WriteConcern.ACKNOWLEDGED                                              | No                      |
+-----------------------------------------------+--------------------------------------------------------------------------------+-------------------------+
| splitSize                                     |       10                                                                       | No                      |
+-----------------------------------------------+--------------------------------------------------------------------------------+-------------------------+
| splitKey                                      | "fieldName"                                                                    | No                      |
+-----------------------------------------------+--------------------------------------------------------------------------------+-------------------------+
| credentials                                   |  "user,database,password;user,database,password"                               | No                      |
+-----------------------------------------------+--------------------------------------------------------------------------------+-------------------------+
| _idField                                      | "fieldName"                                                                    | No                      |
+-----------------------------------------------+--------------------------------------------------------------------------------+-------------------------+
| updateFields                                  |  "fieldName,fieldName"                                                         | No                      |
+-----------------------------------------------+--------------------------------------------------------------------------------+-------------------------+
| ssloptions                                    |  "/path/keystorefile,keystorepassword,/path/truststorefile,truststorepassword" | No                      |
+-----------------------------------------------+--------------------------------------------------------------------------------+-------------------------+
| readpreference                                |  "nearest"                                                                     | No                      |
+-----------------------------------------------+--------------------------------------------------------------------------------+-------------------------+
| language                                      |  "en"                                                                          | No                      |
+-----------------------------------------------+--------------------------------------------------------------------------------+-------------------------+
| connectTimeout                                |   "10000"                                                                      | No                      |
+-----------------------------------------------+--------------------------------------------------------------------------------+-------------------------+
| connectionsPerHost                            |   "100"                                                                        | No                      |
+-----------------------------------------------+--------------------------------------------------------------------------------+-------------------------+
| maxWaitTime                                   |   "10000"                                                                      | No                      |
+-----------------------------------------------+--------------------------------------------------------------------------------+-------------------------+
| socketTimeout                                 |   "1000"                                                                       | No                      |
+-----------------------------------------------+--------------------------------------------------------------------------------+-------------------------+
| threadsAllowedToBlockForConnectionMultiplier  |   "5"                                                                          | No                      |
+-----------------------------------------------+--------------------------------------------------------------------------------+-------------------------+
| idasobjectid                                  |   "false"                                                                      | No                      |
+-----------------------------------------------+--------------------------------------------------------------------------------+-------------------------+

**Note:** '_id' field is autogenerated in MongoDB, so by default, you can filter it as String. If you need a custom '_id', you have to set 'idasobjectid' property to "false" like in the above table.

Examples
========

Scala API
---------

To read a DataFrame from a Mongo collection, you can use the library by loading the implicits from `com.stratio.datasource.mongodb._`.

To save a DataFrame in MongoDB you should use the saveToMongodb() function as follows:

::

 import org.apache.spark.sql._
 import sqlContext._
 case class Student(name: String, age: Int)
 val dataFrame: DataFrame = createDataFrame(sc.parallelize(List(Student("Torcuato", 27), Student("Rosalinda", 34))))
 import com.mongodb.casbah.{WriteConcern => MongodbWriteConcern}
 import com.stratio.datasource.mongodb._
 import MongodbConfig._
 val saveConfig = MongodbConfigBuilder(Map(Host -> List("localhost:27017"), Database -> "highschool", Collection -> "students", SamplingRatio -> 1.0, WriteConcern -> MongodbWriteConcern.Normal, SplitKey -> "_id", SplitSize -> 8, SplitKey -> "_id"))
 dataFrame.saveToMongodb(saveConfig.build)


In the example we can see how to use the fromMongoDB() function to read from MongoDB and transform it to a DataFrame.

::

 import com.mongodb.casbah.{WriteConcern => MongodbWriteConcern}
 import com.stratio.datasource._
 import com.stratio.datasource.mongodb._
 import com.stratio.datasource.mongodb.schema._
 import com.stratio.datasource.mongodb.writer._
 import org.apache.spark.sql.SQLContext
 import Config._
 import MongodbConfig._
 val builder = MongodbConfigBuilder(Map(Host -> List("localhost:27017"), Database -> "highschool", Collection -> "students", SamplingRatio -> 1.0, WriteConcern -> MongodbWriteConcern.Normal))
 val readConfig = builder.build()
 val mongoRDD = sqlContext.fromMongoDB(readConfig)
 mongoRDD.registerTempTable("students")
 sqlContext.sql("SELECT name, age FROM students")



If you want to use a SSL connection, you need to add this 'import', and add 'SSLOptions' to the MongodbConfigBuilder:

::

 import com.stratio.datasource.mongodb.MongodbSSLOptions._
 val builder = MongodbConfigBuilder(Map(Host -> List("localhost:27017"), Database -> "highschool", Collection -> "students", SamplingRatio -> 1.0, WriteConcern -> MongodbWriteConcern.Normal, SSLOptions -> MongodbSSLOptions("<path-to>/keyStoreFile.keystore","keyStorePassword","<path-to>/trustStoreFile.keystore","trustStorePassword")))


Using  StructType:

::


 import org.apache.spark.sql.types._
 val schemaMongo = StructType(StructField("name", StringType, true) :: StructField("age", IntegerType, true ) :: Nil)
 sqlContext.createExternalTable("mongoTable", "com.stratio.datasource.mongodb", schemaMongo, Map("host" -> "localhost:27017", "database" -> "highschool", "collection" -> "students"))
 sqlContext.sql("SELECT * FROM mongoTable WHERE name = 'Torcuato'").show()
 sqlContext.sql("DROP TABLE mongoTable")


Using DataFrameWriter:

::

 import org.apache.spark.sql.SQLContext._
 import org.apache.spark.sql._
 val options = Map("host" -> "localhost:27017", "database" -> "highschool", "collection" -> "students")
 case class Student(name: String, age: Int)
 val dfw: DataFrame = sqlContext.createDataFrame(sc.parallelize(List(Student("Michael", 46))))
 dfw.write.format("com.stratio.datasource.mongodb").mode(SaveMode.Append).options(options).save()
 val df = sqlContext.read.format("com.stratio.datasource.mongodb").options(options).load
 df.show


Using HiveContext (sqlContext in spark-shell provide Hive support):

::

 sqlContext.sql("CREATE TABLE IF NOT EXISTS mongoTable(name STRING, age INTEGER) USING com.stratio.datasource.mongodb OPTIONS (host 'localhost:27017', database 'highschool', collection 'students')")
 sqlContext.sql("SELECT * FROM mongoTable WHERE name = 'Torcuato'").show()
 sqlContext.sql("DROP TABLE mongoTable")

Using spark-sql shell:

::

 CREATE TEMPORARY TABLE mongoTable USING com.stratio.datasource.mongodb OPTIONS (host 'host:port', database 'highschool', collection 'students');
 SELECT * FROM mongoTable WHERE name = 'Torcuato';
 DROP TABLE mongoTable;

Python API
----------

Mongo data can be queried from Python too:

First, enter the pyspark shell from your SPARK_HOME.

::

 $ bin/pyspark --packages com.stratio.datasource:spark-mongodb_2.10:<VERSION>

Then:

::

 from pyspark.sql import SQLContext
 sqlContext.sql("CREATE TEMPORARY TABLE students_table USING com.stratio.datasource.mongodb OPTIONS (host 'host:port', database 'highschool', collection 'students')")
 sqlContext.sql("SELECT * FROM students_table").collect()


R API
-----
Mongo data can also be queried from SparkR (sparkR shell example):

First, enter the SparkR shell from your SPARK_HOME.

::

 $ bin/sparkR --packages com.stratio.datasource:spark-mongodb_2.10:<VERSION>

Then:

::

 # credentials and samplingratio are optionals.
 df <- read.df(sqlContext, source= "com.stratio.datasource.mongodb", host = "host:port", database = "highschool", collection = "students", splitSize = 8, splitKey = "_id", credentials="user1,database,password;user2,database2,password2", samplingRatio=1.0)
 registerTempTable(df, "students_table")
 collect(sql(sqlContext, "SELECT * FROM students_table"))


License
*******

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

