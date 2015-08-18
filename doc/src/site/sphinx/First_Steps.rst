First steps
***********

We are going to introduce how to use our MongoDB provider for Apache Spark.

Using the library
=================

There are two ways of using Spark-Mongodb library:

You can link against this library by putting the following lines in your program:

::

 <groupId>com.stratio</groupId>
 <artifactId>spark-mongodb-core</artifactId>
 <version>LATEST_VERSION</version>

There also exists the possibility of downloading the project by doing:

::

 git clone https://github.com/Stratio/spark-mongodb.git
 mvn clean install

In order to add the spark-mongodb jar file to Spark, you can use the --jars command line option.
For example, to include it when starting the spark shell:

::

$ bin/spark-shell --jars <path-to>/spark-mongodb-core-<version>.jar,<path-to>/casbah-commons_2.10-2.8.0.jar,<path-to>/casbah-core_2.10-2.8.0.jar,<path-to>/casbah-query_2.10-2.8.0.jar,<path-to>/mongo-java-driver-2.13.0.jar

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



Connection options
==================

+-------------------------+--------------------------------------------------------------------------------+
|      Option             |    Format  example                                                             |
+=========================+================================================================================+
| "host"                  | "host:port,host:port"                                                          |
+-------------------------+--------------------------------------------------------------------------------+
| "database"              | "databaseName"                                                                 |
+-------------------------+--------------------------------------------------------------------------------+
| "collection"            | "collectionName"                                                               |
+-------------------------+--------------------------------------------------------------------------------+
| "schema_samplingRatio"  |      1.0                                                                       |
+-------------------------+--------------------------------------------------------------------------------+
| "writeConcern"          | mongodb.WriteConcern.ACKNOWLEDGED                                              |
+-------------------------+--------------------------------------------------------------------------------+
| "splitSize"             |       10                                                                       |
+-------------------------+--------------------------------------------------------------------------------+
| "splitKey"              | "fieldName"                                                                    |
+-------------------------+--------------------------------------------------------------------------------+
| "allowSlaveReads"       |      false                                                                     |
+-------------------------+--------------------------------------------------------------------------------+
| "credentials"           |  "user,database,password;user,database,password"                               |
+-------------------------+--------------------------------------------------------------------------------+
| "_idField"              | "fieldName"                                                                    |
+-------------------------+--------------------------------------------------------------------------------+
| "searchFields"          |  "fieldName,fieldName"                                                         |
+-------------------------+--------------------------------------------------------------------------------+
| "ssloptions"            |  "/path/keystorefile,keystorepassword,/path/truststorefile,truststorepassword" |
+-------------------------+--------------------------------------------------------------------------------+
| "readpreference"        |  "nearest"                                                                     |
+-------------------------+--------------------------------------------------------------------------------+
| "language"              |  "en"                                                                          |
+-------------------------+--------------------------------------------------------------------------------+


Scala API
---------

To read a DataFrame from a Mongo collection, you can use the library by loading the implicits from `com.stratio.provider.mongodb._`.

::

 import com.mongodb.casbah.{WriteConcern => MongodbWriteConcern}
 import com.stratio.provider._
 import com.stratio.provider.mongodb._
 import com.stratio.provider.mongodb.schema._
 import com.stratio.provider.mongodb.writer._
 import org.apache.spark.sql.SQLContext
 import DeepConfig._
 import MongodbConfig._
 val builder = MongodbConfigBuilder(Map(Host -> List("host:port"), Database -> "highschool", Collection -> "students", SamplingRatio -> 1.0, WriteConcern -> MongodbWriteConcern.Normal))
 val readConfig = builder.build()
 val mongoRDD = sqlContext.fromMongoDB(readConfig)
 mongoRDD.registerTempTable("students")
 sqlContext.sql("SELECT name, age FROM students")


In the example we can see how to use the fromMongoDB() function to read from MongoDB and transform it to a DataFrame.

To save a DataFrame in MongoDB you should use the saveToMongodb() function as follows:

::

 import org.apache.spark.sql._
 val sqlContext = new SQLContext(sc)
 import sqlContext._
 case class Student(name: String, age: Int)
 val dataFrame: DataFrame = createDataFrame(sc.parallelize(List(Student("Torcuato", 27), Student("Rosalinda", 34))))
 import com.mongodb.casbah.{WriteConcern => MongodbWriteConcern}
 import com.stratio.provider.mongodb._
 import MongodbConfig._
 val saveConfig = MongodbConfigBuilder(Map(Host -> List("host:port"), Database -> "highschool", Collection -> "students", SamplingRatio -> 1.0, WriteConcern -> MongodbWriteConcern.Normal, SplitSize -> 8, SplitKey -> "_id", SplitSize -> 8, SplitKey -> "_id"))
 dataFrame.saveToMongodb(saveConfig.build)


Python API
----------

Mongo data can be queried from Python too:

::

 from pyspark.sql import SQLContext
 sqlContext.sql("CREATE TEMPORARY TABLE students_table USING com.stratio.provider.mongodb OPTIONS (host 'host:port', database 'highschool', collection 'students')")
 sqlContext.sql("SELECT * FROM students_table").collect()


R API
-----
Mongo data can also be queried from SparkR (sparkR shell example):

First, enter the SparkR shell from your SPARK_HOME.

::

 $ bin/sparkR --jars <path-to>/spark-mongodb-core-<version>.jar,<path-to>/casbah-commons_2.10-2.8.0.jar,<path-to>/casbah-core_2.10-2.8.0.jar, <path-to>/casbah-query_2.10-2.8.0.jar,<path-to>/mongo-java-driver-2.13.0.jar

Then:

::

 # credentials and samplingratio are optionals.
 df <- read.df(sqlContext, source= "com.stratio.provider.mongodb", host = "host:port", database = "highschool", collection = "students", splitSize = 8, splitKey = "_id", credentials="user1,database,password;user2,database2,password2", samplingRatio=1.0)
 registerTempTable(df, "students_table")
 collect(sql(sqlContext, "SELECT * FROM students_table"))



SSL support
-----------

If you want to use a SSL connection, you need to add some options to the previous examples:

Scala API 
---------

For both Scala examples you need to add this 'import', and add 'SSLOptions' to the MongodbConfigBuilder:

::

 import com.stratio.provider.mongodb.MongodbSSLOptions._
 val builder = MongodbConfigBuilder(Map(Host -> List("host:port"), Database -> "highschool", Collection -> "students", SamplingRatio -> 1.0, WriteConcern -> MongodbWriteConcern.Normal, SSLOptions -> MongodbSSLOptions("<path-to>/keyStoreFile.keystore","keyStorePassword","<path-to>/trustStoreFile.keystore","trustStorePassword")))


Python API 
----------
In this case you only need to add SSL options when you create the temporary table in the specified format below:

::

 sqlContext.sql("CREATE TEMPORARY TABLE students_table USING com.stratio.provider.mongodb OPTIONS (host 'host:port', database 'databaseName', collection 'collectionName', ssloptions '<path-to>/keyStoreFile.keystore,keyStorePassword,<path-to>/trustStoreFile.keystore,trustStorePassword')")


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

