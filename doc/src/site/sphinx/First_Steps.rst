First steps
***********

We are going to introduce how to use our MongoDB datasource for Apache Spark.

Table of Contents
*****************

-  `Using the library <#using-the-library>`__

-  `Configuration parameters <#configuration-parameters>`__

-  `Examples <#examples>`__

   -  `Scala API <#scala-api>`__
   -  `Python API <#python-api>`__
   -  `R API <#r-api>`__



Using the library
=================

There are two ways of using Spark-Mongodb library:

You can link against this library by putting the following lines in your program:

::

 <groupId>com.stratio.datasource</groupId>
 <artifactId>spark-mongodb_2.11</artifactId>
 <version>LATEST_VERSION</version>

There also exists the possibility of downloading the project by doing:

::

 git clone https://github.com/Stratio/spark-mongodb.git
 mvn clean install

In order to add the spark-mongodb jar file to Spark, you can use the --packages or --jars command line option.
For example, to include it when starting the spark shell:


Spark packages option:

::

 $ bin/spark-shell --packages com.stratio.datasource:spark-mongodb_2.11:<VERSION>


Jars option:

::

 $ bin/spark-shell --jars <path-to>/spark-mongodb_2.11-<version>.jar,<path-to>/casbah-commons_2.11-2.8.0.jar,<path-to>/casbah-core_2.11-2.8.0.jar,<path-to>/casbah-query_2.11-2.8.0.jar,<path-to>/mongo-java-driver-2.13.0.jar,<path-to>/akka-actor_2.11-2.3.11.jar,<path-to-typesafeconfig>/config-1.2.1.jar

::

 Welcome to
       ____              __
      / __/__  ___ _____/ /__
     _\ \/ _ \/ _ `/ __/  '_/
    /___/ .__/\_,_/_/ /_/\_\   version 2.0.0
       /_/
 
 Using Scala version 2.11.8 (Java HotSpot(TM) 64-Bit Server VM, Java 1.8.0_65)
 Type in expressions to have them evaluated.
 Type :help for more information.

 Spark context available as sc.



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
| writeConcern                                  | "safe"                                                                         | No                      |
+-----------------------------------------------+--------------------------------------------------------------------------------+-------------------------+
| splitSize                                     |       10                                                                       | No                      |
+-----------------------------------------------+--------------------------------------------------------------------------------+-------------------------+
| splitKey                                      | "fieldName"                                                                    | No                      |
+-----------------------------------------------+--------------------------------------------------------------------------------+-------------------------+
| splitKeyType                                  | "dataTypeName"                                                                 | No                      |
+-----------------------------------------------+--------------------------------------------------------------------------------+-------------------------+
| splitKeyMin                                   | "minvalue"                                                                     | No                      |
+-----------------------------------------------+--------------------------------------------------------------------------------+-------------------------+
| splitKeyMax                                   | "maxvalue"                                                                     | No                      |
+-----------------------------------------------+--------------------------------------------------------------------------------+-------------------------+
| credentials                                   |  "user,database,password;user,database,password"                               | No                      |
+-----------------------------------------------+--------------------------------------------------------------------------------+-------------------------+
| updateFields                                  |  "fieldName,fieldName"                                                         | No                      |
+-----------------------------------------------+--------------------------------------------------------------------------------+-------------------------+
| sslOptions                                    |  "/path/keystorefile,keystorepassword,/path/truststorefile,truststorepassword" | No                      |
+-----------------------------------------------+--------------------------------------------------------------------------------+-------------------------+
| readPreference                                |  "nearest"                                                                     | No                      |
+-----------------------------------------------+--------------------------------------------------------------------------------+-------------------------+
| language                                      |  "en"                                                                          | No                      |
+-----------------------------------------------+--------------------------------------------------------------------------------+-------------------------+
| connectTimeout                                |   "10000"                                                                      | No                      |
+-----------------------------------------------+--------------------------------------------------------------------------------+-------------------------+
| connectionsPerHost                            |   "10"                                                                         | No                      |
+-----------------------------------------------+--------------------------------------------------------------------------------+-------------------------+
| maxWaitTime                                   |   "10000"                                                                      | No                      |
+-----------------------------------------------+--------------------------------------------------------------------------------+-------------------------+
| socketTimeout                                 |   "1000"                                                                       | No                      |
+-----------------------------------------------+--------------------------------------------------------------------------------+-------------------------+
| threadsAllowedToBlockForConnectionMultiplier  |   "5"                                                                          | No                      |
+-----------------------------------------------+--------------------------------------------------------------------------------+-------------------------+
| idAsObjectId                                  |   "false"                                                                      | No                      |
+-----------------------------------------------+--------------------------------------------------------------------------------+-------------------------+
| connectionsTime                               |   "180000"                                                                     | No                      |
+-----------------------------------------------+--------------------------------------------------------------------------------+-------------------------+
| cursorBatchSize                               |   "101"                                                                        | No                      |
+-----------------------------------------------+--------------------------------------------------------------------------------+-------------------------+
| bulkBatchSize                                 |   "1000"                                                                       | No                      |
+-----------------------------------------------+--------------------------------------------------------------------------------+-------------------------+


**Note:** '_id' field is autogenerated in MongoDB, so by default, you can filter it as String. If you need a custom '_id', you have to set 'idasobjectid' property to "false" like in the above table.

There are two ways to set up configuration:

1. Using MongodbConfigBuilder, which should contains all the config with the right types.

2. Using DataFrame API to read/write with a Map[String, String] or setting configuration from a SQL sentence (in String to String format) as in:

   ::

      CREATE TEMPORARY TABLE tableName USING com.stratio.datasource.mongodb
      OPTIONS (host 'host:port', database 'highschool', collection 'students')"


Credentials
-----------

To connect with credentials you should specify user, database and password.

From MongodbConfigBuilder, you have to create a list of MongodbCredentials, here is an example:

::

    MongodbConfigBuilder(Map(Host -> List("localhost:27017"), Database -> "highschool", Collection ->"students",
    List(com.stratio.datasource.mongodb.MongodbCredentials(user, database, password.toCharArray))
    )).build


In other case, (String format) you have to use the format set in the table above.

    One credential:

    ::

        "user,database,password"



    Two credentials:

    ::

        "user1,database1,password1;user2,database2,password2"



SplitKey parameters
-------------------

An index is needed in the splitKey field.

All splitKey parameters are optionals.

    splitKey: Field to split for.

    splitSize: Max size of each chunk in MB.

If you want to use explicit boundaries to choose what data get from MongoDB, you will have to use these parameters:

    - splitKeyType: Data type of splitKey field. Next MongoDB types are supported:
        - "isoDate"
        - "int"
        - "long"
        - "double"
        - "string"

    - splitKeyMin: Min value of the split in string format.

    - splitKeyMax: Max value of the split in string format.

    **Note:** Only data between boundaries would be available


Examples
========

Scala API
---------

Launch the spark shell:
::

 $ bin/spark-shell --packages com.stratio.datasource:spark-mongodb_2.10:<VERSION>

If you are using the spark shell, a SQLContext is already created and is available as a variable: 'sqlContext'.
Alternatively, you could create a SQLContext instance in your spark application code:

::

 val sqlContext = new SQLContext(sc)

To read a DataFrame from a Mongo collection, you can use the library by loading the implicits from `com.stratio.datasource.mongodb._`.

To save a DataFrame in MongoDB you should use the saveToMongodb() function as follows:

::

 import org.apache.spark.sql._
 import com.mongodb.casbah.{WriteConcern => MongodbWriteConcern}
 import com.stratio.datasource.mongodb._
 import com.stratio.datasource.mongodb.config._
 import com.stratio.datasource.mongodb.config.MongodbConfig._

 val sparkSession = SparkSession.builder().getOrCreate()
 case class Student(name: String, age: Int)
 val dataFrame: DataFrame = sparkSession.createDataFrame(sc.parallelize(List(Student("Torcuato", 27), Student("Rosalinda", 34))))
 val saveConfig = MongodbConfigBuilder(Map(Host -> List("localhost:27017"), Database -> "highschool", Collection ->"students", SamplingRatio -> 1.0, WriteConcern -> "normal", SplitSize -> 8, SplitKey -> "_id"))
 dataFrame.saveToMongodb(saveConfig.build)


In the example we can see how to use the fromMongoDB() function to read from MongoDB and transform it to a DataFrame.

::

 import org.apache.spark.sql._
 import com.mongodb.casbah.{WriteConcern => MongodbWriteConcern}
 import com.stratio.datasource.mongodb._
 import com.stratio.datasource.mongodb.config._
 import com.stratio.datasource.mongodb.config.MongodbConfig._

 val builder = MongodbConfigBuilder(Map(Host -> List("localhost:27017"), Database -> "highschool", Collection ->"students", SamplingRatio -> 1.0, WriteConcern -> "normal"))
 val readConfig = builder.build()
 val mongoRDD = sparkSession.sqlContext.fromMongoDB(readConfig)
 mongoRDD.createTempView("students")
 val dataFrame = sparkSession.sql("SELECT name, age FROM students")
 dataFrame.show


If you want to use a SSL connection, you need to add this 'import', and add 'SSLOptions' to the MongodbConfigBuilder:

::

 import com.stratio.datasource.mongodb.MongodbSSLOptions._
 val builder = MongodbConfigBuilder(Map(Host -> List("localhost:27017"), Database -> "highschool", Collection -> "students", SamplingRatio -> 1.0, WriteConcern -> MongodbWriteConcern.Normal, SSLOptions -> MongodbSSLOptions("<path-to>/keyStoreFile.keystore","keyStorePassword","<path-to>/trustStoreFile.keystore","trustStorePassword")))


Using  StructType:

::


 import org.apache.spark.sql.types._
 val schemaMongo = StructType(StructField("name", StringType, true) :: StructField("age", IntegerType, true ) :: Nil)
 val df = sparkSession.read.schema(schemaMongo).format("com.stratio.datasource.mongodb").options(Map("host" -> "localhost:27017", "database" -> "highschool", "collection" -> "students")).load
 df.createTempView("mongoTable")
 sparkSession.sql("SELECT * FROM mongoTable WHERE name = 'Torcuato'").show()


Using DataFrameWriter:

::

 import org.apache.spark.sql._

 val options = Map("host" -> "localhost:27017", "database" -> "highschool", "collection" -> "students")
 case class Student(name: String, age: Int)
 val sparkSession = SparkSession.builder().getOrCreate()
 val dfw: DataFrame = sparkSession.createDataFrame(sc.parallelize(List(Student("Michael", 46))))
 dfw.write.format("com.stratio.datasource.mongodb").mode(SaveMode.Append).options(options).save()
 val df = sparkSession.read.format("com.stratio.datasource.mongodb").options(options).load
 df.show


Using HiveContext:

::

 sparkSession.sql("CREATE TABLE IF NOT EXISTS mongoTable(name STRING, age INTEGER) USING com.stratio.datasource.mongodb OPTIONS (host 'localhost:27017', database 'highschool', collection 'students')")
 sparkSession.sql("SELECT * FROM mongoTable WHERE name = 'Torcuato'").show()
 sparkSession.sql("DROP TABLE mongoTable")

Using spark-sql shell:

::

 CREATE TEMPORARY VIEW mongoTable USING com.stratio.datasource.mongodb OPTIONS (host 'host:port', database 'highschool', collection 'students');
 SELECT * FROM mongoTable WHERE name = 'Torcuato';


Python API
----------

Mongo data can be queried from Python too:

First, enter the pyspark shell from your SPARK_HOME.

::

 $ bin/pyspark --packages com.stratio.datasource:spark-mongodb_2.11:<VERSION>

Then:

::

 from pyspark.sql import SparkSession
 spark = SparkSession.builder.getOrCreate()
 spark.sql("CREATE TEMPORARY VIEW students_table USING com.stratio.datasource.mongodb OPTIONS (host 'localhost:27017', database 'highschool', collection 'students')")
 spark.sql("SELECT * FROM students_table").collect()

Using DataFrameReader and DataFrameWriter:
::

 df = sparkSession.read.format('com.stratio.datasource.mongodb').options(host='localhost:27017', database='highschool', collection='students').load()
 df.select("name").collect()

 df.select("name").write.format("com.stratio.datasource.mongodb").mode('overwrite').options(host='localhost:27017', database='highschool', collection='studentsview').save()
 dfView = sparkSession.read.format('com.stratio.datasource.mongodb').options(host='localhost:27017', database='highschool', collection='studentsview').load()
 dfView.show()

Java API
--------

You need to add spark-mongodb and spark-sql dependencies to the java project.
::

public class SparkMongodbJavaExample {

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder().master("local[2]).getOrCreate();

        Map options = new HashMap();
        options.put("host", "localhost:27017");
        options.put("database", "highschoolCredentials");
        options.put("collection", "students");
        options.put("credentials", "user,highschoolCredentials,password");

        DataFrame df = spark.read().format("com.stratio.datasource.mongodb").options(options).load();
        df.createTempView("students");
        spark.sql("SELECT * FROM students");
        df.show();        }
}

R API
-----
Mongo data can also be queried from SparkR (sparkR shell example):

First, enter the SparkR shell from your SPARK_HOME.

::

 $ bin/sparkR --packages com.stratio.datasource:spark-mongodb_2.11:<VERSION>

Then:

::

 # credentials and samplingratio are optionals.
 df <- read.df(sqlContext, source= "com.stratio.datasource.mongodb", host = "host:port", database = "highschool", collection = "students", splitSize = 8, splitKey = "_id", credentials="user1,database,password;user2,database2,password2", samplingRatio=1.0)
 createOrReplaceTempView(df, "students_table")
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

