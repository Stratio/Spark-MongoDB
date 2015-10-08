/**
 * Copyright (C) 2015 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.provider.mongodb.examples

import com.mongodb.QueryBuilder
import com.mongodb.casbah.MongoClient
import com.mongodb.casbah.commons.{MongoDBList, MongoDBObject}
import com.stratio.provider.mongodb.examples.DataFrameAPIExample._
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

trait MongoDefaultConstants {
  val Database = "highschool"
  val Collection = "students"
  val MongoHost = "127.0.0.1"
  val MongoPort = 27017
  val MongoProvider = "com.stratio.provider.mongodb"
}

object MongoExampleFunctions {

  def withSQLContext(block: SQLContext => Unit) = {

    val sparkConf = new SparkConf().
      setAppName("MongoDFExample").
      setMaster("local[4]")

    val sc = new SparkContext(sparkConf)
    try {
      val sqlContext = new SQLContext(sc)
      block(sqlContext)
    } finally {
      sc.stop()
    }

  }

  def prepareEnvironment(): MongoClient = {
    val mongoClient = MongoClient(MongoHost, MongoPort)
    populateTable(mongoClient)
    mongoClient
  }

  def cleanEnvironment(mongoClient: MongoClient) = {
    cleanData(mongoClient)
    mongoClient.close()
  }

  private def populateTable(client: MongoClient): Unit = {

    val collection = client(Database)(Collection)
    for (a <- 1 to 10) {
      collection.insert {
        MongoDBObject("id" -> a.toString,
          "age" -> (10 + a),
          "description" -> s"description $a",
          "enrolled" -> (a % 2 == 0),
          "name" -> s"Name $a"
        )
      }
    }

    collection.update(QueryBuilder.start("age").greaterThan(14).get, MongoDBObject(("$set", MongoDBObject(("optionalField", true)))), multi = true)
    collection.update(QueryBuilder.start("age").is(14).get, MongoDBObject(("$set", MongoDBObject(("fieldWithSubDoc", MongoDBObject(("subDoc", MongoDBList("foo", "bar"))))))))
  }

  private def cleanData(client: MongoClient): Unit = {
    val collection = client(Database)(Collection)
    collection.dropCollection()
  }
}
