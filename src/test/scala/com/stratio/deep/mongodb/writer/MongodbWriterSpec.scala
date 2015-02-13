/*
 *  Licensed to STRATIO (C) under one or more contributor license agreements.
 *  See the NOTICE file distributed with this work for additional information
 *  regarding copyright ownership. The STRATIO (C) licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied. See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package com.stratio.deep.mongodb.writer

import com.mongodb._
import com.mongodb.util.JSON
import com.stratio.deep.mongodb.{MongoEmbedDatabase, TestBsonData, MongodbConfig, MongodbConfigBuilder}
import org.scalatest.{FlatSpec, Matchers}

class MongodbWriterSpec extends FlatSpec
with Matchers
with MongoEmbedDatabase
with TestBsonData {

  private val host: String = "localhost"
  private val port: Int = 12345
  private val database: String = "testDb"
  private val collection: String = "testCol"
  private val writeConcern : WriteConcern = WriteConcern.NORMAL

  val testConfig = MongodbConfigBuilder()
    .set(MongodbConfig.Host, List(host + ":" + port))
    .set(MongodbConfig.Database, database)
    .set(MongodbConfig.Collection, collection)
    .set(MongodbConfig.SamplingRatio, 1.0)
    .set(MongodbConfig.WriteConcern, writeConcern)
    .build()

  val dbObject = JSON.parse(
    """{ "att5" : [ 1 , 2 , 3] ,
          "att4" :  null  ,
          "att3" : "hi" ,
          "att6" : { "att61" : 1 , "att62" :  null } ,
          "att2" : 2.0 ,
          "att1" : 1}""").asInstanceOf[DBObject]


  behavior of "A writer"

  it should "properly write in a Mongo collection using the Simple Writer" in {

    withEmbedMongoFixture(List()) { mongodbProc =>

      val mongodbSimpleWriter = new MongodbSimpleWriter(testConfig)

      val dbOIterator = List(dbObject).iterator

      mongodbSimpleWriter.save(dbOIterator)

      val mongodbClient = new MongoClient(host, port)

      val dbCollection = mongodbClient.getDB(database).getCollection(collection)

      val dbCursor = dbCollection.find()

      import scala.collection.JavaConversions._

      dbCursor.iterator().toList should equal(List(dbObject))


    }
  }

  it should "properly write in a Mongo collection using the Batch Writer" in {

    withEmbedMongoFixture(List()) { mongodbProc =>

      val mongodbBatchWriter = new MongodbBatchWriter(testConfig)

      val dbOIterator = List(dbObject).iterator

      mongodbBatchWriter.save(dbOIterator)

      val mongodbClient = new MongoClient(host, port)

      val dbCollection = mongodbClient.getDB(database).getCollection(collection)

      val dbCursor = dbCollection.find()

      import scala.collection.JavaConversions._

      dbCursor.iterator().toList should equal(List(dbObject))


    }
  }
}
