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
package com.stratio.datasource.mongodb.writer

import com.mongodb._
import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.util.JSON
import com.stratio.datasource.MongodbTestConstants
import com.stratio.datasource.mongodb.{MongoEmbedDatabase, TestBsonData, MongodbConfig, MongodbConfigBuilder}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, Matchers}

@RunWith(classOf[JUnitRunner])
class MongodbWriterIT extends FlatSpec
with Matchers
with MongoEmbedDatabase
with TestBsonData
with MongodbTestConstants{

  private val host: String = "localhost"
  private val collection: String = "testCol"
  private val writeConcern: WriteConcern = WriteConcern.NORMAL
  private val idField: String = "att2"
  private val updateField: String = "att3"
  private val wrongIdField: String = "non-existentColumn"
  private val language: String = "english"


  val testConfig = MongodbConfigBuilder()
    .set(MongodbConfig.Host, List(host + ":" + mongoPort))
    .set(MongodbConfig.Database, db)
    .set(MongodbConfig.Collection, collection)
    .set(MongodbConfig.SamplingRatio, 1.0)
    .set(MongodbConfig.WriteConcern, writeConcern)
    .build()

  val testConfigWithPk = MongodbConfigBuilder()
    .set(MongodbConfig.Host, List(host + ":" + mongoPort))
    .set(MongodbConfig.Database, db)
    .set(MongodbConfig.Collection, collection)
    .set(MongodbConfig.SamplingRatio, 1.0)
    .set(MongodbConfig.WriteConcern, writeConcern)
    .set(MongodbConfig.IdField, idField)
    .build()

  val testConfigWithLanguage = MongodbConfigBuilder()
    .set(MongodbConfig.Host, List(host + ":" + mongoPort))
    .set(MongodbConfig.Database, db)
    .set(MongodbConfig.Collection, collection)
    .set(MongodbConfig.SamplingRatio, 1.0)
    .set(MongodbConfig.WriteConcern, writeConcern)
    .set(MongodbConfig.Language, language)
    .build()

  val testConfigWithWrongPk = MongodbConfigBuilder()
    .set(MongodbConfig.Host, List(host + ":" + mongoPort))
    .set(MongodbConfig.Database, db)
    .set(MongodbConfig.Collection, collection)
    .set(MongodbConfig.SamplingRatio, 1.0)
    .set(MongodbConfig.WriteConcern, writeConcern)
    .set(MongodbConfig.IdField, wrongIdField)
    .build()

  val testConfigWithUpdateFields = MongodbConfigBuilder()
    .set(MongodbConfig.Host, List(host + ":" + mongoPort))
    .set(MongodbConfig.Database, db)
    .set(MongodbConfig.Collection, collection)
    .set(MongodbConfig.SamplingRatio, 1.0)
    .set(MongodbConfig.WriteConcern, writeConcern)
    .set(MongodbConfig.UpdateFields, updateField)
    .build()

  val dbObject = JSON.parse(
    """{ "att5" : [ 1 , 2 , 3] ,
          "att4" :  null  ,
          "att3" : "hi" ,
          "att6" : { "att61" : 1 , "att62" :  null } ,
          "att2" : 2.0 ,
          "att1" : 1}""").asInstanceOf[DBObject]

  val listDbObject = List(
    JSON.parse(
    """{ "att5" : [ 1 , 2 , 3] ,
          "att4" :  null  ,
          "att3" : "hi" ,
          "att6" : { "att61" : 1 , "att62" :  null } ,
          "att2" : 2.0 ,
          "att1" : 1}""").asInstanceOf[DBObject],
    JSON.parse(
      """{ "att5" : [ 1 , 2 , 3] ,
          "att4" :  null  ,
          "att3" : "holo" ,
          "att6" : { "att61" : 1 , "att62" :  null } ,
          "att2" : 2.0 ,
          "att1" : 1}""").asInstanceOf[DBObject])

  val updateDbObject = List(
    JSON.parse(
      """{ "att5" : [ 1 , 2 , 3] ,
          "att4" :  null  ,
          "att3" : "holo" ,
          "att6" : { "att61" : 1 , "att62" :  null } ,
          "att2" : 2.0 ,
          "att1" : 2}""").asInstanceOf[DBObject])

  behavior of "A writer"

  it should "properly write in a Mongo collection using the Simple Writer" + scalaBinaryVersion in {

    withEmbedMongoFixture(List()) { mongodbProc =>

      val mongodbSimpleWriter = new MongodbSimpleWriter(testConfig)

      val dbOIterator = List(dbObject).iterator

      mongodbSimpleWriter.saveWithPk(dbOIterator)

      val mongodbClient = new MongoClient(host, mongoPort)

      val dbCollection = mongodbClient.getDB(db).getCollection(collection)

      val dbCursor = dbCollection.find()

      import scala.collection.JavaConversions._

      dbCursor.iterator().toList should equal(List(dbObject))

    }
  }

  it should "properly write in a Mongo collection using the Batch Writer" + scalaBinaryVersion in {

    withEmbedMongoFixture(List()) { mongodbProc =>

      val mongodbBatchWriter = new MongodbBatchWriter(testConfig)

      val dbOIterator = List(dbObject).iterator

      mongodbBatchWriter.saveWithPk(dbOIterator)

      val mongodbClient = new MongoClient(host, mongoPort)

      val dbCollection = mongodbClient.getDB(db).getCollection(collection)

      val dbCursor = dbCollection.find()

      import scala.collection.JavaConversions._

      dbCursor.iterator().toList should equal(List(dbObject))

    }
  }

  it should "manage the primary key rightly, it has to read the same value " +
    "from the primary key as from the _id column" + scalaBinaryVersion in {
    withEmbedMongoFixture(List()) { mongodbProc =>

      val mongodbBatchWriter = new MongodbBatchWriter(testConfigWithPk)

      val dbOIterator = List(dbObject).iterator

      mongodbBatchWriter.saveWithPk(dbOIterator)

      val mongodbClient = new MongoClient(host, mongoPort)

      val dbCollection = mongodbClient.getDB(db).getCollection(collection)

      val dbCursor = dbCollection.find()

      import scala.collection.JavaConversions._

      dbCursor.iterator().toList.forall { case obj: BasicDBObject =>
        obj.get("_id") == obj.get("att2")
      } should be (true)
    }
  }

  it should "manage the incorrect primary key, created in a column that" +
    " doesn't exist, rightly" + scalaBinaryVersion in {
    withEmbedMongoFixture(List()) { mongodbProc =>

      val mongodbBatchWriter = new MongodbBatchWriter(testConfigWithWrongPk)

      val dbOIterator = List(dbObject).iterator

      mongodbBatchWriter.saveWithPk(dbOIterator)

      val mongodbClient = new MongoClient(host, mongoPort)

      val dbCollection = mongodbClient.getDB(db).getCollection(collection)

      val dbCursor = dbCollection.find()

      import scala.collection.JavaConversions._

      dbCursor.iterator().toList.forall { case obj: BasicDBObject =>
        obj.get("_id") != obj.get("non-existentColumn")

      } should be (true)
    }
  }

  it should "manage the language field for text index" + scalaBinaryVersion in {
    withEmbedMongoFixture(List()) { mongodbProc =>

      val mongodbBatchWriter = new MongodbBatchWriter(testConfigWithLanguage)

      val dbOIterator = List(dbObject).iterator

      mongodbBatchWriter.saveWithPk(dbOIterator)

      val mongodbClient = new MongoClient(host, mongoPort)

      val dbCollection = mongodbClient.getDB(db).getCollection(collection)

      val dbCursor = dbCollection.find()

      import scala.collection.JavaConversions._

      dbCursor.iterator().toList.forall { case obj: BasicDBObject =>
        obj.get("language") == language
      } should be (true)
    }
  }

  it should "manage the search fields and the update query, it has to read the same value from the search fields in " +
    "configuration" + scalaBinaryVersion in {
    withEmbedMongoFixture(List()) { mongodbProc =>

      val mongodbBatchWriter = new MongodbBatchWriter(testConfigWithPk)

      val dbOIterator = listDbObject.iterator

      val dbUpdateIterator = updateDbObject.iterator

      mongodbBatchWriter.saveWithPk(dbOIterator)

      val mongodbClient = new MongoClient(host, mongoPort)

      val dbCollection = mongodbClient.getDB(db).getCollection(collection)

      mongodbClient.getDB(db).getName should be (db)

      val dbCursor = dbCollection.find(MongoDBObject("att3" -> "holo"))

      import scala.collection.JavaConversions._

      dbCursor.iterator().foreach(println)

      dbCursor.iterator().toList.forall { case obj: BasicDBObject =>
        obj.getInt("att1") == 1
      } should be (true)

      mongodbBatchWriter.saveWithPk(dbUpdateIterator)

      mongodbClient.getDB(db).getName should be (db)

      val dbCursor2 = dbCollection.find(MongoDBObject("att3" -> "holo"))

      dbCursor2.iterator().foreach(println)

      dbCursor2.iterator().toList.forall { case obj: BasicDBObject =>
        obj.getInt("att1") == 2
      } should be (true)

    }
  }
}