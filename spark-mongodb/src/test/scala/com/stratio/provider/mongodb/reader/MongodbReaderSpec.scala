/*
 *
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
 * /
 */

package com.stratio.provider.mongodb.reader

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Locale

import com.mongodb.util.JSON
import com.mongodb.{BasicDBObject, DBObject}
import com.stratio.provider.mongodb._
import com.stratio.provider.mongodb.partitioner.MongodbPartition
import com.stratio.provider.partitioner.PartitionRange
import org.apache.spark.sql.sources.{EqualTo, Filter}
import org.apache.spark.sql.test.TestSQLContext
import org.apache.spark.sql.types._
import org.scalatest.{FlatSpec, Matchers}

class MongodbReaderSpec extends FlatSpec
with Matchers
with MongoEmbedDatabase
with TestBsonData {

  private val host: String = "localhost"
  private val port: Int = 12345
  private val database: String = "testDb"
  private val collection: String = "testCol"

  val testConfig = MongodbConfigBuilder()
    .set(MongodbConfig.Host, List(host + ":" + port))
    .set(MongodbConfig.Database, database)
    .set(MongodbConfig.Collection, collection)
    .set(MongodbConfig.SamplingRatio, 1.0)
    .build()

  behavior of "A reader"

  it should "throw IllegalStateException if next() operation is invoked after closing the Reader" in {
    val mongodbReader = new MongodbReader(testConfig,Array(),Array())
    mongodbReader.init(
      MongodbPartition(0,
        testConfig[Seq[String]](MongodbConfig.Host),
        PartitionRange[DBObject](None, None)))
    
    mongodbReader.close()

    a[IllegalStateException] should be thrownBy {
      mongodbReader.next()
    }
  }

  it should "not advance the cursor position when calling hasNext() operation" in {
    withEmbedMongoFixture(complexFieldAndType1) { mongodbProc =>

      val mongodbReader = new MongodbReader(testConfig,Array(),Array())
      mongodbReader.init(
        MongodbPartition(0,
          testConfig[Seq[String]](MongodbConfig.Host),
          PartitionRange[DBObject](None, None)))

      (1 until 20).map(_ => mongodbReader.hasNext).distinct.toList==List(true)
    }
  }

  it should "advance the cursor position when calling next() operation" in {
    withEmbedMongoFixture(complexFieldAndType1) { mongodbProc =>

      val mongodbReader = new MongodbReader(testConfig,Array(),Array())
      mongodbReader.init(
        MongodbPartition(0,
          testConfig[Seq[String]](MongodbConfig.Host),
          PartitionRange[DBObject](None, None)))
      val posBefore = mongodbReader.hasNext
      mongodbReader.next()
      val posAfter = mongodbReader.hasNext
      posBefore should equal(!posAfter)

    }
  }

  it should "properly read java.util.Date (mongodb Date) type as Timestamp" in {
    val dfunc = (s: String) => new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy", Locale.ENGLISH).parse(s)
    import com.mongodb.casbah.Imports.DBObject
    val stringAndDate = List(DBObject("string" -> "this is a simple string.", "date" -> dfunc("Mon Aug 10 07:52:49 EDT 2015")))

    withEmbedMongoFixture(stringAndDate) { mongodbProc =>
      val back = TestSQLContext.fromMongoDB(testConfig)
      back.printSchema()
      assert(back.schema.fields.filter(_.name == "date").head.dataType == TimestampType)
      val timestamp = back.first().get(2).asInstanceOf[Timestamp]
      val origTimestamp = new Timestamp(stringAndDate.head.get("date").asInstanceOf[java.util.Date].getTime)
      timestamp should equal(origTimestamp)
    }
  }

  it should "retrieve the data properly filtering & selecting some fields " +
    "from a one row table" in {
    withEmbedMongoFixture(primitiveFieldAndType) { mongodbProc =>
      //Test data preparation
      val requiredColumns = Array("_id","string", "integer")
      val filters = Array[Filter](EqualTo("boolean", true))
      val mongodbReader =
        new MongodbReader(testConfig, requiredColumns, filters)

      mongodbReader.init(
        MongodbPartition(0,
          testConfig[Seq[String]](MongodbConfig.Host),
          PartitionRange[DBObject](None, None)))

      //Data retrieving
      var l = List[DBObject]()
      while (mongodbReader.hasNext){
        l = l :+ mongodbReader.next()
      }

      //Data validation
      l.headOption.foreach{
        case obj: BasicDBObject =>
          obj.size() should equal(3)
          obj.get("string") should equal(
            primitiveFieldAndType.head.get("string"))
          obj.get("integer") should equal(
            primitiveFieldAndType.head.get("integer"))

      }
    }

  }


  it should "retrieve the data properly filtering & selecting some fields " +
    "from a five rows table" in {
    withEmbedMongoFixture(primitiveFieldAndType5rows) { mongodbProc =>

      //Test data preparation
      val requiredColumns = Array("_id","string", "integer")
      val filters = Array[Filter](EqualTo("boolean", true))
      val mongodbReader =
        new MongodbReader(testConfig, requiredColumns, filters)

      mongodbReader.init(
        MongodbPartition(0,
          testConfig[Seq[String]](MongodbConfig.Host),
          PartitionRange[DBObject](None, None)))

      val desiredData =
        JSON.parse(
          """{"string":"this is a simple string.",
          "integer":10
          }""").asInstanceOf[DBObject] ::
          JSON.parse(
            """{"string":"this is the third simple string.",
          "integer":12
          }""").asInstanceOf[DBObject] ::
          JSON.parse(
            """{"string":"this is the forth simple string.",
          "integer":13
          }""").asInstanceOf[DBObject] :: Nil

      //Data retrieving
      var l = List[BasicDBObject]()
      while (mongodbReader.hasNext){
        l = l :+ mongodbReader.next().asInstanceOf[BasicDBObject]
      }

      //Data validation

      def pruneId(dbObject: BasicDBObject):BasicDBObject ={
        import scala.collection.JavaConversions._
        import scala.collection.JavaConverters._
        new BasicDBObject(dbObject.toMap.asScala.filter{case (k,v) => k!="_id"})
      }
      val desiredL = l.map(pruneId)

      l.size should equal(3)
      desiredData.diff(desiredL) should equal (List())
      l.headOption.foreach{
        case obj: BasicDBObject =>
          obj.size() should equal(3)
          obj.get("string") should equal(
            primitiveFieldAndType5rows.head.get("string"))
          obj.get("integer") should equal(
            primitiveFieldAndType5rows.head.get("integer"))

      }
    }

  }
}
