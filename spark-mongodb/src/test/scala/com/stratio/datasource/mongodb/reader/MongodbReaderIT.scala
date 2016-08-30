/*
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
package com.stratio.datasource.mongodb.reader

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Locale

import com.mongodb.util.JSON
import com.mongodb.{BasicDBObject, DBObject}
import com.stratio.datasource.MongodbTestConstants
import com.stratio.datasource.mongodb._
import com.stratio.datasource.mongodb.client.MongodbClientFactory
import com.stratio.datasource.mongodb.config.{MongodbConfig, MongodbConfigBuilder}
import com.stratio.datasource.mongodb.partitioner.MongodbPartition
import com.stratio.datasource.mongodb.query.FilterSection
import com.stratio.datasource.partitioner.PartitionRange
import org.apache.spark.sql.Row
import org.apache.spark.sql.mongodb.{TemporaryTestSQLContext, TestSQLContext}
import org.apache.spark.sql.sources.{EqualTo, Filter}
import org.apache.spark.sql.types._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfter, FlatSpec, Matchers}

@RunWith(classOf[JUnitRunner])
class MongodbReaderIT extends FlatSpec
with Matchers
with MongoEmbedDatabase
with TestBsonData
with MongodbTestConstants
with BeforeAndAfterAll {

  private val host: String = "localhost"
  private val collection: String = "testCol"

  implicit val testConfig = MongodbConfigBuilder()
    .set(MongodbConfig.Host, List(host + ":" + mongoPort))
    .set(MongodbConfig.Database, db)
    .set(MongodbConfig.Collection, collection)
    .set(MongodbConfig.SamplingRatio, "1.0")
    .build()

  behavior of "A reader"

  it should "throw IllegalStateException if next() operation is invoked after closing the Reader" +
    scalaBinaryVersion in {
    val mongodbReader = new MongodbReader(testConfig,Array(), FilterSection(Array()))
    mongodbReader.init(
      MongodbPartition(0,
        testConfig[Seq[String]](MongodbConfig.Host),
        PartitionRange[DBObject](None, None)))

    mongodbReader.close()

    a[IllegalStateException] should be thrownBy {
      mongodbReader.next()
    }
  }

  it should "not advance the cursor position when calling hasNext() operation" + scalaBinaryVersion in {
    withEmbedMongoFixture(complexFieldAndType1) { mongodbProc =>

      val mongodbReader = new MongodbReader(testConfig,Array(),FilterSection(Array()))
      mongodbReader.init(
        MongodbPartition(0,
          testConfig[Seq[String]](MongodbConfig.Host),
          PartitionRange[DBObject](None, None)))

      (1 until 20).map(_ => mongodbReader.hasNext).distinct.toList==List(true)

    }
  }

  it should "advance the cursor position when calling next() operation" + scalaBinaryVersion in {
    withEmbedMongoFixture(complexFieldAndType1) { mongodbProc =>

      val mongodbReader = new MongodbReader(testConfig,Array(),FilterSection(Array()))
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

  it should "properly read java.util.Date (mongodb Date) type as Timestamp" + scalaBinaryVersion in {
    val dfunc = (s: String) => new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy", Locale.ENGLISH).parse(s)
    import com.mongodb.casbah.Imports.DBObject
    val stringAndDate = List(DBObject("string" -> "this is a simple string.", "date" -> dfunc("Mon Aug 10 07:52:49 EDT 2015")))

    withEmbedMongoFixture(stringAndDate) { mongodbProc =>
      val back = TemporaryTestSQLContext.fromMongoDB(testConfig)
      back.printSchema()
      assert(back.schema.fields.filter(_.name == "date").head.dataType == TimestampType)
      val timestamp = back.first().get(2).asInstanceOf[Timestamp]
      val origTimestamp = new Timestamp(stringAndDate.head.get("date").asInstanceOf[java.util.Date].getTime)
      timestamp should equal(origTimestamp)
    }
  }

  it should "retrieve the data properly filtering & selecting some fields " +
    "from a one row table" + scalaBinaryVersion in {
    withEmbedMongoFixture(primitiveFieldAndType) { mongodbProc =>
      //Test data preparation
      val requiredColumns = Array("_id","string", "integer")
      val filters = FilterSection(Array[Filter](EqualTo("boolean", true)))
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
    "from a five rows table" + scalaBinaryVersion in {
    withEmbedMongoFixture(primitiveFieldAndType5rows) { mongodbProc =>

      //Test data preparation
      val requiredColumns = Array("_id","string", "integer")
      val filters = FilterSection(Array[Filter](EqualTo("boolean", true)))
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

  it should "retrieve data correctly using a NOT filter" + scalaBinaryVersion in {
    withEmbedMongoFixture(primitiveFieldAndType5rows) { mongodbProc =>

      val mongoDF = TemporaryTestSQLContext.fromMongoDB(testConfig)
      mongoDF.registerTempTable("testTable")

      val resultNotBetween = TemporaryTestSQLContext.sql("SELECT integer FROM testTable WHERE integer NOT BETWEEN 11 AND 15").collect()
      resultNotBetween.head(0) should be (10)

      val resultEqualToAndNotBetween = TemporaryTestSQLContext.sql("SELECT integer FROM testTable WHERE integer = 11 AND integer NOT BETWEEN 12 AND 15").collect()
      resultEqualToAndNotBetween.head(0) should be (11)

      val resultNotLike = TemporaryTestSQLContext.sql("SELECT string FROM testTable WHERE string NOT LIKE '%third%'").collect()

      val notLike = Array(Row("this is a simple string."),
        Row("this is another simple string."),
        Row("this is the forth simple string."),
        Row("this is the fifth simple string."))

      resultNotLike should be (notLike)

    }
  }


  override def afterAll {
    MongodbClientFactory.closeAll(false)
  }

}
