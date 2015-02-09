package com.stratio.deep.mongodb.schema

import com.mongodb._
import com.mongodb.util.JSON
import com.stratio.deep.DeepConfig
import com.stratio.deep.mongodb.MongodbConfig
import com.stratio.deep.mongodb.rdd.MongodbRDD
import com.stratio.deep.mongodb.writer.{MongodbBatchWriter, MongodbSimpleWriter, MongodbWriter}
import org.apache.spark.sql.{SQLContext, SchemaRDD}
import org.apache.spark.sql.test.TestSQLContext
import org.scalatest.{fixture, Matchers, FlatSpec}

/**
 * Created by lfernandez on 9/02/15.
 */
class MongodbWriterSpec extends FlatSpec
with Matchers
with MongoEmbedDatabase
with TestBsonData {

  private val host: String = "localhost"
  private val port: Int = 12345
  private val database: String = "testDb"
  private val collection: String = "testCol"
  private val writeConcern : WriteConcern = WriteConcern.NORMAL

  val testConfig = DeepConfig()
    .set(MongodbConfig.Host, List(host + ":" + port))
    .set(MongodbConfig.Database, database)
    .set(MongodbConfig.Collection, collection)
    .set(MongodbConfig.SamplingRatio, 1.0)
    .set(MongodbConfig.WriteConcern, writeConcern)

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
