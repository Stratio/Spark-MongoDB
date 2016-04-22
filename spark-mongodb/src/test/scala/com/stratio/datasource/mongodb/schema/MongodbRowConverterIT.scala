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
package com.stratio.datasource.mongodb.schema

import com.mongodb.DBObject
import com.mongodb.util.JSON
import com.stratio.datasource.{MongodbTestConstants, TemporaryTestSQLContext}
import com.stratio.datasource.mongodb.client.MongodbClientFactory
import com.stratio.datasource.mongodb.config.{MongodbConfig, MongodbConfigBuilder}
import com.stratio.datasource.mongodb.partitioner.MongodbPartitioner
import com.stratio.datasource.mongodb.rdd.MongodbRDD
import com.stratio.datasource.mongodb.schema.MongodbRowConverter._
import com.stratio.datasource.mongodb._

import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfter, FlatSpec, Matchers}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

@RunWith(classOf[JUnitRunner])
class MongodbRowConverterIT extends FlatSpec
with Matchers
with MongoEmbedDatabase
with TestBsonData
with MongodbTestConstants
with BeforeAndAfterAll {

  private val host: String = "localhost"
  private val collection: String = "testCol"

  val testConfig = MongodbConfigBuilder()
    .set(MongodbConfig.Host,List(host + ":" + mongoPort))
    .set(MongodbConfig.Database,db)
    .set(MongodbConfig.Collection,collection)
    .set(MongodbConfig.SamplingRatio,1.0)
    .build()

  //  Sample values
  val valueWithType: List[(Any, StructField)] = List(
    1 -> new StructField(
      "att1",IntegerType,false),
    2.0 -> new StructField(
      "att2",DoubleType,false),
    "hi" -> new StructField(
      "att3",StringType,false),
    null.asInstanceOf[Any] -> new StructField(
      "att4",StringType,true),
    new ArrayBuffer[Int]().+=(1).+=(2).+=(3) -> new StructField(
      "att5",new ArrayType(IntegerType,false),false),
    new GenericRow(List(1,null).toArray) -> new StructField(
      "att6",new StructType(Array(
        new StructField("att61",IntegerType ,false),
        new StructField("att62",IntegerType,true)
      )),false),
    // Subdocument
    new GenericRow(List(1, new GenericRow(List(1, "b").toArray)).toArray) ->
      new StructField(
        "att7", new StructType(Array(
          new StructField("att71",IntegerType,false),
          new StructField("att72",new StructType(Array(
            new StructField("att721", IntegerType, false),
            new StructField("att722", StringType, false)
          )),false))), false),
    //  Subdocument with List of a document
    new GenericRow(List(1, new ArrayBuffer[Any]().+=(new GenericRow(List(2,"b").toArray))).toArray) ->
      new StructField("att8", new StructType(
      Array(StructField("att81", IntegerType, false), StructField("att82",
        new ArrayType(StructType(Array(StructField("att821", IntegerType, false),StructField("att822", StringType, false))), false)
        ,false))), false),
    //  Subdocument with List of a document with wrapped array
    new GenericRow(List(1, new mutable.WrappedArray.ofRef[AnyRef](Array(
        new GenericRow(List(2,"b").toArray)
      ))).toArray) ->
      new StructField("att9", new StructType(
        Array(StructField("att91", IntegerType, false), StructField("att92",
          new ArrayType(StructType(Array(StructField("att921", IntegerType, false),StructField("att922", StringType, false))), false)
          ,false))), false)
  )

  val rowSchema = new StructType(valueWithType.map(_._2).toArray)

  val row = new GenericRow(valueWithType.map(_._1).toArray)

  val dbObject = JSON.parse(
    """{ "att5" : [ 1 , 2 , 3] ,
          "att4" :  null  ,
          "att3" : "hi" ,
          "att6" : { "att61" : 1 , "att62" :  null } ,
          "att2" : 2.0 ,
          "att1" : 1,
          "att7" : {"att71": 1, "att72":{"att721":1, "att722":"b"}},
          "att8" : {"att81": 1, "att82":[{"att821":2, "att822":"b"}]},
          "att9" : {"att91": 1, "att92":[{"att921":2, "att922":"b"}]}
          }
          """).asInstanceOf[DBObject]

  behavior of "The MongodbRowConverter"

  it should "be able to convert any value from a row into a dbobject field" + scalaBinaryVersion in{
    toDBObject(row, rowSchema) should equal(dbObject)
  }

  it should "be able to convert any value from a dbobject field  into a row field" + scalaBinaryVersion in{
    toSQL(dbObject,rowSchema) should equal(row)
  }

  it should "apply dbobject to row mapping in a RDD context" + scalaBinaryVersion in {
    withEmbedMongoFixture(complexFieldAndType2) { mongodProc =>
      val mongodbPartitioner = new MongodbPartitioner(testConfig)
      val mongodbRDD = new MongodbRDD(TemporaryTestSQLContext, testConfig, mongodbPartitioner)
      val schema = MongodbSchema(mongodbRDD, 1.0).schema()
      println("\n\nschema")
      schema.fieldNames.foreach(println)
      val collected = toSQL(complexFieldAndType2.head,schema)
      MongodbRowConverter
        .asRow(schema,mongodbRDD)
        .collect().toList should equal(List(collected))
    }
  }

  override def afterAll {
    MongodbClientFactory.closeAll(false)
  }

}
