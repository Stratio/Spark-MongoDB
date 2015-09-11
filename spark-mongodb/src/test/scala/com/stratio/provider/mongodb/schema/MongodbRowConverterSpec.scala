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

package com.stratio.provider.mongodb.schema

import com.mongodb.DBObject
import com.mongodb.util.JSON
import com.stratio.provider.mongodb.partitioner.MongodbPartitioner
import com.stratio.provider.mongodb.rdd.MongodbRDD
import com.stratio.provider.mongodb.schema.MongodbRowConverter._
import com.stratio.provider.mongodb.{MongoEmbedDatabase, MongodbConfig, MongodbConfigBuilder, TestBsonData, TestSQLContext}
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.types._
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.mutable.ArrayBuffer

class MongodbRowConverterSpec extends FlatSpec
with Matchers
with MongoEmbedDatabase
with TestBsonData {

  private val host: String = "localhost"
  private val port: Int = 12345
  private val database: String = "testDb"
  private val collection: String = "testCol"

  val testConfig = MongodbConfigBuilder()
    .set(MongodbConfig.Host,List(host + ":" + port))
    .set(MongodbConfig.Database,database)
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
      )),false))

  val rowSchema = new StructType(valueWithType.map(_._2).toArray)

  val row = new GenericRow(valueWithType.map(_._1).toArray)

  val dbObject = JSON.parse(
    """{ "att5" : [ 1 , 2 , 3] ,
          "att4" :  null  ,
          "att3" : "hi" ,
          "att6" : { "att61" : 1 , "att62" :  null } ,
          "att2" : 2.0 ,
          "att1" : 1}""").asInstanceOf[DBObject]

  behavior of "The MongodbRowConverter"

  it should "be able to convert any value from a row into a dbobject field" in{
    toDBObject(row, rowSchema) should equal(dbObject)
  }

  it should "be able to convert any value from a dbobject field  into a row field" in{
    toSQL(dbObject,rowSchema) should equal(row)
  }

  it should "apply dbobject to row mapping in a RDD context" in {
    withEmbedMongoFixture(complexFieldAndType2) { mongodProc =>
      val mongodbPartitioner = new MongodbPartitioner(testConfig)
      val mongodbRDD = new MongodbRDD(TestSQLContext, testConfig, mongodbPartitioner)
      val schema = MongodbSchema(mongodbRDD, 1.0).schema()
      val collected = toSQL(complexFieldAndType2.head,schema)
      MongodbRowConverter
        .asRow(schema,mongodbRDD)
        .collect().toList should equal(List(collected))
    }
  }

}
