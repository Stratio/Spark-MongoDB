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
package com.stratio.datasource.mongodb

import com.mongodb.WriteConcern
import com.stratio.datasource.MongodbTestConstants
import com.stratio.datasource.mongodb.client.MongodbClientFactory
import com.stratio.datasource.mongodb.config.{MongodbCredentials, MongodbConfig, MongodbConfigBuilder}
import org.apache.spark.sql.mongodb.{TemporaryTestSQLContext, TestSQLContext}
import org.apache.spark.sql.types._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

@RunWith(classOf[JUnitRunner])
class MongodbRelationIT extends FlatSpec
with Matchers
with BeforeAndAfter
with MongodbTestConstants {

  private val host: String = "localhost"
  private val port: Int = 12345
  private val port2: Int = 67890
  private val database: String = "testDb"
  private val database2: String = "testDb2"
  private val collection: String = "testCol"
  private val collection2: String = "testCol2"
  private val writeConcern : WriteConcern = WriteConcern.NORMAL


  val testConfig = MongodbConfigBuilder()
    .set(MongodbConfig.Host, List(host + ":" + port))
    .set(MongodbConfig.Database, database)
    .set(MongodbConfig.Collection, collection)
    .build()

  val testConfig2 = MongodbConfigBuilder()
    .set(MongodbConfig.Host, List(host + ":" + port))
    .set(MongodbConfig.Collection, collection)
    .set(MongodbConfig.Database, database)
    .build()

  val testConfig3 = MongodbConfigBuilder()
    .set(MongodbConfig.Collection, collection2)
    .set(MongodbConfig.Database, database2)
    .set(MongodbConfig.SamplingRatio, 1.0)
    .set(MongodbConfig.WriteConcern, writeConcern)
    .set(MongodbConfig.Host, List(host + ":" + port2))
    .build()

  val testConfig4 = MongodbConfigBuilder()
    .set(MongodbConfig.Host, List(host + ":" + port))
    .set(MongodbConfig.Database, database)
    .set(MongodbConfig.Collection, collection)
    .set(MongodbConfig.Credentials, List(MongodbCredentials("user","database", "password".toCharArray)))
    .build()

  val schema = new StructType(Array(new StructField(
    "att1",IntegerType,false),
    new StructField(
      "att2",DoubleType,false),
    new StructField(
      "att3",StringType,false),
    new StructField(
      "att4",StringType,true),
    new StructField(
      "att5",new ArrayType(IntegerType,false),false),
    new StructField(
      "att6",new StructType(Array(
        new StructField("att61",IntegerType ,false),
        new StructField("att62",IntegerType,true)
      )),false)))

  behavior of "MongodbRelation"

  it should "prune schema to adapt it to required columns" + scalaBinaryVersion in {

    MongodbRelation.pruneSchema(schema,Array[String]()) should equal(
      new StructType(Array()))

    MongodbRelation.pruneSchema(schema,Array("fakeAtt")) should equal(
      new StructType(Array()))

    MongodbRelation.pruneSchema(schema,Array("att1")) should equal(
      new StructType(Array(
        new StructField(
          "att1",IntegerType,false))))

    MongodbRelation.pruneSchema(schema,Array("att3","att1")) should equal(
      new StructType(Array(
        new StructField(
          "att3",StringType,false),
        new StructField(
          "att1",IntegerType,false))))

  }

  val mongodbrelation = new MongodbRelation(testConfig, Some(schema))(TemporaryTestSQLContext)
  val mongodbrelation2 = new MongodbRelation(testConfig2, Some(schema))(TemporaryTestSQLContext)
  val mongodbrelation3 = new MongodbRelation(testConfig3, Some(schema))(TemporaryTestSQLContext)
  val mongodbrelation4 = new MongodbRelation(testConfig4, Some(schema))(TemporaryTestSQLContext)

  it should "provide info about equality in MongodbRelation" + scalaBinaryVersion in {
    mongodbrelation.equals(mongodbrelation) shouldEqual true
    mongodbrelation.equals(mongodbrelation2) shouldEqual true
    mongodbrelation.equals(mongodbrelation3) shouldEqual false
    mongodbrelation.equals(mongodbrelation4) shouldEqual false
  }

  after {
    MongodbClientFactory.closeAll(false)
  }

}
