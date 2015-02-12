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

package com.stratio.deep.mongodb

import com.mongodb.{DBObject, WriteConcern}
import com.mongodb.util.JSON
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.test.TestSQLContext
import org.scalatest.{Matchers, FlatSpec}

import scala.collection.mutable.ArrayBuffer

/**
 * Created by jsantos on 12/02/15.
 */
class MongodbRelationSpec extends FlatSpec
with Matchers {

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

  val schema = new StructType(List(new StructField(
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
      "att6",new StructType(List(
        new StructField("att61",IntegerType ,false),
        new StructField("att62",IntegerType,true)
      )),false)))

  val relation = new MongodbRelation(testConfig,Some(schema))(TestSQLContext)

  behavior of "MongodbRelationSpec"

  it should "prune schema to adapt it to required columns" in {

    relation.pruneSchema(relation.schema,Array()) should equal(
      new StructType(List()))

    relation.pruneSchema(relation.schema,Array("fakeAtt")) should equal(
      new StructType(List()))

    relation.pruneSchema(relation.schema,Array("att1")) should equal(
      new StructType(List(
        new StructField(
          "att1",IntegerType,false))))

    relation.pruneSchema(relation.schema,Array("att3","att1")) should equal(
      new StructType(List(
        new StructField(
          "att3",StringType,false),
        new StructField(
          "att1",IntegerType,false))))

  }

}
