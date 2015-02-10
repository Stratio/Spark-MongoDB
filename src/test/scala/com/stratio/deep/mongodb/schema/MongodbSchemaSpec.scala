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

package com.stratio.deep.mongodb.schema

import com.stratio.deep.DeepConfig
import com.stratio.deep.mongodb.{MongodbConfigBuilder, MongodbConfig}
import com.stratio.deep.mongodb.rdd.MongodbRDD
import org.apache.spark.sql.test.TestSQLContext
import org.scalatest._

/**
 * Created by rmorandeira on 4/02/15.
 */
class MongodbSchemaSpec extends FlatSpec
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

  behavior of "A schema"

  it should "be inferred from rdd with primitives" in {
    withEmbedMongoFixture(primitiveFieldAndType) { mongodProc =>
      val mongodbRDD = new MongodbRDD(TestSQLContext, testConfig)
      val schema = MongodbSchema(mongodbRDD, 1.0).schema()

      schema.fields should have size 7
      schema.fieldNames should contain allOf("string", "integer", "long", "double", "boolean", "null")

      schema.printTreeString()
    }
  }

  it should "be inferred from rdd with complex fields" in {
    withEmbedMongoFixture(complexFieldAndType1) { mongodProc =>
      val mongodbRDD = new MongodbRDD(TestSQLContext, testConfig)
      val schema = MongodbSchema(mongodbRDD, 1.0).schema()

      schema.fields should have size 12

      schema.printTreeString()
    }
  }

  it should "resolve type conflicts between fields" in {
    withEmbedMongoFixture(primitiveFieldValueTypeConflict) { mongodProc =>
      val mongodbRDD = new MongodbRDD(TestSQLContext, testConfig)
      val schema = MongodbSchema(mongodbRDD, 1.0).schema()

      schema.fields should have size 7

      schema.printTreeString()
    }
  }

  it should "be inferred from rdd with more complex fields" in {
    withEmbedMongoFixture(complexFieldAndType2) { mongodProc =>
      val mongodbRDD = new MongodbRDD(TestSQLContext, testConfig)
      val schema = MongodbSchema(mongodbRDD, 1.0).schema()

      schema.fields should have size 5

      schema.printTreeString()
    }
  }
}
