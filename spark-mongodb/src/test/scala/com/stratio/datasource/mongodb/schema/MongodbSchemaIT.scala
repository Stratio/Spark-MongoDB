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

import java.text.SimpleDateFormat
import java.util.Locale

import com.stratio.datasource.{MongodbTestConstants, TemporaryTestSQLContext}
import com.stratio.datasource.mongodb.config.{MongodbConfig, MongodbConfigBuilder}
import com.stratio.datasource.mongodb.partitioner.MongodbPartitioner
import com.stratio.datasource.mongodb.rdd.MongodbRDD
import com.stratio.datasource.mongodb._
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, TimestampType}
import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class MongodbSchemaIT extends FlatSpec
with Matchers
with MongoEmbedDatabase
with TestBsonData
with MongodbTestConstants {

  private val host: String = "localhost"
  private val collection: String = "testCol"
  private val readPreference = "secondaryPreferred"

  val testConfig = MongodbConfigBuilder()
    .set(MongodbConfig.Host,List(host + ":" + mongoPort))
    .set(MongodbConfig.Database,db)
    .set(MongodbConfig.Collection,collection)
    .set(MongodbConfig.SamplingRatio,1.0)
    .set(MongodbConfig.ReadPreference, readPreference)
    .build()

  val mongodbPartitioner = new MongodbPartitioner(testConfig)

  val mongodbRDD = new MongodbRDD(TemporaryTestSQLContext, testConfig, mongodbPartitioner)

  behavior of "A schema"

  it should "be inferred from rdd with primitives" + scalaBinaryVersion in {
    withEmbedMongoFixture(primitiveFieldAndType) { mongodProc =>
      val schema = MongodbSchema(mongodbRDD, 1.0).schema()

      schema.fields should have size 7
      schema.fieldNames should contain allOf("string", "integer", "long", "double", "boolean", "null")

      schema.printTreeString()
    }
  }

  it should "be inferred from rdd with complex fields" + scalaBinaryVersion in {
    withEmbedMongoFixture(complexFieldAndType1) { mongodProc =>
      val schema = MongodbSchema(mongodbRDD, 1.0).schema()

      schema.fields should have size 13

      schema.fields filter {
        case StructField(name, ArrayType(StringType, _), _, _) => Set("arrayOfNull", "arrayEmpty") contains name
        case _ => false
      } should have size 2

      schema.printTreeString()
    }
  }

  it should "resolve type conflicts between fields" + scalaBinaryVersion in {
    withEmbedMongoFixture(primitiveFieldValueTypeConflict) { mongodProc =>
      val schema = MongodbSchema(mongodbRDD, 1.0).schema()

      schema.fields should have size 7

      schema.printTreeString()
    }
  }

  it should "be inferred from rdd with more complex fields" + scalaBinaryVersion in {
    withEmbedMongoFixture(complexFieldAndType2) { mongodProc =>
      val schema = MongodbSchema(mongodbRDD, 1.0).schema()

      schema.fields should have size 5

      schema.printTreeString()
    }
  }

  it should "read java.util.Date fields as timestamptype" + scalaBinaryVersion in {
    val dfunc = (s: String) => new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy", Locale.ENGLISH).parse(s)
    import com.mongodb.casbah.Imports.DBObject
    val stringAndDate = List(DBObject("string" -> "this is a simple string.", "date" -> dfunc("Mon Aug 10 07:52:49 EDT 2015")))
    withEmbedMongoFixture(stringAndDate) { mongodProc =>
      val schema = MongodbSchema(mongodbRDD, 1.0).schema()

      schema.fields should have size 3
      schema.fields.filter(_.name == "date").head.dataType should equal(TimestampType)
      schema.printTreeString()
    }
  }
}
