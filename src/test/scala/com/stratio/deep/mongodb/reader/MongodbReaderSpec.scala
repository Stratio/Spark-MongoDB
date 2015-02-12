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

package com.stratio.deep.mongodb.reader

import com.mongodb.DBObject
import com.stratio.deep.mongodb.partitioner.MongodbPartition
import com.stratio.deep.mongodb.{MongoEmbedDatabase, TestBsonData, MongodbConfig, MongodbConfigBuilder}
import com.stratio.deep.partitioner.DeepPartitionRange
import org.scalatest.{FlatSpec, Matchers}

/**
 * Created by lfernandez on 9/02/15.
 */
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
        DeepPartitionRange[DBObject](None, None)))
    
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
          DeepPartitionRange[DBObject](None, None)))

      (1 until 20).map(_ => mongodbReader.hasNext).distinct.toList==List(true)
    }
  }

  it should "advance the cursor position when calling next() operation" in {
    withEmbedMongoFixture(complexFieldAndType1) { mongodbProc =>

      val mongodbReader = new MongodbReader(testConfig,Array(),Array())
      mongodbReader.init(
        MongodbPartition(0,
          testConfig[Seq[String]](MongodbConfig.Host),
          DeepPartitionRange[DBObject](None, None)))
      val posBefore = mongodbReader.hasNext
      val dbObject2 = mongodbReader.next()
      val posAfter = mongodbReader.hasNext
      posBefore should equal(!posAfter)

    }
  }
}
