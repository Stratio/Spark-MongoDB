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
package com.stratio.datasource.mongodb.partitioner

import com.mongodb.DBObject
import com.mongodb.util.JSON
import com.stratio.datasource.MongodbTestConstants
import com.stratio.datasource.mongodb._
import com.stratio.datasource.mongodb.MongodbConfig._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, Matchers, FlatSpec}

@RunWith(classOf[JUnitRunner])
class MongodbPartitionerIT extends FlatSpec
with BeforeAndAfter
with Matchers
with MongoClusterEmbedDatabase
with MongoEmbedDatabase
with TestBsonData
with MongodbTestConstants {

  val configServerPorts = List(mongoPort+10)
  val database = "database-1"
  val collection = "collection-1"
  val shardKey = "_id"
  val shardMaxSize = 1
  val chunkSize = 1
  val currentHost = "localhost"
  val replicaSets = Map(
    "replicaSet1" -> List(mongoPort+1, mongoPort+2, mongoPort+3),
    "replicaSet2" -> List(mongoPort+4, mongoPort+5, mongoPort+6))

  behavior of "MongodbPartitioner"
  it should "get proper partition ranges when connecting" + " to a sharded cluster" + scalaBinaryVersion in {

    val testConfig = MongodbConfigBuilder()
      .set(MongodbConfig.Host, replicaSets.values.flatMap(
      ports => ports.take(1).map(
        p => s"$currentHost:$p")))
      .set(MongodbConfig.Database, database)
      .set(MongodbConfig.Collection, collection)
      .set(MongodbConfig.SamplingRatio, 1.0)
      .build()

    withCluster { system =>
      val partitioner1 = new MongodbPartitioner(testConfig)
      val (partition :: Nil) = partitioner1.computePartitions().toList
      partition.index should equal(0)
      partition.partitionRange.minKey should equal(None)
      partition.partitionRange.maxKey should equal(None)
      //TODO: Check what happens when shard is enable due to get over max chunk size
    }

  }

  def objectSample(amount: Long): Stream[DBObject] = {
    def longs: Stream[Long] = 0 #:: longs.map(_ + 1)
    longs.map { n =>
      n -> JSON.parse(
        s"""{"string":"this is a simple string.",
          "integer":10,
          "long":$n,
          "double":1.7976931348623157E308,
          "boolean":true,
          "null":null
      }""").asInstanceOf[DBObject]
    }.takeWhile {
      case (idx, _) => idx <= amount
    }.map(_._2)
  }


  it should "get proper partition ranges using splitVector with bounds" + scalaBinaryVersion in {

    import com.mongodb.casbah.Imports.MongoDBObject
    val dataSet = (1 to 15000).map(n=> MongoDBObject("name" -> s"name$n" , "id" -> n)).toList

    withEmbedMongoFixture(dataSet) { mongoProc =>
      val mongoClient = com.mongodb.casbah.MongoClient("localhost", mongoPort)

      val coll = mongoClient(db)("testCol"
      )
      // to run splitVector index by the splitKey field is needed
      coll.createIndex(MongoDBObject("id" ->1))

      val testConfig = MongodbConfigBuilder(Map(
        Host -> List(s"localhost:$mongoPort"),
        Database -> db,
        Collection -> "testCol",
        SamplingRatio -> 1.0,
        SplitSize -> 1,
        SplitKey -> "id",
        SplitSize -> "1",
        SplitKeyType -> "int",
        SplitKeyMin -> "500",
        SplitKeyMax -> "14000")
      ).build()

      val partitioner = new MongodbPartitioner(testConfig)
      val partitions = partitioner.computePartitions().toList.size

      //With the dataSet for this test and this splitVector config, 3 partitions would be created
      partitions should equal(3)
    }
  }


}