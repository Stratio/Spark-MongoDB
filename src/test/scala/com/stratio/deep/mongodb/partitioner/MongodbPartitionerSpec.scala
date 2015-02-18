package com.stratio.deep.mongodb.partitioner

import com.mongodb.DBObject
import com.mongodb.util.JSON
import com.stratio.deep.mongodb._
import org.scalatest.{BeforeAndAfter, Matchers, FlatSpec}

class MongodbPartitionerSpec extends FlatSpec
with BeforeAndAfter
with Matchers
with MongoClusterEmbedDatabase
with TestBsonData {

  val configServerPorts = List(12341)
  val database = "database-1"
  val collection = "collection-1"
  val shardKey = "_id"
  val shardMaxSize = 1
  val chunkSize = 1
  val mongoPort = 12344
  val currentHost = "localhost"
  val replicaSets = Map(
    "replicaSet1" -> List(12345, 12346, 12347),
    "replicaSet2" -> List(12348, 12349, 12350))

  behavior of "MongodbPartitioner"

  it should "get proper partition ranges when connecting" +
    " to a sharded cluster" in {

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


}

