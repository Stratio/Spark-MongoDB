package com.stratio.deep.mongodb.partitioner

import com.stratio.deep.mongodb.MongoClusterEmbedDatabase
import org.scalatest.{BeforeAndAfter, Matchers, FlatSpec}

class MongodbPartitionerSpec extends FlatSpec
with BeforeAndAfter
with Matchers
with MongoClusterEmbedDatabase {

  val configServerPorts = List(12341)
  val database = "database-1"
  val mongoPort = 12344
  val currentHost = "localhost"
  val replicaSets = Map(
    "replicaSet1" -> List(12345, 12346, 12347),
    "replicaSet2" -> List(12348, 12349, 12350))

  system.start()

  behavior of "MongodbPartitioner"

  it should "get proper partition ranges when connecting" +
    " to a sharded cluster" in {

    Thread.sleep( 30 * 1000)

  }

  system.stop()

}

