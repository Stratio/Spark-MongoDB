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

package com.stratio.deep.mongodb.partitioner

import com.mongodb.CommandResult
import com.mongodb.casbah.Imports._
import com.stratio.deep.DeepConfig
import com.stratio.deep.mongodb.MongodbConfig
import com.stratio.deep.partitioner.{DeepPartitionRange, DeepPartitioner}
import org.apache.spark.Partition

/**
 * Created by rmorandeira on 6/02/15.
 */
class MongodbPartitioner(config: DeepConfig) extends DeepPartitioner {

  private val hosts: List[ServerAddress] =
    config[List[String]](MongodbConfig.Host)
      .map(add => new ServerAddress(add)).toList

  private val mongoClient: MongoClient = MongoClient(hosts)
  mongoClient.readPreference = ReadPreference.Nearest 

  private val collection: MongoCollection =
    mongoClient(config(MongodbConfig.Database))(config(MongodbConfig.Collection))


  override def computePartitions(): Array[Partition] = {
    if (isShardedCollection())
      computeShardedChunkPartitions().asInstanceOf[Array[Partition]]
    else
      computeNotShardedPartitions().asInstanceOf[Array[Partition]]
  }

  protected def isShardedCollection(): Boolean =
    collection.stats.ok && collection.stats.getBoolean("sharded", false)

  protected def computeNotShardedPartitions(): Array[MongodbPartition] = {

    val ranges = splitRanges

    val serverAddressList: Seq[String] = mongoClient.allAddress.map {
      server => server.getHost + ":" + server.getPort
    }.toSeq

    val partitions: Array[MongodbPartition] = ranges.zipWithIndex.map {
      case ((previous: Option[DBObject], current: Option[DBObject]), i) =>
        MongodbPartition(i,
          serverAddressList,
          DeepPartitionRange(previous, current))
    }.toArray

    partitions
  }

  protected def splitRanges(): Seq[(Option[DBObject], Option[DBObject])] = {

    val cmd: MongoDBObject = MongoDBObject(
      "splitVector" -> collection.fullName,
      "keyPattern" -> MongoDBObject(MongodbPartitioner.Id -> 1),
      "force" -> false,
      "maxChunkSize" -> MongodbPartitioner.SplitSize
    )

    val data: CommandResult = collection.db.getSisterDB("admin").command(cmd)

    val splitKeys = data.as[List[DBObject]]("splitKeys").map(Option(_))

    val ranges = (None +: splitKeys) zip (splitKeys :+ None)
    ranges.toSeq
  }

  protected def computeShardedChunkPartitions(): Array[MongodbPartition] = {

    val chunksCollection = mongoClient(MongodbPartitioner.ConfigDatabase)("chunks")
    val dbCursor = chunksCollection.find(MongoDBObject("ns" -> collection.fullName))

    val shards = describeShardsMap

    val partitions = dbCursor.zipWithIndex.map { case (chunk: DBObject, i: Int) =>
      val lowerBound = chunk.getAs[DBObject]("min")
      val upperBound = chunk.getAs[DBObject]("max")

      val hosts: Seq[String] = (for {
        shard <- chunk.getAs[String]("shard")
        hosts <- shards.get(shard)
      } yield hosts).getOrElse(Seq[String]())

      MongodbPartition(i,
        hosts,
        DeepPartitionRange(lowerBound, upperBound))
    }.toArray

    partitions
  }

  protected def describeShardsMap(): Map[String, Seq[String]] = {

    val shardsCollection = mongoClient(MongodbPartitioner.ConfigDatabase)("shards")

    val shards = shardsCollection.find.map { shard =>
      val hosts: Seq[String] = shard.getAs[String]("host")
        .fold(ifEmpty = Seq[String]())(_.split(",").map(_.split("/").reverse.head).toSeq)
      (shard.as[String](MongodbPartitioner.Id), hosts)
    }.toMap

    shards
  }
}

object MongodbPartitioner {
  private val Id: String = "_id"
  private val ConfigDatabase: String = "config"
  private val SplitSize: Int = 10

  def apply(config: DeepConfig): MongodbPartitioner =
    new MongodbPartitioner(config)

}
