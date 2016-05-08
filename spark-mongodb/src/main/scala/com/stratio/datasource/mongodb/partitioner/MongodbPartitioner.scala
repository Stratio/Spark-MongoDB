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

import java.text.SimpleDateFormat
import com.mongodb.{MongoCredential, ServerAddress}
import com.mongodb.casbah.Imports._
import com.stratio.datasource.Config
import com.stratio.datasource.mongodb.{MongodbSSLOptions, MongodbClientFactory, MongodbCredentials, MongodbConfig}

import com.stratio.datasource.mongodb.partitioner.MongodbPartitioner._
import com.stratio.datasource.partitioner.{PartitionRange, Partitioner}
import com.stratio.datasource.util.using
import scala.util.Try

/**
 * @param config Partition configuration
 */
class MongodbPartitioner(
  config: Config) extends Partitioner[MongodbPartition] {


  @transient private val hosts: List[ServerAddress] =
    config[List[String]](MongodbConfig.Host)
      .map(add => new ServerAddress(add))

  @transient private val credentials: List[MongoCredential] =
    config.getOrElse[List[MongodbCredentials]](MongodbConfig.Credentials, MongodbConfig.DefaultCredentials).map{
      case MongodbCredentials(user,database,password) =>
        MongoCredential.createCredential(user,database,password)
    }

  @transient private val ssloptions: Option[MongodbSSLOptions] =
    config.get[MongodbSSLOptions](MongodbConfig.SSLOptions)

  private val clientOptions = {
    val lowerCaseOptions = MongodbConfig.ListMongoClientOptions.map(_.toLowerCase).toSet
    config.properties.filter { case (k, _) => lowerCaseOptions contains k }
  }

  private val databaseName: String = config(MongodbConfig.Database)

  private val collectionName: String = config(MongodbConfig.Collection)

  private val collectionFullName: String = s"$databaseName.$collectionName"

  override def computePartitions(): Array[MongodbPartition] =
    if (isShardedCollection)  computeShardedChunkPartitions()
    else computeNotShardedPartitions()

  /**
   * @return Whether this is a sharded collection or not
   */
  protected def isShardedCollection: Boolean =
     using(MongodbClientFactory.createClient(hosts,credentials, ssloptions, clientOptions)) { mongoClient =>
      val collection = mongoClient(databaseName)(collectionName)
      collection.stats.ok && collection.stats.getBoolean("sharded", false)
  }

  /**
   * @return MongoDB partitions as sharded chunks.
   */
  protected def computeShardedChunkPartitions(): Array[MongodbPartition] =
    using(MongodbClientFactory.createClient(hosts,credentials, ssloptions, clientOptions)) { mongoClient =>

      Try {
        val chunksCollection = mongoClient(ConfigDatabase)(ChunksCollection)
        val dbCursor = chunksCollection.find(MongoDBObject("ns" -> collectionFullName))

        val shards = describeShardsMap()

        val partitions = dbCursor.zipWithIndex.map {
          case (chunk: DBObject, i: Int) =>
            val lowerBound = chunk.getAs[DBObject]("min")
            val upperBound = chunk.getAs[DBObject]("max")

            val hosts: Seq[String] = (for {
              shard <- chunk.getAs[String]("shard")
              hosts <- shards.get(shard)
            } yield hosts).getOrElse(Seq[String]())

            MongodbPartition(i,
              hosts,
              PartitionRange(lowerBound, upperBound))
        }.toArray

        partitions

      }.recover {
        case _: Exception =>
          val serverAddressList: Seq[String] = mongoClient.allAddress.map {
            server => server.getHost + ":" + server.getPort
          }.toSeq
          Array(MongodbPartition(0, serverAddressList, PartitionRange(None, None)))
      }.get
    }

  /**
   * @return Array of not-sharded MongoDB partitions.
   */
  protected def computeNotShardedPartitions(): Array[MongodbPartition] =
    using(MongodbClientFactory.createClient(hosts,credentials, ssloptions, clientOptions)) { mongoClient =>

      val ranges = splitRanges()

      val serverAddressList: Seq[String] = mongoClient.allAddress.map {
        server => server.getHost + ":" + server.getPort
      }.toSeq

      val partitions: Array[MongodbPartition] = ranges.zipWithIndex.map {
        case ((previous: Option[DBObject], current: Option[DBObject]), i) =>
          MongodbPartition(i,
            serverAddressList,
            PartitionRange(previous, current))
      }.toArray

      partitions
    }

  /**
   * @return A sequence of minimum and maximum DBObject in range.
   */
  protected def splitRanges(): Seq[(Option[DBObject], Option[DBObject])] = {

    def BoundWithCorrectType(value: String, dataType: String) : Any = dataType match {
      case "isoDate"  => convertToISODate(value)
      case "int"      => value.toInt
      case "long"     => value.toLong
      case "double"   => value.toDouble
      case "string"   => value
      case _          => throw new IllegalArgumentException(s"Illegal type $dataType for ${MongodbConfig.SplitKeyType} parameter.")
    }

    def convertToISODate(value: String) : java.util.Date = {
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
      dateFormat.parse(value)
    }

    val splitKey = config.getOrElse(MongodbConfig.SplitKey, MongodbConfig.DefaultSplitKey)

    val requiredCustomSplitParams = Seq(MongodbConfig.SplitKeyMin, MongodbConfig.SplitKeyMax, MongodbConfig.SplitKeyType)

    val customSplitIsDefined = requiredCustomSplitParams.forall(key => config.get(key).isDefined)

    val (splitBounds , splitKeyMin , splitKeyMax) = if(customSplitIsDefined){

      val keyType = config[String](MongodbConfig.SplitKeyType)
      val splitKeyMinValue = BoundWithCorrectType(config[String](MongodbConfig.SplitKeyMin), keyType)
      val splitKeyMaxValue = BoundWithCorrectType(config[String](MongodbConfig.SplitKeyMax), keyType)

      val splitKeyMin = MongoDBObject(splitKey -> splitKeyMinValue)
      val splitKeyMax = MongoDBObject(splitKey -> splitKeyMaxValue)

      val bounds = MongoDBObject(
        "min" -> splitKeyMin,
        "max" -> splitKeyMax
      )

      (bounds, Some(splitKeyMin), Some(splitKeyMax))
    }
    else (MongoDBObject.empty, None, None)

    val maxChunkSize = config.get[String](MongodbConfig.SplitSize).map(_.toInt)
      .getOrElse(MongodbConfig.DefaultSplitSize)

    val cmd: MongoDBObject = MongoDBObject(
      "splitVector" -> collectionFullName,
      "keyPattern" -> MongoDBObject(splitKey -> 1),
      "force" -> false,
      "maxChunkSize" -> maxChunkSize
    ) ++ splitBounds

    using(MongodbClientFactory.createClient(hosts,credentials, ssloptions, clientOptions)) { mongoClient =>

      Try {
        val data = mongoClient("admin").command(cmd)
        val splitKeys = data.as[List[DBObject]]("splitKeys").map(Option(_))
        val ranges = (splitKeyMin +: splitKeys) zip (splitKeys :+ splitKeyMax)
        ranges.toSeq
      }.recover {
        case _: Exception =>
          val stats = mongoClient(databaseName)(collectionName).stats
          val shards = mongoClient(ConfigDatabase)(ShardsCollection)
            .find(MongoDBObject("_id" -> stats.getString("primary")))

          val shard = shards.next()
          val shardHost: String = shard.as[String]("host")
            .replace(shard.get("_id") + "/", "")

          using(MongodbClientFactory.createClient(shardHost)) { shardClient =>
            val data = shardClient.getDB("admin").command(cmd)
            val splitKeys = data.as[List[DBObject]]("splitKeys").map(Option(_))
            val ranges = (splitKeyMin +: splitKeys) zip (splitKeys :+ splitKeyMax )

            ranges.toSeq
          }
      }.getOrElse(Seq((None, None)))
    }

  }

  /**
   * @return Map of shards.
   */
  protected def describeShardsMap(): Map[String, Seq[String]] =
    using(MongodbClientFactory.createClient(hosts,credentials, ssloptions, clientOptions)) { mongoClient =>

      val shardsCollection = mongoClient(ConfigDatabase)(ShardsCollection)

      val shards = shardsCollection.find().map { shard =>
        val hosts: Seq[String] = shard.getAs[String]("host")
          .fold(ifEmpty = Seq[String]())(_.split(",").map(_.split("/").reverse.head).toSeq)
        (shard.as[String]("_id"), hosts)
      }.toMap

      shards
    }

}

object MongodbPartitioner {

  val ConfigDatabase = "config"
  val ChunksCollection = "chunks"
  val ShardsCollection = "shards"

}
