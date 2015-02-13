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

package com.stratio.deep.mongodb.reader

import com.mongodb.QueryBuilder
import com.mongodb.casbah.Imports._
import com.stratio.deep.DeepConfig
import com.stratio.deep.mongodb.MongodbConfig
import com.stratio.deep.mongodb.partitioner.MongodbPartition
import org.apache.spark.Partition
import org.apache.spark.sql.sources._
import scala.util.Try

/**
 *
 * @param config Configuration object.
 * @param requiredColumns Pruning fields
 * @param filters Added query filters
 */
class MongodbReader(
  config: DeepConfig,
  requiredColumns: Array[String],
  filters: Array[Filter]) {

  private var mongoClient: Option[MongoClient] = None

  private var dbCursor: Option[MongoCursor] = None

  def close(): Unit = {
    dbCursor.fold(ifEmpty = ()) { cursor =>
      cursor.close()
      dbCursor = None
    }

    mongoClient.fold(ifEmpty = ()) { client =>
      client.close()
      mongoClient = None
    }
  }

  def hasNext: Boolean = {
    dbCursor.fold(ifEmpty = false)(_.hasNext)
  }

  def next(): DBObject = {
    dbCursor.fold(ifEmpty =
      throw new IllegalStateException("DbCursor is not initialized"))(_.next())
  }


  /**
   * Initialize MongoDB reader
   * @param partition Where to read from
   */
  def init(partition: Partition): Unit =
    Try {
      val mongoPartition = partition.asInstanceOf[MongodbPartition]

      mongoClient = Option(MongoClient(
        mongoPartition.hosts.map(add => new ServerAddress(add)).toList,
        List.empty[MongoCredential]))

      dbCursor = (for {
        client <- mongoClient
        collection <- Option(client(config(MongodbConfig.Database))(config(MongodbConfig.Collection)))
        dbCursor <- Option(collection.find(queryPartition(partition, filters), selectFields(requiredColumns)))
      } yield {
        mongoPartition.partitionRange.minKey.foreach(min => dbCursor.addSpecial("$min", min))
        mongoPartition.partitionRange.maxKey.foreach(min => dbCursor.addSpecial("$max", min))
        dbCursor
      }).headOption

    }.recover {
      case throwable =>
        throw MongodbReadException(throwable.getMessage, throwable)
    }

  /**
   * Create query partition using given filters.
   *
   * @param partition the partition
   * @return the dB object
   */
  private def queryPartition(
    partition: Partition,
    filters: Array[Filter]): DBObject = {

    val queryBuilder: QueryBuilder = QueryBuilder.start

    filters.foreach {
      case EqualTo(attribute, value) =>
        queryBuilder.put(attribute).is(value)
      case GreaterThan(attribute, value) =>
        queryBuilder.put(attribute).greaterThan(value)
      case GreaterThanOrEqual(attribute, value) =>
        queryBuilder.put(attribute).greaterThanEquals(value)
      case In(attribute, values) =>
        queryBuilder.put(attribute).in(values)
      case LessThan(attribute, value) =>
        queryBuilder.put(attribute).lessThan(value)
      case LessThanOrEqual(attribute, value) =>
        queryBuilder.put(attribute).lessThanEquals(value)
    }
    queryBuilder.get

  }

  /**
   *
   * Prepared DBObject used to specify required fields in mongodb 'find'
   * @param fields Required fields
   * @return A mongodb object that represents required fields.
   */
  private def selectFields(fields: Array[String]): DBObject =
    MongoDBObject(fields.toList.map(_ -> 1))

}

case class MongodbReadException(
  msg: String,
  causedBy: Throwable) extends RuntimeException(msg, causedBy)
