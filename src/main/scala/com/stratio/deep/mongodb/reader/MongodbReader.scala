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

import com.mongodb.{DBCursor,MongoCredential,QueryBuilder}
import com.mongodb.casbah.Imports._
import com.stratio.deep.DeepConfig
import com.stratio.deep.mongodb.MongodbConfig
import com.stratio.deep.mongodb.partitioner.MongodbPartition
import org.apache.spark.Partition
import org.apache.spark.sql.sources._

import scala.util.Try

/**
 * Created by rmorandeira on 29/01/15.
 */
class MongodbReader(
  config: DeepConfig,
  requiredColumns: Array[String],
  filters: Array[Filter]) {

  private val mongoClient: MongoClient =
    MongoClient(config[List[String]](MongodbConfig.Host)
      .map(add => new ServerAddress(add)).toList,
      List.empty[MongoCredential])

  private val db = mongoClient.getDB(config(MongodbConfig.Database))

  private val collection = db.getCollection(config(MongodbConfig.Collection))

  private var dbCursor: Option[DBCursor] = None

  /**
   * Close void.
   */
  def close(): Unit = {
    dbCursor.fold(ifEmpty = ()){cursor =>
      cursor.close()
      dbCursor = None
    }
    mongoClient.close()
  }

  /**
   * Has next.
   *
   * @return the boolean
   */
  def hasNext: Boolean = {
    dbCursor.fold(ifEmpty = false)(_.hasNext)
  }

  /**
   * Next row.
   *
   * @return the cells
   */
  def next(): DBObject = {
    dbCursor.fold(ifEmpty =
      throw new IllegalStateException("DbCursor is not initialized"))(_.next())
  }

  /**
   * Init void.
   *
   * @param partition the partition
   */
  def init(partition: Partition): Unit =
    Try {
      val mongoPartition = partition.asInstanceOf[MongodbPartition]
      dbCursor = Option(collection.find(
        queryPartition(partition,filters),
        selectFields(requiredColumns)))
      dbCursor.foreach { cursor =>
        mongoPartition.partitionRange.minKey.foreach(min => cursor.addSpecial("$min", min))
        mongoPartition.partitionRange.maxKey.foreach(min => cursor.addSpecial("$max", min))
      }
    }.recover{
      case throwable =>
        throw MongodbReadException(throwable.getMessage,throwable)
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

    filters.map{
      case equalsTo: EqualTo =>
        queryBuilder.put(equalsTo.attribute).is(equalsTo.value)
      case greaterThan: GreaterThan =>
        queryBuilder.put(greaterThan.attribute).greaterThan(greaterThan.value)
      case greaterThanOrEqual: GreaterThanOrEqual =>
        queryBuilder.put(greaterThanOrEqual.attribute).greaterThanEquals(greaterThanOrEqual.value)
      case in: In =>
        queryBuilder.put(in.attribute).in(in.values)
      case lessThan: LessThan =>
        queryBuilder.put(lessThan.attribute).lessThan(lessThan.value)
      case lessThanOrEqual: LessThanOrEqual =>
        queryBuilder.put(lessThanOrEqual.attribute).lessThanEquals(lessThanOrEqual.value)
    }
    queryBuilder.get

  }

  /**
   *
   * Prepared dbobject used to specify required fields in mongodb 'find'
   * @param fields Required fields
   * @return A mongodb object that represents required fields.
   */
  private def selectFields(fields: Array[String]): DBObject =
    MongoDBObject(fields.toList.map(_ -> 1))

}

case class MongodbReadException(
  msg: String,
  causedBy: Throwable) extends RuntimeException(msg,causedBy)
