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

package com.stratio.provider.mongodb.reader



import com.mongodb.{MongoCredential, QueryBuilder}
import com.mongodb.casbah.Imports._
import com.stratio.provider.DeepConfig
import com.stratio.provider.mongodb.{MongodbCredentials, MongodbSSLOptions, MongodbClientFactory, MongodbConfig}
import com.stratio.provider.mongodb.partitioner.MongodbPartition
import org.apache.spark.Partition
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.UTF8String
import scala.util.Try
import java.util.regex.Pattern

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

  private var mongoClient: Option[MongodbClientFactory.Client] = None

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

      mongoClient = Option(MongodbClientFactory.createClient(
        mongoPartition.hosts.map(add => new ServerAddress(add)).toList,
        config[List[MongodbCredentials]](MongodbConfig.Credentials).map{
          case MongodbCredentials(user,database,password) =>
            MongoCredential.createCredential(user,database,password)},
        config.get[MongodbSSLOptions](MongodbConfig.SSLOptions), config[String](MongodbConfig.readPreference)))

      dbCursor = (for {
        client <- mongoClient
        collection <- Option(client(config(MongodbConfig.Database))(config(MongodbConfig.Collection)))
        dbCursor <- Option(collection.find(queryPartition(filters), selectFields(requiredColumns)))
      } yield {
        mongoPartition.partitionRange.minKey.foreach(min => dbCursor.addSpecial("$min", min))
        mongoPartition.partitionRange.maxKey.foreach(max => dbCursor.addSpecial("$max", max))
        dbCursor
      }).headOption

    }.recover {
      case throwable =>
        throw MongodbReadException(throwable.getMessage, throwable)
    }

  /**
   * Create query partition using given filters.
   *
   * @param filters the Spark filters to be converted to Mongo filters
   * @return the dB object
   */
  private def queryPartition(
    filters: Array[Filter]): DBObject = {

    def filtersToDBObject( sFilters: Array[Filter] ): DBObject = {
      val queryBuilder: QueryBuilder = QueryBuilder.start

      sFilters.foreach {
        case EqualTo(attribute, value) =>
          queryBuilder.put(attribute).is(convertToStandardType(value))
        case GreaterThan(attribute, value) =>
          queryBuilder.put(attribute).greaterThan(convertToStandardType(value))
        case GreaterThanOrEqual(attribute, value) =>
          queryBuilder.put(attribute).greaterThanEquals(convertToStandardType(value))
        case In(attribute, values) =>
          queryBuilder.put(attribute).in(values.map(convertToStandardType))
        case LessThan(attribute, value) =>
          queryBuilder.put(attribute).lessThan(convertToStandardType(value))
        case LessThanOrEqual(attribute, value) =>
          queryBuilder.put(attribute).lessThanEquals(convertToStandardType(value))
        case IsNull(attribute) =>
          queryBuilder.put(attribute).is(null)
        case IsNotNull(attribute) =>
          queryBuilder.put(attribute).notEquals(null)
        case And(leftFilter, rightFilter) =>
          queryBuilder.and(filtersToDBObject(Array(leftFilter)), filtersToDBObject(Array(rightFilter)))
        case Or(leftFilter, rightFilter) =>
          queryBuilder.or(filtersToDBObject(Array(leftFilter)), filtersToDBObject(Array(rightFilter)))
        case StringStartsWith(attribute, value) =>
          queryBuilder.put(attribute).regex(Pattern.compile("^" + value + ".*$"))
        case StringEndsWith(attribute, value) =>
          queryBuilder.put(attribute).regex(Pattern.compile("^.*" + value + "$"))
        case StringContains(attribute, value) =>
          queryBuilder.put(attribute).regex(Pattern.compile(".*" + value + ".*"))
        // TODO Not filter
      }

      queryBuilder.get
    }

    filtersToDBObject(filters)
  }

  private def convertToStandardType(value: Any): Any = {
    if (value.isInstanceOf[UTF8String])
      value.toString
    else
      value
  }

  /**
   *
   * Prepared DBObject used to specify required fields in mongodb 'find'
   * @param fields Required fields
   * @return A mongodb object that represents required fields.
   */
  private def selectFields(fields: Array[String]): DBObject =
    MongoDBObject(
      if (fields.isEmpty) List()
      else fields.toList.filterNot(_ == "_id").map(_ -> 1) ::: {
        List("_id" -> fields.find(_ == "_id").fold(0)(_ => 1))
      })

}

case class MongodbReadException(
  msg: String,
  causedBy: Throwable) extends RuntimeException(msg, causedBy)
