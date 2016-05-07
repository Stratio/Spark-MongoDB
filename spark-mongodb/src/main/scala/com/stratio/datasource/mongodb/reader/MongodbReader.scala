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
package com.stratio.datasource.mongodb.reader

import com.mongodb.{MongoCredential, QueryBuilder}
import com.mongodb.casbah.Imports._
import com.stratio.datasource.Config
import com.stratio.datasource.mongodb.{MongodbCredentials, MongodbSSLOptions, MongodbClientFactory, MongodbConfig}
import com.stratio.datasource.mongodb.partitioner.MongodbPartition
import com.stratio.datasource.mongodb.query.FilterSection

import org.apache.spark.Partition
import scala.util.Try
import java.util.regex.Pattern

/**
 *
 * @param config Configuration object.
 * @param requiredColumns Pruning fields
 * @param filters Added query filters
 */
class MongodbReader(
                     config: Config,
                     requiredColumns: Array[String],
                     filters: FilterSection) {

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
        config.getOrElse[List[MongodbCredentials]](MongodbConfig.Credentials, MongodbConfig.DefaultCredentials).map{
          case MongodbCredentials(user,database,password) =>
            MongoCredential.createCredential(user,database,password)},
        config.get[MongodbSSLOptions](MongodbConfig.SSLOptions), config.properties.filterKeys(_.contains(MongodbConfig.ListMongoClientOptions))))

      val emptyFilter = MongoDBObject(List())
      val filter = Try(queryPartition(filters)).getOrElse(emptyFilter)

      dbCursor = (for {
        client <- mongoClient
        collection <- Option(client(config(MongodbConfig.Database))(config(MongodbConfig.Collection)))
        dbCursor <- Option(collection.find(filter, selectFields(requiredColumns)))
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
  private def queryPartition(filters: FilterSection): DBObject = {
    implicit val c: Config = config
    filters.filtersToDBObject()
  }

  private lazy val idAsObjectId: Boolean = config.getOrElse[String](MongodbConfig.IdAsObjectId, MongodbConfig.DefaultIdAsObjectId).equalsIgnoreCase("true")
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
