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
package com.stratio.datasource.mongodb.writer

import com.mongodb.casbah.Imports._
import com.stratio.datasource.mongodb.client.MongodbClientFactory
import com.stratio.datasource.mongodb.config.{MongodbConfigReader, MongodbConfig}
import com.stratio.datasource.mongodb.util.usingMongoClient
import com.stratio.datasource.util.Config

/**
 * Abstract Mongodb writer.
 * Used for saving a bunch of mongodb objects
 * into specified database and collection
 *
 * @param config Configuration parameters (host,database,collection,...)
 */
protected[mongodb] abstract class MongodbWriter(config: Config) extends Serializable {

  import MongodbConfigReader._

  private val languageConfig = config.get[String](MongodbConfig.Language)

  private val connectionsTime = config.get[String](MongodbConfig.ConnectionsTime).map(_.toLong)

  protected val writeConcern = config.writeConcern

  /**
   * A MongoDB collection created from the specified database and collection.
   */
  protected def dbCollection(mongoClient: MongoClient): MongoCollection =
    mongoClient(config(MongodbConfig.Database))(config(MongodbConfig.Collection))

  /**
   * Abstract method that checks if a primary key exists in provided configuration
   * and the language parameter.
   * Then calls the 'save' method.
   *
   * @param it DBObject iterator.
   */
  def saveWithPk(it: Iterator[DBObject]): Unit = {
    val itModified = if (languageConfig.isDefined) {
      it.map {
        case obj: BasicDBObject =>
          if (languageConfig.isDefined) obj.append("language", languageConfig.get)
          obj
      }
    } else it

    usingMongoClient(MongodbClientFactory.getClient(config.hosts, config.credentials, config.sslOptions, config.clientOptions).clientConnection) { mongoClient =>
      save(itModified, mongoClient: MongoClient)
    }
  }

  /**
   * Abstract method for storing a bunch of MongoDB objects.
   *
   * @param it Iterator of mongodb objects.
   */
  def save(it: Iterator[DBObject], mongoClient: MongoClient): Unit

}