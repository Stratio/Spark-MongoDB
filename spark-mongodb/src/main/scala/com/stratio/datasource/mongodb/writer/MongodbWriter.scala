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
import com.mongodb.{MongoCredential, ServerAddress}
import com.stratio.datasource.Config
import com.stratio.datasource.mongodb.MongodbClientFactory.Client
import com.stratio.datasource.mongodb.MongodbConfig._
import com.stratio.datasource.mongodb.{MongodbClientFactory, MongodbConfig, MongodbCredentials, MongodbSSLOptions}
import com.stratio.datasource.mongodb.query.FilterSection

/**
 * Abstract Mongodb writer.
 * Used for saving a bunch of mongodb objects
 * into specified database and collection
 *
 * @param config Configuration parameters (host,database,collection,...)
 */
abstract class MongodbWriter(config: Config) extends Serializable {

  /**
   * A MongoDB client is created for each writer.
   */

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


  private val clientOptions = config.properties.filterKeys(_.contains(MongodbConfig.ListMongoClientOptions))

  private val databaseName: String = config(MongodbConfig.Database)

  private val collectionName: String = config(MongodbConfig.Collection)

  private val collectionFullName: String = s"$databaseName.$collectionName"

  protected val mongoClient: Client = MongodbClientFactory.createClient(hosts, credentials, ssloptions, clientOptions)
  
  /**
   * A MongoDB collection created from the specified database and collection.
   */
  protected val dbCollection: MongoCollection =
    mongoClient(config(MongodbConfig.Database))(config(MongodbConfig.Collection))

  /**
   * Abstract method that checks if a primary key exists in provided configuration
   * and the language parameter.
   * Then calls the 'save' method.
   *
   * @param it DBObject iterator.
   */
  def saveWithPk (it: Iterator[DBObject]): Unit = {
    val languageConfig = config.get[String](MongodbConfig.Language)
    val itModified = it.map { case obj: BasicDBObject => {
      if(languageConfig.isDefined) obj.append("language", languageConfig.get)
      obj
    }}
    save(itModified)
  }

  /**
   * Abstract method for storing a bunch of MongoDB objects.
   *
   * @param it Iterator of mongodb objects.
   */
  def save(it: Iterator[DBObject]): Unit

  /**
   * Drop MongoDB collection.
   */
  def dropCollection: Unit = dbCollection.dropCollection()

  /**
   * Indicates if a collection is empty.
   */
  def isEmpty: Boolean = dbCollection.isEmpty

  /**
   * Close current MongoDB client.
   */
  def close(): Unit = {
    mongoClient.close()
  }

}