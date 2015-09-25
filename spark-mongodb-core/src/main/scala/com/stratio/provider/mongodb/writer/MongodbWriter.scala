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

package com.stratio.provider.mongodb.writer

import com.mongodb.casbah.Imports._
import com.mongodb.{MongoCredential, ServerAddress}
import com.stratio.provider.Config
import com.stratio.provider.mongodb.MongodbClientFactory.Client
import com.stratio.provider.mongodb.{MongodbClientFactory, MongodbConfig, MongodbCredentials, MongodbSSLOptions}

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
//  protected val mongoClient: MongodbClientFactory.Client =
//    MongodbClientFactory.createClient(
//      config[List[String]](MongodbConfig.Host)
//        .map(add => new ServerAddress(add)),
//      config[List[MongodbCredentials]](MongodbConfig.Credentials).map{
//        case MongodbCredentials(user,database,password) =>
//          MongoCredential.createCredential(user,database,password)},
//      config.get[MongodbSSLOptions](MongodbConfig.SSLOptions), config[String](MongodbConfig.Timeout))

  @transient private val hosts: List[ServerAddress] =
    config[List[String]](MongodbConfig.Host)
      .map(add => new ServerAddress(add))

  @transient private val credentials: List[MongoCredential] =
    config[List[MongodbCredentials]](MongodbConfig.Credentials).map{
      case MongodbCredentials(user,database,password) =>
        MongoCredential.createCredential(user,database,password)
    }

  @transient private val ssloptions: Option[MongodbSSLOptions] =
    config.get[MongodbSSLOptions](MongodbConfig.SSLOptions)

  @transient private val readpreference: String = config[String](MongodbConfig.readPreference)

  private val timeout: Option[String] = config.get[String](MongodbConfig.Timeout)

  private val databaseName: String = config(MongodbConfig.Database)

  private val collectionName: String = config(MongodbConfig.Collection)

  private val collectionFullName: String = s"$databaseName.$collectionName"


  protected val mongoClient: Client = MongodbClientFactory.createClient(hosts,credentials, ssloptions, readpreference, timeout)




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
    val idFieldConfig = config.get[String](MongodbConfig.IdField)
    val languageConfig = config.get[String](MongodbConfig.Language)
    val itModified = it.map { case obj: BasicDBObject => {
      if(idFieldConfig.isDefined) obj.append("_id", obj.get(idFieldConfig.get))
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
   * Drop MongoDB collection.
   */
  def isEmpty: Boolean = dbCollection.isEmpty

  /**
   * Close current MongoDB client.
   */
  def close(): Unit = {
    mongoClient.close()
  }

}