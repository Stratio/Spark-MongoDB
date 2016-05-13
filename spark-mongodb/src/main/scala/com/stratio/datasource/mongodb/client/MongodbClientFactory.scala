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

package com.stratio.datasource.mongodb.client

import javax.net.ssl.SSLSocketFactory

import com.mongodb.ServerAddress
import com.mongodb.casbah.Imports._
import com.mongodb.casbah.{MongoClient, MongoClientOptions}
import com.stratio.datasource.mongodb.config.MongodbConfig.{ReadPreference => ProviderReadPreference, _}
import com.stratio.datasource.mongodb.config.MongodbSSLOptions

/**
 * Different client configurations to Mongodb database
 */
// TODO Refactor - MongodbClientFactory should be used internally and should not delegate to other when closing/freeing connections
object MongodbClientFactory {

  type Client = MongoClient

  /**
   * Get or Create one client connection to MongoDb
   * @param host Ip or Dns to connect
   * @return Client connection with identifier
   */
  private[mongodb] def getClient(host: String): MongoClient = MongoClient(host)


  /**
   * Get or Create one client connection to MongoDb
   * @param host Ip or Dns to connect
   * @param port Port to connect
   * @param user User for credentials
   * @param database Database for credentials
   * @param password Password for credentials
   * @return Client connection with identifier
   */
  private[mongodb] def getClient(host: String, port: Int, user: String, database: String, password: String): MongoClient = {
    val credentials = List(MongoCredential.createCredential(user, database, password.toCharArray))
    val hostPort = new ServerAddress(host, port)
    createClient(List(hostPort), credentials)
  }

  /**
   * Get or Create one client connection to MongoDb
   * @param hostPort Server addresses to connect to one MongoDb ReplicaSet or Sharded Cluster
   * @param credentials Credentials to connect
   * @param optionSSLOptions SSL options for secure connections
   * @param clientOptions All options for the client connections
   * @return Client connection with identifier
   */
  private[mongodb] def getClient(hostPort: List[ServerAddress],
                                 credentials: List[MongoCredential] = List(),
                                 optionSSLOptions: Option[MongodbSSLOptions] = None,
                                 clientOptions: Map[String, Any] = Map()): MongoClient =
    createClient(hostPort, credentials, optionSSLOptions, clientOptions)


  private def createClient(hostPort: List[ServerAddress],
                           credentials: List[MongoCredential] = List(),
                           optionSSLOptions: Option[MongodbSSLOptions] = None,
                           clientOptions: Map[String, Any] = Map()): MongoClient = {

    val options = {
      val builder = new MongoClientOptions.Builder()
        .readPreference(extractValue[String](clientOptions, ProviderReadPreference) match {
        case Some(preference) => parseReadPreference(preference)
        case None => DefaultReadPreference
      })
        .connectTimeout(extractValue[String](clientOptions, ConnectTimeout).map(_.toInt)
        .getOrElse(DefaultConnectTimeout))
        .connectionsPerHost(extractValue[String](clientOptions, ConnectionsPerHost).map(_.toInt)
        .getOrElse(DefaultConnectionsPerHost))
        .maxWaitTime(extractValue[String](clientOptions, MaxWaitTime).map(_.toInt)
        .getOrElse(DefaultMaxWaitTime))
        .threadsAllowedToBlockForConnectionMultiplier(extractValue[String](clientOptions, ThreadsAllowedToBlockForConnectionMultiplier).map(_.toInt)
        .getOrElse(DefaultThreadsAllowedToBlockForConnectionMultiplier))

      if (sslBuilder(optionSSLOptions)) builder.socketFactory(SSLSocketFactory.getDefault)

      builder.build()
    }

    MongoClient(hostPort, credentials, options)
  }

  // TODO Review when refactoring config
  private def extractValue[T](options: Map[String, Any], key: String): Option[T] =
    options.get(key.toLowerCase).map(_.asInstanceOf[T])

  private def sslBuilder(optionSSLOptions: Option[MongodbSSLOptions]): Boolean =
    optionSSLOptions.exists(sslOptions => {
      if (sslOptions.keyStore.nonEmpty) {
        System.setProperty("javax.net.ssl.keyStore", sslOptions.keyStore.get)
        if (sslOptions.keyStorePassword.nonEmpty)
          System.setProperty("javax.net.ssl.keyStorePassword", sslOptions.keyStorePassword.get)
      }
      if (sslOptions.trustStore.nonEmpty) {
        System.setProperty("javax.net.ssl.trustStore", sslOptions.trustStore)
        if (sslOptions.trustStorePassword.nonEmpty)
          System.setProperty("javax.net.ssl.trustStorePassword", sslOptions.trustStorePassword.get)
      }
      true
    })

}
