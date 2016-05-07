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
package com.stratio.datasource.mongodb

import javax.net.ssl.SSLSocketFactory
import com.stratio.datasource.mongodb.MongodbConfig._
import com.stratio.datasource.mongodb.MongodbConfig.{ReadPreference => ProviderReadPreference}
import com.mongodb.ServerAddress
import com.mongodb.casbah.Imports._
import com.mongodb.casbah.{ReadPreference, MongoClient, MongoClientOptions}

/**
 * Different client configurations to Mongodb database
 */
object MongodbClientFactory {

  type Client = MongoClient

  def createClient(host : String) : Client = MongoClient(host)

  def createClient(host : String, port : Int, user: String, database: String, password :String) : Client = {

    val credentials : List[MongoCredential]= List(MongoCredential.createCredential(user, database, password.toCharArray))
    val hostPort = new ServerAddress(host, port)
    MongoClient(hostPort, credentials)
  }

   def createClient(
                    hostPort : List[ServerAddress],
                    credentials : List[MongoCredential] = List(),
                    optionSSLOptions: Option[MongodbSSLOptions] = None,
                    clientOptions: Map[String, Any] = Map()) : Client = {

    val options = {

      val builder = new MongoClientOptions.Builder()
        .readPreference(parseReadPreference(extractValue(clientOptions, ProviderReadPreference).getOrElse(DefaultReadPreference)))
        .connectTimeout(extractValue[String](clientOptions, ConnectTimeout).map(_.toInt).getOrElse(DefaultConnectTimeout))
        .connectionsPerHost(extractValue[String](clientOptions, ConnectionsPerHost).map(_.toInt).getOrElse(DefaultConnectionsPerHost))
        .maxWaitTime(extractValue[String](clientOptions, MaxWaitTime).map(_.toInt).getOrElse(DefaultMaxWaitTime))
        .threadsAllowedToBlockForConnectionMultiplier(extractValue[String](clientOptions, ThreadsAllowedToBlockForConnectionMultiplier).map(_.toInt).getOrElse(DefaultThreadsAllowedToBlockForConnectionMultiplier))

      if (sslBuilder(optionSSLOptions)) builder.socketFactory(SSLSocketFactory.getDefault())

      builder.build()
    }

    MongoClient(hostPort, credentials, options)

  }

  private def extractValue[T](options :Map[String, Any], key : String): Option[T] = options.get(key).map(_.asInstanceOf[T])

  private def sslBuilder(optionSSLOptions: Option[MongodbSSLOptions]): Boolean = {

    optionSSLOptions match{
      case Some(sslOptions) =>

        if(sslOptions.keyStore.nonEmpty) {
          System.setProperty("javax.net.ssl.keyStore", sslOptions.keyStore.get)
          if (sslOptions.keyStorePassword.nonEmpty)
            System.setProperty("javax.net.ssl.keyStorePassword", sslOptions.keyStorePassword.get)
        }
        if(sslOptions.trustStore.nonEmpty) {
          System.setProperty("javax.net.ssl.trustStore", sslOptions.trustStore)
          if (sslOptions.trustStorePassword.nonEmpty)
            System.setProperty("javax.net.ssl.trustStorePassword", sslOptions.trustStorePassword.get)
        }

        true

      case _ => false
    }

  }

  private def parseReadPreference(readPreference: String): ReadPreference ={
    readPreference match{
      case "primary"             => ReadPreference.Primary
      case "secondary"           => ReadPreference.Secondary
      case "nearest"             => ReadPreference.Nearest
      case "primaryPreferred"    => ReadPreference.primaryPreferred
      case "secondaryPreferred"  => ReadPreference.SecondaryPreferred
      case _                     => ReadPreference.Nearest
    }
  }

}
