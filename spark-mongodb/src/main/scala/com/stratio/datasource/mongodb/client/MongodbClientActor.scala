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

import akka.actor.Actor
import com.mongodb.ServerAddress
import com.mongodb.casbah.Imports._
import com.mongodb.casbah.{MongoClient, MongoClientOptions}
import com.stratio.datasource.mongodb.client.MongodbClientActor._
import com.stratio.datasource.mongodb.client.MongodbClientFactory._
import com.stratio.datasource.mongodb.config.MongodbConfig.{ReadPreference => ProviderReadPreference, _}
import com.stratio.datasource.mongodb.config.MongodbSSLOptions

import scala.annotation.tailrec
import scala.util.Try


class MongodbClientActor extends Actor {

  private val KeySeparator = "-"

  private val CloseSleepTime = 1000

  private val mongoClient: scala.collection.mutable.Map[String, MongodbConnection] =
    scala.collection.mutable.Map.empty[String, MongodbConnection]

  override def receive = {
    case CheckConnections => doCheckConnections()
    case GetClient(host) => doGetClient(host)
    case GetClientWithMongoDbConfig(hostPort, credentials, optionSSLOptions, clientOptions) =>
      doGetClientWithMongoDbConfig(hostPort, credentials, optionSSLOptions, clientOptions)
    case GetClientWithUser(host, port, user, database, password) =>
      doGetClientWithUser(host, port, user, database, password)
    case SetFreeConnectionByKey(clientKey, extendedTime) => doSetFreeConnectionByKey(clientKey, extendedTime)
    case SetFreeConnectionsByClient(client, extendedTime) => doSetFreeConnectionByClient(client, extendedTime)
    case CloseAll(gracefully, attempts) => doCloseAll(gracefully, attempts)
    case CloseByClient(client, gracefully) => doCloseByClient(client, gracefully)
    case CloseByKey(clientKey, gracefully) => doCloseByKey(clientKey, gracefully)
    case GetSize => doGetSize
  }

  private def doCheckConnections(): Unit = {
    val currentTime = System.currentTimeMillis()
    mongoClient.foreach { case (key, connection) =>
      if ((connection.status == ConnectionStatus.Free) && (connection.timeOut <= currentTime)) {
        connection.client.close()
        mongoClient.remove(key)
      }
    }
  }

  private def doGetClientWithMongoDbConfig(hostPort: List[ServerAddress],
                                           credentials: List[MongoCredential],
                                           optionSSLOptions: Option[MongodbSSLOptions],
                                           clientOptions: Map[String, Any]): Unit = {
    val connKey = connectionKey(0, hostPort, credentials, clientOptions)
    val (finalKey, connection) = mongoClient.get(connKey) match {
      case Some(client) =>
        if (client.status == ConnectionStatus.Free) (connKey, client)
        else createClient(1 + connKey, hostPort, credentials, optionSSLOptions, clientOptions)
      case None => createClient(connKey, hostPort, credentials, optionSSLOptions, clientOptions)
    }

    mongoClient.update(finalKey, connection.copy(
      timeOut = System.currentTimeMillis() +
        extractValue[Long](clientOptions, ConnectionsTime).getOrElse(DefaultConnectionsTime),
      status = ConnectionStatus.Busy))

    sender ! ClientResponse(finalKey, connection.client)
  }

  private def doGetClientWithUser(host: String, port: Int, user: String, database: String, password: String): Unit = {
    val credentials = List(MongoCredential.createCredential(user, database, password.toCharArray))
    val hostPort = new ServerAddress(host, port)
    val connKey = connectionKey(0, List(hostPort), credentials)

    val (finalKey, connection) = mongoClient.get(connKey) match {
      case Some(client) =>
        if (client.status == ConnectionStatus.Free) (connKey, client)
        else createClient(1 + connKey, List(hostPort), credentials)
      case None => createClient(connKey, List(hostPort), credentials)
    }

    mongoClient.update(finalKey, connection.copy(
      timeOut = System.currentTimeMillis() + DefaultConnectionsTime,
      status = ConnectionStatus.Busy))

    sender ! ClientResponse(finalKey, connection.client)
  }

  private def doGetClient(host: String): Unit = {
    val hostPort = new ServerAddress(host)
    val connKey = connectionKey(0, List(hostPort))
    val (finalKey, connection) = mongoClient.get(connKey) match {
      case Some(client) =>
        if (client.status == ConnectionStatus.Free) (connKey, client)
        else createClient(connKey, host)
      case None => createClient(connKey, host)
    }

    mongoClient.update(finalKey, connection.copy(
      timeOut = System.currentTimeMillis() + DefaultConnectionsTime,
      status = ConnectionStatus.Busy))

    sender ! ClientResponse(finalKey, connection.client)
  }

  private def doSetFreeConnectionByKey(clientKey: String, extendedTime: Option[Long]): Unit = {
    mongoClient.get(clientKey).foreach(clientFound => {
      mongoClient.update(clientKey, clientFound.copy(status = ConnectionStatus.Free,
        timeOut = System.currentTimeMillis() + extendedTime.getOrElse(DefaultConnectionsTime)))
    })
  }

  private def doSetFreeConnectionByClient(client: Client, extendedTime: Option[Long]): Unit = {
    mongoClient.find { case (key, clientSearch) => clientSearch.client == client }
      .foreach { case (key, clientFound) =>
      mongoClient.update(key, clientFound.copy(status = ConnectionStatus.Free,
        timeOut = System.currentTimeMillis() + extendedTime.getOrElse(DefaultConnectionsTime)))
    }
  }

  private def doCloseAll(gracefully: Boolean, attempts: Int): Unit = {
    mongoClient.foreach { case (key, connection) =>
      if (!gracefully || connection.status == ConnectionStatus.Free) {
        connection.client.close()
        mongoClient.remove(key)
      }
    }
    if (mongoClient.nonEmpty && attempts > 0) {
      Thread.sleep(CloseSleepTime)
      doCloseAll(gracefully, attempts - 1)
    }
  }

  private def doCloseByClient(client: Client, gracefully: Boolean): Unit = {
    mongoClient.find { case (key, clientSearch) => clientSearch.client == client }
      .foreach { case (key, clientFound) =>
      if (!gracefully || clientFound.status == ConnectionStatus.Free) {
        clientFound.client.close()
        mongoClient.remove(key)
      }
    }
  }

  private def doCloseByKey(clientKey: String, gracefully: Boolean): Unit = {
    mongoClient.get(clientKey).foreach(clientFound => {
      if (!gracefully || clientFound.status == ConnectionStatus.Free) {
        clientFound.client.close()
        mongoClient.remove(clientKey)
      }
    })
  }

  private def doGetSize() : Unit = sender ! mongoClient.size

  private def createClient(key: String, host: String): (String, MongodbConnection) = {
    saveConnection(key, MongodbConnection(MongoClient(host)))
  }

  private def createClient(key: String,
                           hostPort: List[ServerAddress],
                           credentials: List[MongoCredential] = List(),
                           optionSSLOptions: Option[MongodbSSLOptions] = None,
                           clientOptions: Map[String, Any] = Map()): (String, MongodbConnection) = {

    val options = {

      val builder = new MongoClientOptions.Builder()
        .readPreference(extractValue[String](clientOptions, ProviderReadPreference) match {
        case Some(preference) => parseReadPreference(preference)
        case None => DefaultReadPreference
      })
        .connectTimeout(extractValue[Int](clientOptions, ConnectTimeout).getOrElse(DefaultConnectTimeout))
        .connectionsPerHost(extractValue[Int](clientOptions, ConnectionsPerHost).getOrElse(DefaultConnectionsPerHost))
        .maxWaitTime(extractValue[Int](clientOptions, MaxWaitTime).getOrElse(DefaultMaxWaitTime))
        .threadsAllowedToBlockForConnectionMultiplier(extractValue[Int](clientOptions, ThreadsAllowedToBlockForConnectionMultiplier)
        .getOrElse(DefaultThreadsAllowedToBlockForConnectionMultiplier))

      if (sslBuilder(optionSSLOptions)) builder.socketFactory(SSLSocketFactory.getDefault())

      builder.build()
    }

    saveConnection(key, MongodbConnection(MongoClient(hostPort, credentials, options)))
  }

  @tailrec
  private def saveConnection(key: String, mongoDbConnection: MongodbConnection): (String, MongodbConnection) = {
    mongoClient.put(key, mongoDbConnection) match {
      case Some(_) =>
        val splittedKey = key.split(KeySeparator)
        val index = splittedKey.headOption match {
          case Some(indexNumber) => Try(indexNumber.toInt + 1).getOrElse(0)
          case None => 0
        }
        saveConnection(s"$index${splittedKey.drop(1).mkString(KeySeparator)}", mongoDbConnection)
      case None => (key, mongoDbConnection)
    }
  }

  /**
   * Create the connection string for the concurrent hashMap, the params make the unique key
   * @param index Index for the same concurrent connections to the same database with the same options
   * @param hostPort List of servers addresses
   * @param credentials Credentials for connect
   * @param clientOptions All options for the client connections
   * @return The calculated string
   */
  @tailrec
  private def connectionKey(index: Int,
                            hostPort: List[ServerAddress],
                            credentials: List[MongoCredential] = List(),
                            clientOptions: Map[String, Any] = Map()): String = {
    val key = if (clientOptions.nonEmpty)
      s"$index-${clientOptions.mkString(KeySeparator)}"
    else s"$index$KeySeparator${hostPort.mkString(KeySeparator)}$KeySeparator${credentials.mkString(KeySeparator)}"

    val clientFound = mongoClient.find { case (clientKey, connection) =>
      clientKey == key && connection.status == ConnectionStatus.Busy
    }

    clientFound match {
      case Some(client) => connectionKey(index + 1, hostPort, credentials, clientOptions)
      case None => key
    }
  }

}

object MongodbClientActor {

  case object CheckConnections

  case class GetClient(host: String)

  case class GetClientWithUser(host: String, port: Int, user: String, database: String, password: String)

  case class GetClientWithMongoDbConfig(hostPort: List[ServerAddress],
                                        credentials: List[MongoCredential],
                                        optionSSLOptions: Option[MongodbSSLOptions],
                                        clientOptions: Map[String, Any])

  case class ClientResponse(key: String, clientConnection: Client)

  case class SetFreeConnectionsByClient(client: Client, extendedTime: Option[Long])

  case class SetFreeConnectionByKey(clientKey: String, extendedTime: Option[Long])

  case class CloseAll(gracefully: Boolean, attempts: Int)

  case class CloseByClient(client: Client, gracefully: Boolean)

  case class CloseByKey(clientKey: String, gracefully: Boolean)

  case object GetSize

}
