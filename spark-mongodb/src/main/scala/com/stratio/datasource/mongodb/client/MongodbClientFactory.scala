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

import java.util.concurrent._
import javax.net.ssl.SSLSocketFactory

import akka.actor.ActorSystem
import com.mongodb.ServerAddress
import com.mongodb.casbah.Imports._
import com.mongodb.casbah.{MongoClient, MongoClientOptions}
import com.stratio.datasource.mongodb.config.MongodbConfig.{ReadPreference => ProviderReadPreference, _}
import com.stratio.datasource.mongodb.config.{MongodbConfig, MongodbSSLOptions}

import scala.annotation.tailrec
import scala.collection.JavaConversions._
import scala.concurrent.duration._
import scala.util.Try

/**
 * Different client configurations to Mongodb database
 */
object MongodbClientFactory {

  type Client = MongoClient

  val KeySeparator = "-"

  val CloseSleepTime = 1000

  val CloseAttempts = 180

  /**
   * MongoDb Client connections are saved in a Concurrent hashMap, is used to have only one concurrent connection for
   * each operation, when this are finished the connection are reused
   */
  var mongoClient: scala.collection.concurrent.Map[String, MongodbConnection] =
    new ConcurrentHashMap[String, MongodbConnection]()

  /**
   * Scheduler that close connections automatically when the timeout was expired
   */
  val actorSystem = ActorSystem()
  val scheduler = actorSystem.scheduler
  val SecondsToCheckConnections = 60
  val task = new Runnable {
    def run() {
      synchronized {
        val currentTime = System.currentTimeMillis()
        mongoClient.foreach { case (key, connection) =>
          if((connection.status == ConnectionStatus.Free) && (connection.timeOut <= currentTime)) {
            connection.client.close()
            mongoClient.remove(key)
          }
        }
      }
    }
  }
  implicit val executor = actorSystem.dispatcher
  scheduler.schedule(
    initialDelay = Duration(SecondsToCheckConnections, TimeUnit.SECONDS),
    interval = Duration(SecondsToCheckConnections, TimeUnit.SECONDS),
    runnable = task)

  /**
   * Get or Create one client connection to MongoDb
   * @param host Ip or Dns to connect
   * @return Client connection
   */
  def getClient(host: String): Client = {
    synchronized {
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

      connection.client
    }
  }

  /**
   * Get or Create one client connection to MongoDb
   * @param host Ip or Dns to connect
   * @param port Port to connect
   * @param user User for credentials
   * @param database Database for credentials
   * @param password Password for credentials
   * @return Client connection
   */
  def getClient(host: String, port: Int, user: String, database: String, password: String): Client = {
    synchronized {
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

      connection.client
    }
  }

  /**
   * Get or Create one client connection to MongoDb
   * @param hostPort Server addresses to connect to one MongoDb ReplicaSet or Sharded Cluster
   * @param credentials Credentials to connect
   * @param optionSSLOptions SSL options for secure connections
   * @param clientOptions All options for the client connections
   * @return Client connection
   */
  def getClient(hostPort: List[ServerAddress],
                credentials: List[MongoCredential] = List(),
                optionSSLOptions: Option[MongodbSSLOptions] = None,
                clientOptions: Map[String, Any] = Map()): Client = {
    synchronized {
      val connKey = connectionKey(0, hostPort, credentials, clientOptions)
      val (finalKey, connection) = mongoClient.get(connKey) match {
        case Some(client) =>
          if (client.status == ConnectionStatus.Free) (connKey,client)
          else createClient(1 + connKey, hostPort, credentials, optionSSLOptions, clientOptions)
        case None => createClient(connKey, hostPort, credentials, optionSSLOptions, clientOptions)
      }

      mongoClient.update(finalKey, connection.copy(
        timeOut = System.currentTimeMillis() +
          extractValue[String](clientOptions, ConnectionsTime).map(_.toLong).getOrElse(DefaultConnectionsTime),
        status = ConnectionStatus.Busy))

      connection.client
    }
  }

  private def createClient(key: String , host: String): (String ,MongodbConnection) = {
    saveConnection(key, MongodbConnection(MongoClient(host)))
  }

  private def createClient(key: String,
                           hostPort: List[ServerAddress],
                           credentials: List[MongoCredential] = List(),
                           optionSSLOptions: Option[MongodbSSLOptions] = None,
                           clientOptions: Map[String, Any] = Map()): (String ,MongodbConnection) = {

    val options = {

      val builder = new MongoClientOptions.Builder()
        .readPreference(extractValue[String](clientOptions, ProviderReadPreference) match {
          case Some(preference) => parseReadPreference(preference)
          case None => DefaultReadPreference
        })
        .connectTimeout(extractValue[String](clientOptions, ConnectTimeout).map(_.toInt).getOrElse(DefaultConnectTimeout))
        .connectionsPerHost(extractValue[String](clientOptions, ConnectionsPerHost).map(_.toInt).getOrElse(DefaultConnectionsPerHost))
        .maxWaitTime(extractValue[String](clientOptions, MaxWaitTime).map(_.toInt).getOrElse(DefaultMaxWaitTime))
        .threadsAllowedToBlockForConnectionMultiplier(extractValue[String](clientOptions, ThreadsAllowedToBlockForConnectionMultiplier).map(_.toInt).getOrElse(DefaultThreadsAllowedToBlockForConnectionMultiplier))

      if (sslBuilder(optionSSLOptions)) builder.socketFactory(SSLSocketFactory.getDefault())

      builder.build()
    }

    saveConnection(key, MongodbConnection(MongoClient(hostPort, credentials, options)))
  }

  @tailrec
  private def saveConnection(key: String, mongoDbConnection : MongodbConnection) : (String, MongodbConnection) = {
    mongoClient.putIfAbsent(key, mongoDbConnection) match {
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
  private def connectionKey(index : Int,
                            hostPort: List[ServerAddress],
                            credentials: List[MongoCredential] = List(),
                            clientOptions: Map[String, Any] = Map()): String = {
    val key = if (clientOptions.nonEmpty)
      s"$index-${clientOptions.mkString(KeySeparator)}"
    else s"$index$KeySeparator${hostPort.mkString(KeySeparator)}$KeySeparator${credentials.mkString(KeySeparator)}"

    val clientFound = mongoClient.find { case(clientKey, connection) =>
      clientKey == key && connection.status == ConnectionStatus.Busy
    }

    clientFound match {
      case Some(client) => connectionKey(index + 1, hostPort, credentials, clientOptions)
      case None => key
    }
  }

  /**
   * Close all client connections on the concurrent map
   * @param gracefully Close the connections if is free
   */
  def closeAll(gracefully : Boolean = true, attempts : Int = CloseAttempts): Unit = {
      mongoClient.foreach { case (key, connection) =>
        if(!gracefully || connection.status == ConnectionStatus.Free) {
          connection.client.close()
          mongoClient.remove(key)
        }
      }
      if(mongoClient.nonEmpty && attempts > 0){
        Thread.sleep(CloseSleepTime)
        closeAll(gracefully, attempts - 1)
      }
  }

  /**
   * Close the connections that have the same client as the client param
   * @param client client value for connect to MongoDb
   * @param gracefully Close the connection if is free
   */
  def close(client: Client, gracefully: Boolean = true): Unit = {
    mongoClient.find { case (key, clientSearch) => clientSearch.client == client }
      .foreach { case (key, clientFound) =>
        if(!gracefully || clientFound.status == ConnectionStatus.Free) {
          clientFound.client.close()
          mongoClient.remove(key)
        }
      }
  }

  /**
   * Close the connections that have the same key as the clientKey param
   * @param clientKey key pre calculated with the connection options
   * @param gracefully Close the connection if is free
   */
  def closeByKey(clientKey: String, gracefully: Boolean = true): Unit = {
    mongoClient.get(clientKey).foreach(clientFound => {
      if(!gracefully || clientFound.status == ConnectionStatus.Free) {
        clientFound.client.close()
        mongoClient.remove(clientKey)
      }
    })
  }

  /**
   * Set Free the connection that have the same client as the client param
   * @param client client value for connect to MongoDb
   */
  def setFreeConnection(client: Client, extendedTime : Option[Long] = None): Unit = {
    mongoClient.find { case (key, clientSearch) => clientSearch.client == client }
      .foreach { case (key, clientFound) =>
        mongoClient.update(key, clientFound.copy(status = ConnectionStatus.Free,
          timeOut = System.currentTimeMillis() + extendedTime.getOrElse(DefaultConnectionsTime)))
      }
  }

  /**
   * Set Free the connection that have the same key as the clientKey param
   * @param clientKey key pre calculated with the connection options
   */
  def setFreeConnectionByKey(clientKey: String, extendedTime : Option[Long] = None): Unit = {
    mongoClient.get(clientKey).foreach(clientFound => {
      mongoClient.update(clientKey, clientFound.copy(status = ConnectionStatus.Free,
        timeOut = System.currentTimeMillis() + extendedTime.getOrElse(DefaultConnectionsTime)))
    })
  }

  private def extractValue[T](options: Map[String, Any], key: String): Option[T] = options.get(key).map(_.asInstanceOf[T])

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
