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

import akka.actor.{ActorSystem, Props}
import akka.pattern.ask
import akka.util.Timeout
import com.mongodb.ServerAddress
import com.mongodb.casbah.Imports._
import com.mongodb.casbah.MongoClient
import com.stratio.datasource.mongodb.client.MongodbClientActor._
import com.stratio.datasource.mongodb.config.MongodbSSLOptions
import com.typesafe.config.ConfigFactory

import scala.concurrent.Await
import scala.concurrent.duration._


/**
 * Different client configurations to Mongodb database
 */
object MongodbClientFactory {

  type Client = MongoClient

  private val CloseAttempts = 120

  /**
   * Scheduler that close connections automatically when the timeout was expired
   */
  private val actorSystem = ActorSystem("mongodbClientFactory", ConfigFactory.load(ConfigFactory.parseString("akka.daemonic=on")))
  private val scheduler = actorSystem.scheduler
  private val SecondsToCheckConnections = 60
  private val mongoConnectionsActor = actorSystem.actorOf(Props(new MongodbClientActor), "mongoConnectionActor")

  private implicit val executor = actorSystem.dispatcher
  private implicit val timeout: Timeout = Timeout(3.seconds)

  scheduler.schedule(
    initialDelay = Duration(SecondsToCheckConnections, TimeUnit.SECONDS),
    interval = Duration(SecondsToCheckConnections, TimeUnit.SECONDS),
    mongoConnectionsActor,
    CheckConnections)

  /**
   * Get or Create one client connection to MongoDb
   * @param host Ip or Dns to connect
   * @return Client connection with identifier
   */
  def getClient(host: String): ClientResponse = {
    val futureResult = mongoConnectionsActor ? GetClient(host)
    Await.result(futureResult, timeout.duration) match {
      case ClientResponse(key, clientConnection) => ClientResponse(key, clientConnection)
    }
  }

  /**
   * Get or Create one client connection to MongoDb
   * @param host Ip or Dns to connect
   * @param port Port to connect
   * @param user User for credentials
   * @param database Database for credentials
   * @param password Password for credentials
   * @return Client connection with identifier
   */
  def getClient(host: String, port: Int, user: String, database: String, password: String): ClientResponse = {
    val futureResult = mongoConnectionsActor ? GetClientWithUser(host, port, user, database, password)
    Await.result(futureResult, timeout.duration) match {
      case ClientResponse(key, clientConnection) => ClientResponse(key, clientConnection)
    }
  }

  /**
   * Get or Create one client connection to MongoDb
   * @param hostPort Server addresses to connect to one MongoDb ReplicaSet or Sharded Cluster
   * @param credentials Credentials to connect
   * @param optionSSLOptions SSL options for secure connections
   * @param clientOptions All options for the client connections
   * @return Client connection with identifier
   */
  def getClient(hostPort: List[ServerAddress],
                credentials: List[MongoCredential] = List(),
                optionSSLOptions: Option[MongodbSSLOptions] = None,
                clientOptions: Map[String, Any] = Map()): ClientResponse = {
    val futureResult =
      mongoConnectionsActor ? GetClientWithMongoDbConfig(hostPort, credentials, optionSSLOptions, clientOptions)
    Await.result(futureResult, timeout.duration) match {
      case ClientResponse(key, clientConnection) => ClientResponse(key, clientConnection)
    }
  }


  /**
   * Close all client connections on the concurrent map
   * @param gracefully Close the connections if is free
   */
  def closeAll(gracefully: Boolean = true, attempts: Int = CloseAttempts): Unit = {
    mongoConnectionsActor ! CloseAll(gracefully, attempts)
  }

  /**
   * Close the connections that have the same client as the client param
   * @param client client value for connect to MongoDb
   * @param gracefully Close the connection if is free
   */
  def closeByClient(client: Client, gracefully: Boolean = true): Unit = {
    mongoConnectionsActor ! CloseByClient(client, gracefully)
  }

  /**
   * Close the connections that have the same key as the clientKey param
   * @param clientKey key pre calculated with the connection options
   * @param gracefully Close the connection if is free
   */
  def closeByKey(clientKey: String, gracefully: Boolean = true): Unit = {
    mongoConnectionsActor ! CloseByKey(clientKey, gracefully)
  }

  /**
   * Set Free the connection that have the same client as the client param
   * @param client client value for connect to MongoDb
   */
  def setFreeConnectionByClient(client: Client, extendedTime: Option[Long] = None): Unit = {
    mongoConnectionsActor ! SetFreeConnectionsByClient(client, extendedTime)
  }

  /**
   * Set Free the connection that have the same key as the clientKey param
   * @param clientKey key pre calculated with the connection options
   */
  def setFreeConnectionByKey(clientKey: String, extendedTime: Option[Long] = None): Unit = {
    mongoConnectionsActor ! SetFreeConnectionByKey(clientKey, extendedTime)
  }

  def getSize: Int = {
    val futureResult = mongoConnectionsActor ? GetSize
    Await.result(futureResult, timeout.duration) match {
      case size: Int => size
    }
  }

  // TODO Review when refactoring config
  def extractValue[T](options: Map[String, Any], key: String): Option[T] =
    options.get(key.toLowerCase).map(_.asInstanceOf[T])

  def sslBuilder(optionSSLOptions: Option[MongodbSSLOptions]): Boolean =
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
