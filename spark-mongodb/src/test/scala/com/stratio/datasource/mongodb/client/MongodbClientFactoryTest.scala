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

import com.mongodb.casbah.MongoClient
import com.mongodb.{MongoCredential, ServerAddress}
import com.stratio.datasource.MongodbTestConstants
import com.stratio.datasource.mongodb.config.{MongodbConfig, MongodbConfigBuilder, MongodbCredentials, MongodbSSLOptions}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FlatSpec, Matchers}

@RunWith(classOf[JUnitRunner])
class MongodbClientFactoryTest extends FlatSpec
with Matchers
with MongodbTestConstants
with BeforeAndAfter
with BeforeAndAfterAll {

  type Client = MongoClient

  val hostClient = MongodbClientFactory.getClient("127.0.0.1").clientConnection

  val hostPortCredentialsClient = MongodbClientFactory.getClient("127.0.0.1", 27017, "user", "database", "password").clientConnection

  val config = MongodbConfigBuilder(Map(
    "readPreference" -> "NEAREST",
    "connectTimeout"-> "50000",
    "socketTimeout"-> "50000",
    "maxWaitTime"-> "50000",
    "connectionsPerHost" -> "20",
    "threadsAllowedToBlockForConnectionMultiplier" -> "5"
  )).set("host", "127.0.0.1:27017")
    .set("database", "database")
    .set("collection", "collection")
    .set(MongodbConfig.Credentials, MongodbCredentials("user","database","password".toCharArray))
    .set(MongodbConfig.SSLOptions, MongodbSSLOptions(Some("/etc/ssl/mongodb.keystore"), Some("password"), "/etc/ssl/mongodb.keystore", Some("password")))
    .build()
  
  val fullClient = MongodbClientFactory.getClient(
    config[List[String]](MongodbConfig.Host).map(add => new ServerAddress(add)),
    config[List[MongodbCredentials]](MongodbConfig.Credentials).map {
      case MongodbCredentials(user, database, password) =>
        MongoCredential.createCredential(user, database, password)
    },
    config.get[MongodbSSLOptions](MongodbConfig.SSLOptions),
    config.properties
  ).clientConnection

  val gracefully = true

  val notGracefully = false


  behavior of "MongodbClientFactory"

  it should "Valid output type " + scalaBinaryVersion in {

    hostClient shouldBe a [Client]
    hostPortCredentialsClient shouldBe a [Client]
    fullClient shouldBe a [Client]

    MongodbClientFactory.closeAll(notGracefully)
  }

  it should "Valid clients size when getting the same client " in {
    val sameHostClient = MongodbClientFactory.getClient("127.0.0.1").clientConnection

    MongodbClientFactory.getClientPoolSize should be (1)

    val otherHostClient = MongodbClientFactory.getClient("127.0.0.1").clientConnection

    MongodbClientFactory.getClientPoolSize should be (2)

    MongodbClientFactory.closeAll(notGracefully)
  }

  it should "Valid clients size when getting the same client and set free " in {
    val sameHostClient = MongodbClientFactory.getClient("127.0.0.1").clientConnection

    MongodbClientFactory.getClientPoolSize should be (1)

    MongodbClientFactory.setFreeConnectionByClient(sameHostClient)

    val otherHostClient = MongodbClientFactory.getClient("127.0.0.1").clientConnection

    MongodbClientFactory.getClientPoolSize should be (1)

    MongodbClientFactory.closeAll(notGracefully)
  }

  it should "Valid clients size when closing one client gracefully " in {
    val sameHostClient = MongodbClientFactory.getClient("127.0.0.1").clientConnection

    MongodbClientFactory.getClientPoolSize should be (1)

    MongodbClientFactory.closeByClient(sameHostClient)

    MongodbClientFactory.getClientPoolSize should be (1)

    MongodbClientFactory.closeAll(notGracefully)
  }

  it should "Valid clients size when closing one client not gracefully " in {
    val sameHostClient = MongodbClientFactory.getClient("127.0.0.1").clientConnection

    MongodbClientFactory.getClientPoolSize should be (1)

    MongodbClientFactory.closeByClient(sameHostClient, notGracefully)

    MongodbClientFactory.getClientPoolSize should be (0)

    MongodbClientFactory.closeAll(notGracefully)
  }

  it should "Valid clients size when closing all clients gracefully " in {
    val sameHostClient = MongodbClientFactory.getClient("127.0.0.1").clientConnection
    val otherHostClient = MongodbClientFactory.getClient("127.0.0.1").clientConnection

    MongodbClientFactory.getClientPoolSize should be (2)

    MongodbClientFactory.closeAll(gracefully, 1)

    MongodbClientFactory.getClientPoolSize should be (2)

    MongodbClientFactory.setFreeConnectionByClient(sameHostClient)

    MongodbClientFactory.closeAll(gracefully, 1)

    MongodbClientFactory.getClientPoolSize should be (1)

    MongodbClientFactory.closeAll(notGracefully)
  }

  it should "Valid clients size when closing all clients not gracefully " in {
    val sameHostClient = MongodbClientFactory.getClient("127.0.0.1").clientConnection
    val otherHostClient = MongodbClientFactory.getClient("127.0.0.1").clientConnection
    val gracefully = false

    MongodbClientFactory.getClientPoolSize should be (2)

    MongodbClientFactory.closeAll(notGracefully)

    MongodbClientFactory.getClientPoolSize should be (0)

    MongodbClientFactory.closeAll(notGracefully)
  }
}
