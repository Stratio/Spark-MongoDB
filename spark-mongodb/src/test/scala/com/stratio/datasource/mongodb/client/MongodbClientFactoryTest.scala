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
import com.stratio.datasource.mongodb.config.MongodbSSLOptions
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FlatSpec, Matchers}

@RunWith(classOf[JUnitRunner])
class MongodbClientFactoryTest extends FlatSpec with Matchers with MongodbTestConstants with BeforeAndAfter {

  type Client = MongoClient

  val hostClient = MongodbClientFactory.getClient("127.0.0.1")._2

  val hostPortCredentialsClient = MongodbClientFactory.getClient("127.0.0.1", 27017, "user", "database", "password")._2

  val fullClient = MongodbClientFactory.getClient(
    List(new ServerAddress("127.0.0.1:27017")),
    List(MongoCredential.createCredential("user","database","password".toCharArray)),
    Some(MongodbSSLOptions(Some("/etc/ssl/mongodb.keystore"), Some("password"), "/etc/ssl/mongodb.keystore", Some("password"))),
      Map(
        "readPreference" -> "nearest",
        "connectTimeout"-> "50000",
        "socketTimeout"-> "50000",
        "maxWaitTime"-> "50000",
        "connectionsPerHost" -> "20",
        "threadsAllowedToBlockForConnectionMultiplier" -> "5"
      )
  )._2

  val gracefully = true

  val notGracefully = false


  behavior of "MongodbClientFactory"

  it should "Valid output type " + scalaBinaryVersion in {

    hostClient shouldBe a [Client]
    hostPortCredentialsClient shouldBe a [Client]
    fullClient shouldBe a [Client]
  }

  it should "Valid clients size when getting the same client " in {
    val sameHostClient = MongodbClientFactory.getClient("127.0.0.1")._2

    MongodbClientFactory.mongoClient.size should be (1)

    val otherHostClient = MongodbClientFactory.getClient("127.0.0.1")._2

    MongodbClientFactory.mongoClient.size should be (2)
  }

  it should "Valid clients size when getting the same client and set free " in {
    val sameHostClient = MongodbClientFactory.getClient("127.0.0.1")._2

    MongodbClientFactory.mongoClient.size should be (1)

    MongodbClientFactory.setFreeConnection(sameHostClient)

    val otherHostClient = MongodbClientFactory.getClient("127.0.0.1")._2

    MongodbClientFactory.mongoClient.size should be (1)
  }

  it should "Valid clients size when closing one client gracefully " in {
    val sameHostClient = MongodbClientFactory.getClient("127.0.0.1")._2

    MongodbClientFactory.mongoClient.size should be (1)

    MongodbClientFactory.close(sameHostClient)

    MongodbClientFactory.mongoClient.size should be (1)
  }

  it should "Valid clients size when closing one client not gracefully " in {
    val sameHostClient = MongodbClientFactory.getClient("127.0.0.1")._2

    MongodbClientFactory.mongoClient.size should be (1)

    MongodbClientFactory.close(sameHostClient, notGracefully)

    MongodbClientFactory.mongoClient.size should be (0)
  }

  it should "Valid clients size when closing all clients gracefully " in {
    val sameHostClient = MongodbClientFactory.getClient("127.0.0.1")._2
    val otherHostClient = MongodbClientFactory.getClient("127.0.0.1")._2

    MongodbClientFactory.mongoClient.size should be (2)

    MongodbClientFactory.closeAll(gracefully, 1)

    MongodbClientFactory.mongoClient.size should be (2)

    MongodbClientFactory.setFreeConnection(sameHostClient)

    MongodbClientFactory.closeAll(gracefully, 1)

    MongodbClientFactory.mongoClient.size should be (1)
  }

  it should "Valid clients size when closing all clients not gracefully " in {
    val sameHostClient = MongodbClientFactory.getClient("127.0.0.1")._2
    val otherHostClient = MongodbClientFactory.getClient("127.0.0.1")._2
    val gracefully = false

    MongodbClientFactory.mongoClient.size should be (2)

    MongodbClientFactory.closeAll(notGracefully)

    MongodbClientFactory.mongoClient.size should be (0)
  }


  after {
    MongodbClientFactory.closeAll(notGracefully)
  }

}
