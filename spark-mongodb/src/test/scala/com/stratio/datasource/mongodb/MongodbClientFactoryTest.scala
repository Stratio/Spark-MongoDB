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

import com.mongodb.casbah.MongoClient
import com.mongodb.{MongoCredential, ServerAddress}
import com.stratio.datasource.MongodbTestConstants
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{Matchers, FlatSpec}

@RunWith(classOf[JUnitRunner])
class MongodbClientFactoryTest extends FlatSpec with Matchers with MongodbTestConstants{

  type Client = MongoClient

  val hostClient = MongodbClientFactory.createClient("127.0.0.1")

  val hostPortCredentialsClient = MongodbClientFactory.createClient("127.0.0.1", 27017, "user", "database", "password")

  val fullClient = MongodbClientFactory.createClient(
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
  )


  behavior of "MongodbClientFactory"

  it should "Valid output type " + scalaBinaryVersion in {

    hostClient shouldBe a [Client]
    hostPortCredentialsClient shouldBe a [Client]
    fullClient shouldBe a [Client]
  }
}
