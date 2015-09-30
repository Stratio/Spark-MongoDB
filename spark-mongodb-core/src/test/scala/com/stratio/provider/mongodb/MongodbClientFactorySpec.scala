package com.stratio.provider.mongodb

import com.mongodb.casbah.MongoClient
import com.mongodb.{MongoCredential, ServerAddress}
import org.scalatest.{Matchers, FlatSpec}

class MongodbClientFactorySpec extends FlatSpec with Matchers{

  type Client = MongoClient

  val hostClient = MongodbClientFactory.createClient("127.0.0.1")

  val hostPortCredentialsClient = MongodbClientFactory.createClient("127.0.0.1", 27017, "user", "database", "password")

  val fullClient = MongodbClientFactory.createClient(
    List(new ServerAddress("127.0.0.1:27017")),
    Map(
      "readPreference" -> "nearest",
      "connectTimeout"-> "50000",
      "socketTimeout"-> "50000",
      "maxWaitTime"-> "50000",
      "connectionsPerHost" -> "20",
      "threadsAllowedToBlockForConnectionMultiplier" -> "5"
    ),
    List(MongoCredential.createCredential("user","database","password".toCharArray)),
    Some(MongodbSSLOptions(Some("/etc/ssl/mongodb.keystore"), Some("password"), "/etc/ssl/mongodb.keystore", Some("password")))
  )

  it should "Valid output type" in {

    hostClient shouldBe a [Client]
    hostPortCredentialsClient shouldBe a [Client]
    fullClient shouldBe a [Client]
  }
}
