package com.stratio.provider.mongodb

import com.mongodb.casbah.MongoClient
import com.mongodb.{MongoCredential, ServerAddress}
import org.scalatest.{Matchers, FlatSpec}

class MongodbClientFactorySpec extends FlatSpec with Matchers{

  type Client = MongoClient

  val hostClient = MongodbClientFactory.createClient("127.0.0.1")

  val hostPortCredentialsClient = MongodbClientFactory.createClient("127.0.0.1", 27017, "user", "database", "password")

  val listServerAddressListCredentialsClient = MongodbClientFactory.createClient(
    List(new ServerAddress("127.0.0.1:27017")),
    List(MongoCredential.createCredential("user","database","password".toCharArray))
  )

  val sslClient = MongodbClientFactory.createClient(
    List(new ServerAddress("127.0.0.1:27017")),
    List(MongoCredential.createCredential("user","database","password".toCharArray)),
    Some(MongodbSSLOptions(Some("/etc/ssl/mongodb.keystore"), Some("password"), "/etc/ssl/mongodb.keystore", Some("password")))
  )

  it should "Valid output type" in {

    hostClient shouldBe a [Client]
    hostPortCredentialsClient shouldBe a [Client]
    listServerAddressListCredentialsClient shouldBe a [Client]
    sslClient shouldBe a [Client]
  }
}
