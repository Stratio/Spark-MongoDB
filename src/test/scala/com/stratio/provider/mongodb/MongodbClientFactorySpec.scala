package com.stratio.provider.mongodb

import com.mongodb.casbah.MongoClient
import com.mongodb.{MongoCredential, ServerAddress}
import org.scalatest.{Matchers, FlatSpec}

class MongodbClientFactorySpec extends FlatSpec with Matchers{

  type Client = MongoClient

  val hostClient = MongodbClientFactory.createClient("127.0.0.1")
  //println("warning here")//TODO delete this print
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

  it should "properly extract the parameters of different connections" in {

    hostClient.getAddress.getHost should be("127.0.0.1")

    hostPortCredentialsClient.getAddress.getHost should be("127.0.0.1")
    hostPortCredentialsClient.getAddress.getPort should be(27017)
    hostPortCredentialsClient.credentialsList.head.getUserName should be("user")
    hostPortCredentialsClient.credentialsList.head.getSource should be("database")
    hostPortCredentialsClient.credentialsList.head.getPassword should be("password".toCharArray)

    listServerAddressListCredentialsClient.getAddress.getHost should be("127.0.0.1")
    listServerAddressListCredentialsClient.getAddress.getPort should be(27017)
    listServerAddressListCredentialsClient.credentialsList.head.getUserName should be("user")
    listServerAddressListCredentialsClient.credentialsList.head.getSource should be("database")
    listServerAddressListCredentialsClient.credentialsList.head.getPassword should be("password".toCharArray)

//    //  println("Error here")//TODO delete this print
//    sslClient.getAddress.getHost should be ("127.0.0.1")
//    sslClient.getAddress.getPort should be  (27017)
//    sslClient.credentialsList.head.getUserName should be ("user")
//    sslClient.credentialsList.head.getSource should be ("database")
//    sslClient.credentialsList.head.getPassword should be ("password".toCharArray)
  }

}
