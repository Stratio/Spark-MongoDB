package com.stratio.deep.mongodb

import javax.net.ssl.SSLSocketFactory

import com.mongodb.ServerAddress
import com.mongodb.casbah.Imports._
import com.mongodb.casbah.{MongoClient, MongoClientOptions}

/**
 * Created by pmadrigal on 18/05/15.
 */
object MongoClientFactory {

  type Client = MongoClient

  def createClient(host : String, port : Int) : Client = MongoClient(host, port)

  def createClient(host : String, port : Int, user: String, database: String, password :String) : Client = {

    val credentials : List[MongoCredential]= List(MongoCredential.createCredential(user, database, password.toCharArray))
    val hostPort = new ServerAddress(host, port)
    MongoClient(hostPort, credentials)
  }

  def createClient(host : String, port : Int, keyStore: Option[String], keyStorePassword: Option[String], trustStore: Option[String], trustStorePassword: Option[String] ): Client={

    //sslOptions.map(option=> System.setProperty("javax.net.ssl."+option._1, option._2))

    if(keyStore.nonEmpty) {
      System.setProperty("javax.net.ssl.keyStore", keyStore.get)
      if (keyStorePassword.nonEmpty)
        System.setProperty("javax.net.ssl.keyStorePassword", keyStorePassword.get)
    }
    if(trustStore.nonEmpty) {
      System.setProperty("javax.net.ssl.trustStore", trustStore.get)
      if (trustStorePassword.nonEmpty)
        System.setProperty("javax.net.ssl.trustStorePassword", trustStorePassword.get)
    }
    val hostPort = new ServerAddress(host, port)
    val options = new MongoClientOptions.Builder().socketFactory(SSLSocketFactory.getDefault()).build()
    MongoClient(hostPort, options)
  }

  //def closeClient(client : Client) = client.close()


  def createClient(host : String, port : Int, user: String, database: String, password :String, keyStore: Option[String], keyStorePassword: Option[String], trustStore: Option[String], trustStorePassword: Option[String] ): Client={

    val credentials : List[MongoCredential]= List(MongoCredential.createCredential(user, database, password.toCharArray))
    val hostPort = new ServerAddress(host, port)

    //sslOptions.map(option=> System.setProperty("javax.net.ssl."+option._1, option._2))

    if(keyStore.nonEmpty) {
      System.setProperty("javax.net.ssl.keyStore", keyStore.get)
      if (keyStorePassword.nonEmpty)
        System.setProperty("javax.net.ssl.keyStorePassword", keyStorePassword.get)
    }
    if(trustStore.nonEmpty) {
      System.setProperty("javax.net.ssl.trustStore", trustStore.get)
      if (trustStorePassword.nonEmpty)
        System.setProperty("javax.net.ssl.trustStorePassword", trustStorePassword.get)
    }
   
    val options = new MongoClientOptions.Builder().socketFactory(SSLSocketFactory.getDefault()).build()
    MongoClient(hostPort, credentials ,options)
  }


}
