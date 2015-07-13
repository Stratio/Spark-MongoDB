package com.stratio.provider.mongodb

import javax.net.ssl.SSLSocketFactory

import com.mongodb.ServerAddress
import com.mongodb.casbah.Imports._
import com.mongodb.casbah.{MongoClient, MongoClientOptions}

/**
 * Different client configurations to Mongodb database
 */
object MongodbClientFactory {

  type Client = MongoClient

  def createClient(host : String) : Client = MongoClient(host)

  def createClient(host : String, port : Int, user: String, database: String, password :String) : Client = {

    val credentials : List[MongoCredential]= List(MongoCredential.createCredential(user, database, password.toCharArray))
    val hostPort = new ServerAddress(host, port)
    MongoClient(hostPort, credentials)
  }

  def createClient(hostPort : List[ServerAddress], credentials : List[MongoCredential]) : Client = MongoClient(hostPort, credentials)

  def createClient(
    hostPort : List[ServerAddress],
    credentials : List[MongoCredential],
    optionSSLOptions: Option[MongodbSSLOptions]) : Client = {

    optionSSLOptions match{
      case Some(sslOptions) =>

        if(sslOptions.keyStore.nonEmpty) {
          System.setProperty("javax.net.ssl.keyStore", sslOptions.keyStore.get)
          if (sslOptions.keyStorePassword.nonEmpty)
            System.setProperty("javax.net.ssl.keyStorePassword", sslOptions.keyStorePassword.get)
        }
        if(sslOptions.trustStore.nonEmpty) {
          System.setProperty("javax.net.ssl.trustStore", sslOptions.trustStore)
          if (sslOptions.trustStorePassword.nonEmpty)
            System.setProperty("javax.net.ssl.trustStorePassword", sslOptions.trustStorePassword.get)
        }

        val options = new MongoClientOptions.Builder().socketFactory(SSLSocketFactory.getDefault()).build()
        MongoClient(hostPort, credentials, options)

      case _ => MongoClient(hostPort, credentials)
    }

  }


  def parseReadPreference(readPreference: Option[String]): com.mongodb.ReadPreference ={
    readPreference match{
      case Some("primary")            => com.mongodb.ReadPreference.primary()
      case Some("secondary")          => com.mongodb.ReadPreference.secondary()
      case Some("nearest")            => com.mongodb.ReadPreference.nearest()
      case Some("primaryPreferred")   => com.mongodb.ReadPreference.primaryPreferred()
      case Some("secondaryPreferred") => com.mongodb.ReadPreference.secondaryPreferred()
      case _                          => com.mongodb.ReadPreference.secondaryPreferred()

    }

  }

}
