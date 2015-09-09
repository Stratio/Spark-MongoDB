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

  def createClient(hostPort : List[ServerAddress], credentials : List[MongoCredential], readPreference: String) : Client = {
    val options = new MongoClientOptions.Builder().readPreference(parseReadPreference(readPreference)).build()
    MongoClient(hostPort, credentials, options)
  }

  def createClient(
                    hostPort : List[ServerAddress],
                    credentials : List[MongoCredential],
                    optionSSLOptions: Option[MongodbSSLOptions]) : Client = {

    if (sslBuilder(optionSSLOptions)) {

      val options = new MongoClientOptions.Builder().socketFactory(SSLSocketFactory.getDefault()).build()
      MongoClient(hostPort, credentials, options)
    }
    else
      MongoClient(hostPort, credentials)

  }

  def createClient(
    hostPort : List[ServerAddress],
    credentials : List[MongoCredential],
    optionSSLOptions: Option[MongodbSSLOptions],
    readPreference: String) : Client = {

    if (sslBuilder(optionSSLOptions)) {
      val options = new MongoClientOptions.Builder()
        .readPreference(parseReadPreference(readPreference))
        .socketFactory(SSLSocketFactory.getDefault()).build()

      MongoClient(hostPort, credentials, options)
    }
    else {
      val options = new MongoClientOptions.Builder().readPreference(parseReadPreference(readPreference)).build()

      MongoClient(hostPort, credentials, options)
    }

  }


  def createClient(
                    hostPort : List[ServerAddress],
                    credentials : List[MongoCredential],
                    optionSSLOptions: Option[MongodbSSLOptions],
                    readPreference: String,
                    timeout: Option[String]) : Client = {



    if (sslBuilder(optionSSLOptions)) {
      val options = new MongoClientOptions.Builder()
        .readPreference(parseReadPreference(readPreference))
        .socketFactory(SSLSocketFactory.getDefault()).connectTimeout(timeout.getOrElse("10").toInt).build()

      MongoClient(hostPort, credentials, options)
    }
    else {
      val options = new MongoClientOptions.Builder().readPreference(parseReadPreference(readPreference)).connectTimeout(timeout.getOrElse("10").toInt).build()

      MongoClient(hostPort, credentials, options)
    }

  }

  private def sslBuilder(optionSSLOptions: Option[MongodbSSLOptions]): Boolean = {

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

        true

      case _ => false
    }

  }

  private def parseReadPreference(readPreference: String): ReadPreference ={
    readPreference match{
      case "primary"             => ReadPreference.Primary
      case "secondary"           => ReadPreference.Secondary
      case "nearest"             => ReadPreference.Nearest
      case "primaryPreferred"    => ReadPreference.primaryPreferred
      case "secondaryPreferred"  => ReadPreference.SecondaryPreferred
      case _                     => ReadPreference.SecondaryPreferred
    }
  }

}
