package com.stratio.provider.mongodb

import javax.net.ssl.SSLSocketFactory
import com.stratio.provider.mongodb.MongodbConfig._
import com.stratio.provider.mongodb.MongodbConfig.{ReadPreference => ProviderReadPreference}
import com.mongodb.ServerAddress
import com.mongodb.casbah.Imports._
import com.mongodb.casbah.{ReadPreference, MongoClient, MongoClientOptions}

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

  def createClient(
                    hostPort : List[ServerAddress],
                    clientOptions: Map[String, Any],
                    credentials : List[MongoCredential] = List(),
                    optionSSLOptions: Option[MongodbSSLOptions] = None
                    ) : Client = {

    val options = {
      val builder = new MongoClientOptions.Builder()
        .readPreference(parseReadPreference(clientOptions.getOrElse(ProviderReadPreference, DefaultReadPreference).asInstanceOf[String]))
        .connectTimeout(clientOptions.getOrElse(ConnectTimeout, DefaultConnectTimeout).asInstanceOf[String].toInt)
        .socketFactory(SSLSocketFactory.getDefault())
        .connectionsPerHost(clientOptions.getOrElse(ConnectionsPerHost, DefaultConnectionsPerHost).asInstanceOf[String].toInt)
        .maxWaitTime(clientOptions.getOrElse(MaxWaitTime, DefaultMaxWaitTime).asInstanceOf[String].toInt)
        .threadsAllowedToBlockForConnectionMultiplier(clientOptions.getOrElse(ThreadsAllowedToBlockForConnectionMultiplier, DefaultThreadsAllowedToBlockForConnectionMultiplier).asInstanceOf[String].toInt)

      if (sslBuilder(optionSSLOptions)) builder.socketFactory(SSLSocketFactory.getDefault())

      builder.build()
    }

    MongoClient(hostPort, credentials, options)

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
