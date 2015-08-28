package com.stratio.provider.mongodb

import javax.net.ssl.SSLSocketFactory

import com.mongodb.ServerAddress
import com.mongodb.casbah.Imports._
import com.mongodb.casbah.{MongoClient, MongoClientOptions}
import org.joda.time.field.MillisDurationField

/**
 * Different client configurations to Mongodb database
 */
object MongodbClientFactory {

  type Client = MongoClient

  def createClient(host : String) : Client = MongoClient(host)

  def createClient(host:String, port:Int, user:String,database:String, password:String ):Client = {
    val credentials : List[MongoCredential]= List(MongoCredential.createCredential(user, database, password.toCharArray))
    val hostPort = new ServerAddress(host, port)

    createClient(List(hostPort), credentials)
  }

  def createClient( hostPort : List[ServerAddress],
                    credentials : List[MongoCredential] = List.empty,
                    optionSSLOptions: Option[MongodbSSLOptions] = None,
                    readPreference: String = "nearest",
                    options:Map[String, String] = Map.empty) : Client = {

    val optionsBuilder = new MongoClientOptions.Builder()

    if (sslBuilder(optionSSLOptions)) {
      optionsBuilder.socketFactory(SSLSocketFactory.getDefault())
    }

    optionsBuilder.readPreference(parseReadPreference(readPreference))

    if( options.get(MongodbConfig.ConnectTimeoutMillis).isDefined) {
      optionsBuilder.connectTimeout(options.get(MongodbConfig.ConnectTimeoutMillis).get.toInt)
    }

    if( options.get(MongodbConfig.SocketTimeoutMillis).isDefined) {
      optionsBuilder.socketTimeout(options.get(MongodbConfig.SocketTimeoutMillis).get.toInt)
    }

    if( options.get(MongodbConfig.ReplicaSet).isDefined){
      optionsBuilder.requiredReplicaSetName(options.get(MongodbConfig.ReplicaSet).get)
    }

    if( options.get(MongodbConfig.MinPoolSize).isDefined){
      optionsBuilder.minConnectionsPerHost(options.get(MongodbConfig.MinPoolSize).get.toInt)
    }

    if( options.get(MongodbConfig.MaxPoolSize).isDefined){
      optionsBuilder.connectionsPerHost(options.get(MongodbConfig.MaxPoolSize).get.toInt)
    }

    if( options.get(MongodbConfig.MaxIdleTimeMillis).isDefined){
      optionsBuilder.maxConnectionIdleTime(options.get(MongodbConfig.MaxIdleTimeMillis).get.toInt)
    }

    if( options.get(MongodbConfig.WaitQueueMultiple).isDefined){
      optionsBuilder.threadsAllowedToBlockForConnectionMultiplier(options.get(MongodbConfig.WaitQueueMultiple).get.toInt)
    }

    if( options.get(MongodbConfig.AutoConnectRetry).isDefined){
      optionsBuilder.autoConnectRetry(options.get(MongodbConfig.AutoConnectRetry).get.toBoolean)
    }

    MongoClient(hostPort, credentials, optionsBuilder.build())
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
