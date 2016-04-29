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

package com.stratio.datasource.mongodb.config

import com.mongodb.casbah.Imports._
import com.mongodb.{MongoClientOptions => JavaMongoClientOptions}
import com.stratio.datasource.util.Config._

/**
 * Values and Functions for access and parse the configuration parameters
 */
object MongodbConfig {

  //  Parameter names
  val Host = "host"
  val Database = "database"
  val Collection = "collection"
  val SSLOptions = "ssloptions"
  val ReadPreference = "readpreference"
  val ConnectTimeout = "connecttimeout"
  val ConnectionsPerHost = "connectionsperhost"
  val MaxWaitTime = "maxwaittime"
  val SocketTimeout = "sockettimeout"
  val ThreadsAllowedToBlockForConnectionMultiplier = "threadsallowedtoblockforconnectionmultiplier"
  val WriteConcern = "writeconcern"
  val Credentials = "credentials"
  val SamplingRatio = "schema_samplingratio"
  val SplitSize = "splitsize"
  val SplitKey = "splitkey"
  val SplitKeyType = "splitkeytype"
  val SplitKeyMin = "splitkeymin"
  val SplitKeyMax = "splitkeymax"
  val UpdateFields = "updatefields"
  val Language = "language"
  val ConnectionsTime = "connectionstime"
  val CursorBatchSize = "cursorbatchsize"
  val BulkBatchSize = "bulkbatchsize"
  val IdAsObjectId = "idasobjectid"

  // List of parameters for mongoClientOptions
  val ListMongoClientOptions = List(
    ReadPreference,
    ConnectionsPerHost,
    ConnectTimeout,
    MaxWaitTime,
    ThreadsAllowedToBlockForConnectionMultiplier,
    ConnectionsTime
  )

  // Mandatory
  val required = List(
    Host,
    Database,
    Collection
  )

  //  Default MongoDB values
  val DefaultMongoClientOptions = new JavaMongoClientOptions.Builder().build()
  val DefaultReadPreference = com.mongodb.casbah.ReadPreference.Nearest
  val DefaultConnectTimeout = DefaultMongoClientOptions.getConnectTimeout
  val DefaultConnectionsPerHost = DefaultMongoClientOptions.getConnectionsPerHost
  val DefaultMaxWaitTime = DefaultMongoClientOptions.getMaxWaitTime
  val DefaultSocketTimeout = DefaultMongoClientOptions.getSocketTimeout
  val DefaultThreadsAllowedToBlockForConnectionMultiplier= DefaultMongoClientOptions.getThreadsAllowedToBlockForConnectionMultiplier
  val DefaultCredentials = List[MongodbCredentials]()
  val DefaultWriteConcern = com.mongodb.WriteConcern.ACKNOWLEDGED

  // Default datasource specific values
  val DefaultSamplingRatio = 1.0
  val DefaultSplitSize = 10
  val DefaultSplitKey = "_id"
  val DefaultConnectionsTime = 120000L
  val DefaultCursorBatchSize = "101"
  val DefaultBulkBatchSize = "1000"
  val DefaultIdAsObjectId = "true"

  /**
   * Parse Map of string parameters to Map with the correct objects used in MongoDb Datasource functions
   * @param parameters List of parameters
   * @return List of parameters parsed to correct mongoDb configurations
   */
  def parseParameters(parameters : Map[String,String]): Map[String, Any] = {

    // required properties
    /** We will assume hosts are provided like 'host:port,host2:port2,...' */
    val properties: Map[String, Any] = parameters.updated(Host, parameters.getOrElse(Host, notFound[String](Host)).split(",").toList)
    if (!parameters.contains(Database)) notFound(Database)
    if (!parameters.contains(Collection)) notFound(Collection)

    //optional parseable properties
    val optionalProperties: List[String] = List(Credentials,SSLOptions, UpdateFields)

    (properties /: optionalProperties){
      /** We will assume credentials are provided like 'user,database,password;...;user,database,password' */
      case (properties,Credentials) =>
        parameters.get(Credentials).map{ credentialInput =>
          val credentials = credentialInput.split(";").map(_.split(",")).toList
            .map(credentials => MongodbCredentials(credentials(0), credentials(1), credentials(2).toCharArray))
          properties + (Credentials -> credentials)
        } getOrElse properties

      /** We will assume ssloptions are provided like '/path/keystorefile,keystorepassword,/path/truststorefile,truststorepassword' */
      case (properties,SSLOptions) =>
        parameters.get(SSLOptions).map{ ssloptionsInput =>

          val ssloption = ssloptionsInput.split(",")
          val ssloptions = MongodbSSLOptions(Some(ssloption(0)), Some(ssloption(1)), ssloption(2), Some(ssloption(3)))
          properties + (SSLOptions -> ssloptions)
        } getOrElse properties

      /** We will assume fields are provided like 'field1,field2,...,fieldN' */
      case (properties, UpdateFields) => {
        parameters.get(UpdateFields).map{ updateInputs =>
          val updateFields = updateInputs.split(",")
          properties + (UpdateFields -> updateFields)
        } getOrElse properties
      }
    }
  }

  /**
   * Parse one key to the associated readPreference
   * @param readPreference string key for identify the correct object
   * @return readPreference object
   */
  def parseReadPreference(readPreference: String): ReadPreference = {
    readPreference.toUpperCase match {
      case "PRIMARY" => com.mongodb.casbah.ReadPreference.Primary
      case "SECONDARY" => com.mongodb.casbah.ReadPreference.Secondary
      case "NEAREST" => com.mongodb.casbah.ReadPreference.Nearest
      case "PRIMARYPREFERRED" => com.mongodb.casbah.ReadPreference.primaryPreferred
      case "SECONDARYPREFERRED" => com.mongodb.casbah.ReadPreference.SecondaryPreferred
      case _ => com.mongodb.casbah.ReadPreference.Nearest
    }
  }

  /**
   * Parse one key to the associated writeConcern
   * @param writeConcern string key for identify the correct object
   * @return writeConcern object
   */
  def parseWriteConcern(writeConcern: String): WriteConcern = {
    writeConcern.toUpperCase match {
      case "FSYNC_SAFE" => com.mongodb.WriteConcern.FSYNC_SAFE
      case "FSYNCED" => com.mongodb.WriteConcern.FSYNCED
      case "JOURNAL_SAFE" => com.mongodb.WriteConcern.JOURNAL_SAFE
      case "JOURNALED" => com.mongodb.WriteConcern.JOURNALED
      case "MAJORITY" => com.mongodb.WriteConcern.MAJORITY
      case "NORMAL" => com.mongodb.WriteConcern.NORMAL
      case "REPLICA_ACKNOWLEDGED" => com.mongodb.WriteConcern.REPLICA_ACKNOWLEDGED
      case "REPLICAS_SAFE" => com.mongodb.WriteConcern.REPLICAS_SAFE
      case "SAFE" => com.mongodb.WriteConcern.SAFE
      case "UNACKNOWLEDGED" => com.mongodb.WriteConcern.UNACKNOWLEDGED
      case _ => DefaultWriteConcern
    }
  }
}
