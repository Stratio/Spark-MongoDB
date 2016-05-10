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
// TODO Review when refactoring config
object MongodbConfig {

  //  Parameter names
  val Host = "host"
  val Database = "database"
  val Collection = "collection"
  val SSLOptions = "sslOptions"
  val ReadPreference = "readPreference"
  val ConnectTimeout = "connectTimeout"
  val ConnectionsPerHost = "connectionsPerHost"
  val MaxWaitTime = "maxWaitTime"
  val SocketTimeout = "socketTimeout"
  val ThreadsAllowedToBlockForConnectionMultiplier = "threadsAllowedToBlockForConnectionMultiplier"
  val WriteConcern = "writeConcern"
  val Credentials = "credentials"
  val SamplingRatio = "schema_samplingRatio"
  val SplitSize = "splitSize"
  val SplitKey = "splitKey"
  val SplitKeyType = "splitKeyType"
  val SplitKeyMin = "splitKeyMin"
  val SplitKeyMax = "splitKeyMax"
  val UpdateFields = "updateFields"
  val Language = "language"
  val ConnectionsTime = "connectionsTime"
  val CursorBatchSize = "cursorBatchSize"
  val BulkBatchSize = "bulkBatchSize"
  val IdAsObjectId = "idAsObjectId"

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
  val DefaultCursorBatchSize = 101
  val DefaultBulkBatchSize = 1000
  val DefaultIdAsObjectId = true

  /**
   * Parse Map of string parameters to Map with the correct objects used in MongoDb Datasource functions
   * @param parameters List of parameters
   * @return List of parameters parsed to correct mongoDb configurations
   */
  // TODO Review when refactoring config
  def parseParameters(parameters : Map[String, Any]): Map[String, Any] = {

    // don't check required properties here, since it will be checked in the Config.build()

    val properties: Map[String, Any] = parameters

    //optional parseable properties
    val optionalProperties: List[String] = List(Host, Credentials, SSLOptions, UpdateFields)

    val optionalParsedProperties = (properties /: optionalProperties){
      /** We will assume hosts are provided like 'host:port,host2:port2,...' or like List('host1:port1','host2:port2',..
        * .) */
      case (properties, Host) =>
        parameters.get(Host).map{
          case hostInput: String => properties + (Host -> hostInput.split(",").toList)
          case hostInput @ List(_: String, _*)  => properties
          case _ => throw new IllegalArgumentException
        } getOrElse properties

      /** We will assume credentials are provided like 'user,database,password;user,database,password;...' or like
        * List('user,database,password', 'user,database,password', ...) */
      case (properties, Credentials) =>
        parameters.get(Credentials).map {
          case credentialInput: String =>
            val credentials = credentialInput.split(";").map(_.split(",")).toList
              .map(credentials => MongodbCredentials(credentials(0), credentials(1), credentials(2).toCharArray))
            properties + (Credentials -> credentials)
          case credentialInput: MongodbCredentials => properties + (Credentials -> List(credentialInput))
          case credentialInput @ List(_: String, _*) =>
            val credentials = credentialInput.map(_.toString.split(","))
              .map(credentials => MongodbCredentials(credentials(0), credentials(1), credentials(2).toCharArray))
            properties + (Credentials -> credentials)
          case credentialInput @ List(_: MongodbCredentials, _*) => properties
          case _ => throw new IllegalArgumentException
        } getOrElse  properties

      /** We will assume ssloptions are provided like '/path/keystorefile,keystorepassword,/path/truststorefile,truststorepassword' */
      case (properties, SSLOptions) =>
        parameters.get(SSLOptions).map {
          case ssloptionsInput: String =>
            val ssloption = ssloptionsInput.toString.split(",")
            val ssloptions = MongodbSSLOptions(Some(ssloption(0)), Some(ssloption(1)), ssloption(2), Some(ssloption(3)))
            properties + (SSLOptions.toLowerCase -> ssloptions) - SSLOptions
          case ssloptionsInput: MongodbSSLOptions => properties + (SSLOptions.toLowerCase -> ssloptionsInput) - SSLOptions
        } getOrElse {
          parameters.get(SSLOptions.toLowerCase).map {
            case ssloptionsInput: String =>
              val ssloption = ssloptionsInput.toString.split(",")
              val ssloptions = MongodbSSLOptions(Some(ssloption(0)), Some(ssloption(1)), ssloption(2), Some(ssloption(3)))
              properties + (SSLOptions.toLowerCase -> ssloptions)
            case ssloptionsInput: MongodbSSLOptions => properties
          } getOrElse properties
        }

      /** We will assume fields are provided like 'fieldName1,fieldName2,...' or like List('fieldName1','fieldName2',..
        * .)*/
      case (properties, UpdateFields) => {
        parameters.get(UpdateFields).map {
          case updateInputs: String =>
            val updateFields = updateInputs.split(",")
            properties + (UpdateFields.toLowerCase -> updateFields) - UpdateFields
          case updateFields @ Array(_: String, _*) => properties + (UpdateFields.toLowerCase -> updateFields) - UpdateFields
          case _ => throw new IllegalArgumentException
        } getOrElse {
          parameters.get(UpdateFields.toLowerCase).map {
            case updateInputs: String =>
              val updateFields = updateInputs.split(",")
              properties + (UpdateFields.toLowerCase -> updateFields)
            case updateFields @ Array(_: String, _*) => properties
            case _ => throw new IllegalArgumentException
          } getOrElse properties
        }
      }
    }

    val intProperties: List[String] = List(SplitSize, ConnectTimeout, ConnectionsPerHost, MaxWaitTime, SocketTimeout,
      ThreadsAllowedToBlockForConnectionMultiplier, CursorBatchSize, BulkBatchSize)

    val intParsedProperties = (optionalParsedProperties /: intProperties){
      case (properties, intProperty) => {
        parameters.get(intProperty).map{
          case intValueInput: String => properties + (intProperty.toLowerCase -> intValueInput.toInt) - intProperty
          case intValueInput: Int => properties + (intProperty.toLowerCase -> intValueInput) - intProperty
          case _ => throw new IllegalArgumentException
        } getOrElse {
          parameters.get(intProperty.toLowerCase).map {
            case intValueInput: String => properties + (intProperty.toLowerCase -> intValueInput.toInt)
            case intValueInput: Int => properties
            case _ => throw new IllegalArgumentException
          } getOrElse properties
        }
      }
    }

    val longProperties: List[String] = List(ConnectionsTime)

    val longParsedProperties = (intParsedProperties /: longProperties){
      case (properties, longProperty) => {
        parameters.get(longProperty).map {
          case longValueInput: String => properties + (longProperty.toLowerCase -> longValueInput.toLong) - longProperty
          case longValueInput: Long => properties + (longProperty.toLowerCase -> longValueInput) - longProperty
          case _ => throw new IllegalArgumentException
        } getOrElse {
          parameters.get(longProperty.toLowerCase).map {
            case longValueInput: String => properties + (longProperty.toLowerCase -> longValueInput.toLong)
            case longValueInput: Long => properties
            case _ => throw new IllegalArgumentException
          } getOrElse properties
        }
      }
    }

    val doubleProperties: List[String] = List(SamplingRatio)

    val doubleParsedProperties = (longParsedProperties /: doubleProperties){
      case (properties, doubleProperty) => {
        parameters.get(doubleProperty).map {
          case doubleValueInput: String => properties + (doubleProperty.toLowerCase -> doubleValueInput.toDouble) - doubleProperty
          case doubleValueInput: Double => properties + (doubleProperty.toLowerCase -> doubleValueInput) - doubleProperty
          case _ => throw new IllegalArgumentException
        } getOrElse {
          parameters.get(doubleProperty.toLowerCase).map {
            case doubleValueInput: String => properties + (doubleProperty.toLowerCase -> doubleValueInput.toDouble)
            case doubleValueInput: Double => properties
            case _ => throw new IllegalArgumentException
          } getOrElse properties
        }
      }
    }

    val booleanProperties: List[String] = List(IdAsObjectId)

    (doubleParsedProperties /: booleanProperties){
      case (properties, booleanProperty) => {
        parameters.get(booleanProperty).map {
          case booleanValueInput: String =>
            properties + (booleanProperty.toLowerCase -> booleanValueInput.toBoolean) - booleanProperty
          case booleanValueInput: Boolean => properties + (booleanProperty.toLowerCase -> booleanValueInput) - booleanProperty
          case _ => throw new IllegalArgumentException
        } getOrElse {
          parameters.get(booleanProperty.toLowerCase).map {
            case booleanValueInput: String => properties + (booleanProperty.toLowerCase -> booleanValueInput.toBoolean)
            case booleanValueInput: Boolean => properties
            case _ => throw new IllegalArgumentException
          } getOrElse properties
        }
      }
    }
  }

  /**
   * Parse one key to the associated readPreference
   * @param readPreference string key for identify the correct object
   * @return readPreference object
   */
  // TODO Review when refactoring config
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
  // TODO Review when refactoring config
  def parseWriteConcern(writeConcern: String): WriteConcern = {
    writeConcern.toUpperCase match {
      case "SAFE" | "ACKNOWLEDGED" => com.mongodb.WriteConcern.SAFE
      case "NORMAL" | "UNACKNOWLEDGED" => com.mongodb.WriteConcern.NORMAL
      case "REPLICAS_SAFE" | "REPLICA_ACKNOWLEDGED" => com.mongodb.WriteConcern.REPLICAS_SAFE
      case "FSYNC_SAFE" | "FSYNCED" => com.mongodb.WriteConcern.FSYNC_SAFE
      case "MAJORITY" => com.mongodb.WriteConcern.MAJORITY
      case "JOURNAL_SAFE" | "JOURNALED" => com.mongodb.WriteConcern.JOURNAL_SAFE
      case "NONE" | "ERRORS_IGNORED" => com.mongodb.WriteConcern.NONE
      case _ => DefaultWriteConcern
    }
  }
}
