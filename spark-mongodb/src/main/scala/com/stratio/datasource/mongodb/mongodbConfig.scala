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
package com.stratio.datasource.mongodb

import com.mongodb.{MongoClientOptions => JavaMongoClientOptions}
import com.stratio.datasource.Config._
import com.stratio.datasource.ConfigBuilder

/**
 * Created by jsantos on 5/02/15.
 *
 * A specialized Mongo configuration builder.
 * It focuses on mongodb config parameters
 * such as host,database,collection, samplingRatio (for schema infer)
 * @param props Initial properties map
 */

case class MongodbConfigBuilder(
                                 props: Map[Property, Any] = Map()
                                 ) extends {

  override val properties = Map() ++ props
} with ConfigBuilder[MongodbConfigBuilder](properties) {

  val requiredProperties: List[Property] = MongodbConfig.required

  def apply(props: Map[Property, Any]) =
    MongodbConfigBuilder(props)
}

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
  val IdField = "_idField"
  val UpdateFields = "updateFields"
  val Language = "language"

  // List of parameters for mongoClientOptions
  val ListMongoClientOptions = List(
    ReadPreference,
    ConnectionsPerHost,
    ConnectTimeout,
    MaxWaitTime,
    ThreadsAllowedToBlockForConnectionMultiplier
  )

  // Mandatory
  val required = List(
    Host,
    Database,
    Collection
  )

  //  Default MongoDB values
  val DefaultMongoClientOptions = new JavaMongoClientOptions.Builder().build()
  val DefaultReadPreference = "nearest"
  val DefaultConnectTimeout = DefaultMongoClientOptions.getConnectTimeout
  val DefaultConnectionsPerHost = DefaultMongoClientOptions.getConnectionsPerHost
  val DefaultMaxWaitTime = DefaultMongoClientOptions.getMaxWaitTime
  val DefaultSocketTimeout = DefaultMongoClientOptions.getSocketTimeout
  val DefaultThreadsAllowedToBlockForConnectionMultiplier= DefaultMongoClientOptions.getThreadsAllowedToBlockForConnectionMultiplier
  val DefaultWriteConcern = DefaultMongoClientOptions.getWriteConcern
  val DefaultCredentials = List[MongodbCredentials]()

  // Default datasource specific values
  val DefaultSamplingRatio = 1.0
  val DefaultSplitSize = 10
  val DefaultSplitKey = "_id"

}
