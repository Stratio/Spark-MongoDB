/*
 *  Licensed to STRATIO (C) under one or more contributor license agreements.
 *  See the NOTICE file distributed with this work for additional information
 *  regarding copyright ownership. The STRATIO (C) licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied. See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package com.stratio.provider.mongodb

import com.stratio.provider.Config._
import com.stratio.provider.mongodb.MongodbConfig._
import org.apache.spark.sql.SaveMode._
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, RelationProvider, SchemaRelationProvider}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

/**
 * Allows creation of MongoDB based tables using
 * the syntax CREATE TEMPORARY TABLE ... USING com.stratio.deep.mongodb.
 * Required options are detailed in [[com.stratio.provider.mongodb.MongodbConfig]]
 */
class DefaultSource extends RelationProvider with SchemaRelationProvider with CreatableRelationProvider{

  override def createRelation(
   sqlContext: SQLContext,
   parameters: Map[String, String]): BaseRelation = {

    new MongodbRelation(
      MongodbConfigBuilder()
        .apply(parseParameters(parameters))
        .build())(sqlContext)

  }

  override def createRelation(
   sqlContext: SQLContext,
   parameters: Map[String, String],
   schema: StructType): BaseRelation = {

    new MongodbRelation(
    MongodbConfigBuilder()
      .apply(parseParameters(parameters))
      .build(),Some(schema))(sqlContext)

  }

  override def createRelation(
   sqlContext: SQLContext,
   mode: SaveMode,
   parameters: Map[String, String],
   data: DataFrame): BaseRelation = {

    val mongodbRelation = new MongodbRelation(
      MongodbConfigBuilder()
        .apply(parseParameters(parameters))
        .build())(sqlContext)

    mode match{
      case Append         => mongodbRelation.insert(data, overwrite = false)
      case Overwrite      => mongodbRelation.insert(data, overwrite = true)
      case ErrorIfExists  => if(mongodbRelation.isEmptyCollection) mongodbRelation.insert(data, overwrite = false)
      else throw new UnsupportedOperationException("Writing in a non-empty collection.")
      case Ignore         => if(mongodbRelation.isEmptyCollection) mongodbRelation.insert(data, overwrite = false)
    }

    mongodbRelation
  }

  private def parseParameters(parameters : Map[String,String]): Map[String, Any] = {

    /** We will assume hosts are provided like 'host:port,host2:port2,...' */
    val properties: Map[String, Any] = parameters.updated(Host, parameters.getOrElse(Host, notFound[String](Host)).split(",").toList)
    if (!parameters.contains(Database)) notFound(Database)
    if (!parameters.contains(Collection)) notFound(Collection)

    val optionalProperties: List[String] = List(Credentials,SSLOptions, UpdateFields)

    val finalMap = (properties /: optionalProperties){
      /** We will assume credentials are provided like 'user,database,password;user,database,password;...' */
      case (properties,Credentials) =>
        parameters.get(Credentials).map{ credentialInput =>
          val credentials = credentialInput.split(";")
            .map(credential => credential.split(",")).toList
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

      /** We will assume fields are provided like 'user,database,password...' */
      case (properties, UpdateFields) => {
        parameters.get(UpdateFields).map{ updateInputs =>
          val updateFields = updateInputs.split(",")
          properties + (UpdateFields -> updateFields)
        } getOrElse properties
      }
    }

    finalMap
  }

}