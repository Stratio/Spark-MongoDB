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

    MongodbRelation(
      MongodbConfigBuilder()
        .apply(parseParameters(parameters))
        .build())(sqlContext)

  }
  override def createRelation(
   sqlContext: SQLContext,
   parameters: Map[String, String],
   schema: StructType): BaseRelation = {

    MongodbRelation(
    MongodbConfigBuilder()
      .apply(parseParameters(parameters))
      .build(), Some(schema))(sqlContext)

  }

  override def createRelation(
   sqlContext: SQLContext,
   mode: SaveMode,
   parameters: Map[String, String],
   data: DataFrame): BaseRelation = {

    val mongodbRelation = MongodbRelation(
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
    val host = parameters
      .getOrElse(Host, notFound[String](Host))
      .split(",").toList

    val database = parameters.getOrElse(Database, notFound(Database))

    val collection = parameters.getOrElse(Collection, notFound(Collection))

    val samplingRatio = parameters
      .get(SamplingRatio)
      .map(_.toDouble).getOrElse(DefaultSamplingRatio)

    val readpreference = parameters.getOrElse(readPreference, DefaultReadPreference)

    val properties :Map[String, Any] =
      Map(Host -> host, Database -> database, Collection -> collection , SamplingRatio -> samplingRatio, readPreference -> readpreference)

    val optionalProperties: List[String] = List(Credentials,SSLOptions, IdField, SearchFields, Language, Timeout)



    val finalMap = (properties /: optionalProperties){    //TODO improve code
      case (properties,Credentials) =>
        /** We will assume credentials are provided like 'user,database,password;user,database,password;...' */
        val credentialInput = parameters.getOrElse(Credentials, " ")
        if(credentialInput.compareTo(" ")!=0){
          val credentials= credentialInput
            .split(";")
            .map(credential => credential.split(",")).toList
            .map(credentials => MongodbCredentials(credentials(0), credentials(1), credentials(2).toCharArray))
          properties.+(Credentials -> credentials)
        } else properties
      case (properties,SSLOptions) =>
        /** We will assume ssloptions are provided like '/path/keystorefile,keystorepassword,/path/truststorefile,truststorepassword' */
        val ssloptionInput = parameters.getOrElse(SSLOptions, " ")
        if(ssloptionInput.compareTo(" ")!=0) {
          val ssloption = ssloptionInput.split(",")
          val ssloptions = MongodbSSLOptions(Some(ssloption(0)), Some(ssloption(1)), ssloption(2), Some(ssloption(3)))
          properties.+(SSLOptions -> ssloptions)
        }
        else properties
      case (properties, IdField) => {
        val idFieldInput = parameters.get(IdField)
        if(idFieldInput.isDefined) properties.+(IdField -> idFieldInput.get) else properties
      }
      case (properties, Language) => {
        val languageInput = parameters.get(Language)
        if(languageInput.isDefined) properties.+(Language -> languageInput.get) else properties
      }
      case (properties, SearchFields) => {
        /** We will assume fields are provided like 'user,database,password...' */
        val searchInputs = parameters.get(SearchFields)
        if(searchInputs.isDefined){
          val searchFields = searchInputs.get.split(",")
          properties.+(SearchFields -> searchFields)
        } else properties
      }
      case (properties, Timeout) => {
        /** Timeout in seconds */
        val timeout = parameters.get(Timeout)
        if(timeout.isDefined){
          properties.+(Timeout -> timeout)
        } else properties
      }

    }


    finalMap
  }

}
