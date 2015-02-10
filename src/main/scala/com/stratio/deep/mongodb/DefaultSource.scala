/*
 *
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
 * /
 */

package com.stratio.deep.mongodb

import com.stratio.deep.DeepConfig
import com.stratio.deep.DeepConfig._
import com.stratio.deep.mongodb.rdd.MongodbRDD
import com.stratio.deep.mongodb.schema.{MongodbRowConverter, MongodbSchema}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, RelationProvider, TableScan}
import org.apache.spark.sql.{Row, SQLContext, StructType}
import MongodbConfig._

/**
 * Created by rmorandeira on 29/01/15.
 */

class DefaultSource extends RelationProvider {

  override def createRelation(
    sqlContext: SQLContext,
    parameters: Map[String, String]): BaseRelation = {

    /** We will assume hosts are provided like 'host:port,host2:port2,...'*/
    val host = parameters.getOrElse(Host, notFound(Host)).split(",").toList

    val database = parameters.getOrElse(Database, notFound(Database))

    val collection = parameters.getOrElse(Collection, notFound(Collection))

    val samplingRatio = parameters
      .get(SamplingRatio)
      .map(_.toDouble).getOrElse(1.0)

    MongodbRelation(
      MongodbConfigBuilder()
        .set(Host,host)
        .set(Database,database)
        .set(Collection,collection)
        .set(SamplingRatio,samplingRatio).build())(sqlContext)

  }

}

case class MongodbRelation(
  config: DeepConfig,
  schemaProvided: Option[StructType]=None)(
  @transient val sqlContext: SQLContext) extends TableScan {

  private lazy val baseRDD =
    new MongodbRDD(sqlContext, config)

  override val schema: StructType = schemaProvided.getOrElse(lazySchema)

  @transient lazy val lazySchema = {
    MongodbSchema(baseRDD, config[Double](SamplingRatio)).schema()
  }

  override def buildScan(): RDD[Row] = {
    MongodbRowConverter.asRow(schema, baseRDD)
  }

}
