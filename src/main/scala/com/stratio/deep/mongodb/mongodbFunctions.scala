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

package com.stratio.deep.mongodb

import com.stratio.deep.DeepConfig
import com.stratio.deep.mongodb.schema.MongodbRowConverter
import com.stratio.deep.mongodb.writer.{MongodbBatchWriter, MongodbSimpleWriter}
import org.apache.spark.sql.{SQLContext, SchemaRDD}

import scala.language.implicitConversions

/**
 * @param sqlContext Spark SQLContext
 */
class MongodbContext(sqlContext: SQLContext) {

  /**
   * It retrieves a bunch of MongoDB objects
   * given a MongDB configuration object.
   * @param config MongoDB configuration object
   * @return A schemaRDD
   */
  def fromMongoDB(config: DeepConfig): SchemaRDD =
    sqlContext.baseRelationToSchemaRDD(
      MongodbRelation(config, None)(sqlContext))

}

/**
 * @param schemaRDD Spark SchemaRDD
 */
class MongodbSchemaRDD(schemaRDD: SchemaRDD) extends Serializable {

  /**
   * It allows storing data in Mongodb from some existing SchemaRDD
   * @param config MongoDB configuration object
   * @param batch It indicates whether it has to be saved in batch mode or not.
   */
  def saveToMongodb(config: DeepConfig, batch: Boolean = true): Unit =
    schemaRDD.foreachPartition(it => {
      val writer =
        if (batch) new MongodbBatchWriter(config)
        else new MongodbSimpleWriter(config)
      writer.saveWithPk(it.map(row =>
        MongodbRowConverter.rowAsDBObject(row, schemaRDD.schema)))
      writer.close()
    })

}

/**
 *  Helpers for getting / storing MongoDB data.
 */
trait MongodbFunctions {

  implicit def toMongodbContext(sqlContext: SQLContext): MongodbContext =
    new MongodbContext(sqlContext)

  implicit def toMongodbSchemaRDD(schemaRDD: SchemaRDD): MongodbSchemaRDD =
    new MongodbSchemaRDD(schemaRDD)

}
