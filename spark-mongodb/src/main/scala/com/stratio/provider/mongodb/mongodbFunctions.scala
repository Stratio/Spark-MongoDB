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


import com.stratio.provider.Config
import com.stratio.provider.mongodb.schema.MongodbRowConverter
import com.stratio.provider.mongodb.writer.{MongodbBatchWriter, MongodbSimpleWriter}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{SQLContext, DataFrame}

import scala.language.implicitConversions

/**
 * @param sqlContext Spark SQLContext
 */
class MongodbContext(sqlContext: SQLContext) {

  /**
   * It retrieves a bunch of MongoDB objects
   * given a MongDB configuration object.
   * @param config MongoDB configuration object
   * @return A dataFrame
   */
  def fromMongoDB(config: Config,schema:Option[StructType]=None): DataFrame =
    sqlContext.baseRelationToDataFrame(
      MongodbRelation(config, schema)(sqlContext))

}

/**
 * @param dataFrame Spark SchemaRDD
 */
class MongodbDataFrame(dataFrame: DataFrame) extends Serializable {

  /**
   * It allows storing data in Mongodb from some existing SchemaRDD
   * @param config MongoDB configuration object
   * @param batch It indicates whether it has to be saved in batch mode or not.
   * @deprecated
   */
  def saveToMongodb(config: Config, batch: Boolean = true): Unit = {
    val schema = dataFrame.schema
    dataFrame.foreachPartition(it => {
      val writer =
        if (batch) new MongodbBatchWriter(config)
        else new MongodbSimpleWriter(config)
      writer.saveWithPk(it.map(row =>
        MongodbRowConverter.rowAsDBObject(row, schema)))
      writer.close()
    })
  }

}

/**
 *  Helpers for getting / storing MongoDB data.
 */
trait MongodbFunctions {

  implicit def toMongodbContext(sqlContext: SQLContext): MongodbContext =
    new MongodbContext(sqlContext)

  implicit def toMongodbSchemaRDD(dataFrame: DataFrame): MongodbDataFrame =
    new MongodbDataFrame(dataFrame)

}
