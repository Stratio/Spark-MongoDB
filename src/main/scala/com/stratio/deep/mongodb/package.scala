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

package com.stratio.deep

import com.stratio.deep.mongodb.schema.MongodbRowConverter
import com.stratio.deep.mongodb.writer.{MongodbSimpleWriter, MongodbBatchWriter}
import org.apache.spark.sql.{SQLContext, SchemaRDD}

/**
 * Created by rmorandeira on 28/01/15.
 */
package object mongodb {

  /**
   * Adds a method, fromMongodb, to SQLContext that allows reading data stored in Mongodb.
   */
  implicit class MongodbContext(sqlContext: SQLContext) {
    def fromMongoDB(config: DeepConfig): SchemaRDD = {
      sqlContext.baseRelationToSchemaRDD(MongodbRelation(config, None)(sqlContext))
    }

  }

  /**
   * Adds a method, fromMongodb, to schemaRDD that allows storing data in Mongodb.
   */
  implicit class MongodbSchemaRDD(schemaRDD: SchemaRDD) extends Serializable {

    def saveToMongodb(config: DeepConfig,batch: Boolean = true): Unit = {
      schemaRDD.foreachPartition(it => {
        val writer =
          if (batch) new MongodbBatchWriter(config)
          else new MongodbSimpleWriter(config)
        writer.save(it.map(row =>
          MongodbRowConverter.rowAsDBObject(row, schemaRDD.schema)))
        writer.close()
      })
    }

  }

}
