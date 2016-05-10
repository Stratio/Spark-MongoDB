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
package com.stratio.datasource.mongodb.writer

import com.mongodb.casbah.Imports._
import com.stratio.datasource.mongodb.config.MongodbConfig
import com.stratio.datasource.util.Config

/**
 * A batch writer implementation for mongodb writer.
 * The used semantics for storing objects is 'replace'.
 *
 * @param config Configuration parameters (host,database,collection,...)
 */
class MongodbBatchWriter(config: Config) extends MongodbWriter(config) {

  private val IdKey = "_id"

  private val bulkBatchSize = config.getOrElse(MongodbConfig.BulkBatchSize, MongodbConfig.DefaultBulkBatchSize)

  private val pkConfig: Option[Array[String]] = config.get[Array[String]](MongodbConfig.UpdateFields)

  override def save(it: Iterator[DBObject]): Unit = {
    it.grouped(bulkBatchSize).foreach { group =>
      val bulkOperation = dbCollection.initializeUnorderedBulkOperation
      group.foreach { element =>
        val query = getUpdateQuery(element)
        if (query.isEmpty) bulkOperation.insert(element)
        else bulkOperation.find(query).upsert().replaceOne(element)
      }

      bulkOperation.execute(writeConcern)
    }
  }

  private def getUpdateQuery(element: DBObject): Map[String, AnyRef] = {
    if(element.contains(IdKey)) Map(IdKey -> element.get(IdKey))
    else {
      val pkValues : Map[String, AnyRef] =
        if (pkConfig.isDefined)
          pkConfig.get.flatMap(field => if (element.contains(field)) Some(field -> element.get(field)) else None).toMap
        else Map.empty[String, AnyRef]
      pkValues
    }
  }
}