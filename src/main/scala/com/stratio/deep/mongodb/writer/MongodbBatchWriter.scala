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

package com.stratio.deep.mongodb.writer

import com.mongodb.{WriteConcern, BasicDBObject, DBObject}
import com.stratio.deep.DeepConfig
import com.stratio.deep.mongodb.MongodbConfig

/**
 * Created by jsantos on 4/02/15.
 *
 * A batch writer implementation for mongodb writer.
 * The used semantics for storing objects is 'replace'.
 *
 * @param config Configuration parameters (host,database,collection,...)
 * @param batchSize Group size to be inserted via bulk operation
 */
class MongodbBatchWriter(
  config: DeepConfig,
  batchSize: Int = 100) extends MongodbWriter(config){
  
  def save(it: Iterator[DBObject]): Unit = {
    import scala.collection.JavaConversions._
    val Id = "_id"
    it.grouped(batchSize).foreach{ group =>
      val bulkop = dbCollection.initializeUnorderedBulkOperation()
      group.foreach{element =>
        val query = new BasicDBObject(Map(Id -> element.get(Id)))
        bulkop.find(query).upsert().replaceOne(element)
      }
      bulkop.execute(config[WriteConcern](MongodbConfig.WriteConcern))
    }

  }

}