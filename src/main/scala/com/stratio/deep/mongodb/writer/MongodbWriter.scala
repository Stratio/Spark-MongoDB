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

package com.stratio.deep.mongodb.writer

import com.mongodb._
import com.stratio.deep.DeepConfig
import com.stratio.deep.mongodb.MongodbConfig
import scala.collection.JavaConversions._

/**
 * Created by jsantos on 5/02/15.
 *
 * Abstract Mongodb writer.
 * Used for saving a bunch of mongodb objects
 * into specified database and collection
 *
 * @param config Configuration parameters (host,database,collection,...)
 */
abstract class MongodbWriter(config:DeepConfig) extends Serializable{

  protected val mongoClient: MongoClient =
    new MongoClient(config[List[String]](MongodbConfig.Host)
      .map(add => new ServerAddress(add)).toList)

  protected val dbCollection: DBCollection =
    mongoClient
      .getDB(config(MongodbConfig.Database))
      .getCollection(config(MongodbConfig.Collection))

  /**
   * Abstract method for storing a bunch of dbobjects
   *
   * @param it Iterator of mongodb objects.
   */
  def save(it: Iterator[DBObject]): Unit

  def close(): Unit = {
    mongoClient.close()
  }

}