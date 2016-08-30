/*
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
import com.stratio.datasource.util.Config

/**
 * A simple mongodb writer.
 *
 * @param config Configuration parameters (host,database,collection,...)
 */
private[mongodb] class MongodbSimpleWriter(config: Config) extends MongodbWriter(config) {

  override def save(it: Iterator[DBObject], mongoClient: MongoClient): Unit =
    it.foreach(dbo => dbCollection(mongoClient).save(dbo, writeConcern))

}
