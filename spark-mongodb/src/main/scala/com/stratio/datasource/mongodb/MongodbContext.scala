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
package com.stratio.datasource.mongodb

import com.stratio.datasource.util.Config
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.types.StructType

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
      new MongodbRelation(config, schema)(sqlContext))

}
