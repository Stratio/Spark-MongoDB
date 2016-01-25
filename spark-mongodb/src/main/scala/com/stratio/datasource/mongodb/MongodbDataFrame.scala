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

package com.stratio.datasource.mongodb

import com.stratio.datasource.mongodb.schema.MongodbRowConverter
import com.stratio.datasource.mongodb.writer.{MongodbSimpleWriter, MongodbBatchWriter}
import com.stratio.datasource.util.Config
import org.apache.spark.sql.DataFrame

import scala.language.implicitConversions

/**
 * @param dataFrame Spark SchemaRDD
 */
class MongodbDataFrame(dataFrame: DataFrame) extends Serializable {

  /**
   * It allows storing data in Mongodb from some existing SchemaRDD
   * @param config MongoDB configuration object
   * @param batch It indicates whether it has to be saved in batch mode or not.
   */
  def saveToMongodb(config: Config, batch: Boolean = true): Unit = {
    val schema = dataFrame.schema
    dataFrame.foreachPartition(it => {
      val writer =
        if (batch) new MongodbBatchWriter(config)
        else new MongodbSimpleWriter(config)
      writer.saveWithPk(it.map(row =>
        MongodbRowConverter.rowAsDBObject(row, schema)))
      writer.freeConnection()
    })
  }

}
