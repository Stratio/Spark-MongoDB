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
package com.stratio.datasource.mongodb.rdd

import com.mongodb.casbah.Imports._
import com.stratio.datasource.Config
import com.stratio.datasource.mongodb.reader.MongodbReader
import com.stratio.datasource.mongodb.query.FilterSection

import org.apache.spark._
import org.apache.spark.sql.sources.Filter

/**
 * MongoRDD values iterator.
 *
 * @param taskContext Spark task context.
 * @param partition Spark partition.
 * @param config Configuration object.
 * @param requiredColumns Pruning fields
 * @param filters Added query filters
 */
class MongodbRDDIterator(
  taskContext: TaskContext,
  partition: Partition,
  config: Config,
  requiredColumns: Array[String],
  filters: FilterSection)
  extends Iterator[DBObject] {

  protected var finished = false
  private var closed = false
  private var initialized = false

  lazy val reader = {
    initialized = true
    initReader()
  }

  // Register an on-task-completion callback to close the input stream.
  taskContext.addTaskCompletionListener((context: TaskContext) => closeIfNeeded())

  override def hasNext: Boolean = {
    !finished && reader.hasNext
  }

  override def next(): DBObject = {
    if (!hasNext) {
      throw new NoSuchElementException("End of stream")
    }
    reader.next()
  }

  def closeIfNeeded(): Unit = {
    if (!closed) {
      close()
      closed = true
    }
  }

  protected def close(): Unit = {
    if (initialized) {
      reader.close()
    }
  }

  def initReader() = {
    val reader = new MongodbReader(config, requiredColumns, filters)
    reader.init(partition)
    reader
  }
}