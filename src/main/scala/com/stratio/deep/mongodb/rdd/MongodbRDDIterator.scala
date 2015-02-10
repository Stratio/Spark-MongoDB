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

package com.stratio.deep.mongodb.rdd

import com.mongodb.DBObject
import com.stratio.deep.DeepConfig
import com.stratio.deep.mongodb.reader.MongodbReader
import org.apache.spark._

/**
 * Created by rmorandeira on 29/01/15.
 */
class MongodbRDDIterator(
  taskContext: TaskContext,
  partition: Partition,
  config: DeepConfig)
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
    val reader = new MongodbReader(config)
    reader.init(partition)
    reader
  }
}