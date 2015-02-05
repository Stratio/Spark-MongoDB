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