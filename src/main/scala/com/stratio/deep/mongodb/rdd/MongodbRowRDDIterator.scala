package com.stratio.deep.mongodb.rdd

import com.mongodb.DBObject
import com.stratio.deep.mongodb.reader.MongodbReader
import com.stratio.deep.mongodb.schema.{MongodbRecord, MongodbSchema}
import org.apache.spark._
import org.apache.spark.sql.Row

/**
 * Created by rmorandeira on 29/01/15.
 */
class MongodbRowRDDIterator(taskContext: TaskContext,
                            schema: MongodbSchema,
                            partition: Partition)
  extends Iterator[Row] {

  protected var finished = false
  private var closed = false
  private var initialized = false

  lazy val reader = {
    initialized = true
    initReader()
  }

  // Register an on-task-completion callback to close the input stream.
  taskContext.addTaskCompletionListener((context :TaskContext) => closeIfNeeded())

  override def hasNext(): Boolean = {
    !finished && reader.hasNext()
  }
  override def next(): Row = {
    if (!hasNext) {
      throw new NoSuchElementException("End of stream")
    }
    val value = reader.next()
    createRow(value)
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
    val reader = new MongodbReader()
    reader.init(partition)
    reader
  }

  def createRow(doc: DBObject): Row = {
    val values = MongodbRecord(schema, doc).values()
    Row.fromSeq(values)
  }
}