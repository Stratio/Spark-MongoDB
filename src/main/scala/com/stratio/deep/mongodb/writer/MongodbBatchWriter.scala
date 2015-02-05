package com.stratio.deep.mongodb.writer

import com.mongodb.{BasicDBObject, DBObject, WriteConcern}
import com.stratio.deep.mongodb.Config

/**
 * Created by jsantos on 4/02/15.
 *
 * A batch writer implementation for mongodb writer.
 * The used semantics for storing objects is 'replace'.
 *
 * @param config Configuration parameters (host,database,collection,...)
 */
class MongodbBatchWriter(
  config: Config) extends MongodbWriter(config){

  private val Id = "_id"
  
  def save(it: Iterator[DBObject]): Unit = {
    import scala.collection.JavaConversions._
    val bulkop = dbCollection.initializeUnorderedBulkOperation()
    it.foreach{element =>
      val query = new BasicDBObject(Map(Id -> element.get(Id)))
      bulkop.find(query).upsert().replaceOne(element)
    }
    bulkop.execute()
  }

}