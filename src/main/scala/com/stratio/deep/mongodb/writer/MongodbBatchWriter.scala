package com.stratio.deep.mongodb.writer

import com.mongodb.{BasicDBObject, DBObject}
import com.stratio.deep.mongodb.Config

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
  config: Config,
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
      bulkop.execute()
    }

  }

}