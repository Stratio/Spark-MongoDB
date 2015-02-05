package com.stratio.deep.mongodb.writer

import com.mongodb.{DBObject, ServerAddress, MongoClient, WriteConcern}
import com.stratio.deep.DeepConfig
import com.stratio.deep.mongodb.MongodbConfig

/**
 * Created by jsantos on 5/02/15.
 *
 * A simple mongodb writer.
 *
 * @param config Configuration parameters (host,database,collection,...)
 */
class MongodbSimpleWriter(
  config: DeepConfig) extends MongodbWriter(config) {

  def save(it: Iterator[DBObject]): Unit =
    it.foreach(dbo =>
      dbCollection.save(dbo, config(MongodbConfig.WriteConcern)))

}
