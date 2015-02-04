package com.stratio.deep.mongodb.writer

import com.mongodb._
import com.stratio.deep.mongodb.Config
import scala.collection.JavaConversions._


class MongodbWriter(
                     config: Config,
                     writeConcern: WriteConcern) {
  /**
   * The Mongo client.
   */
  val mongoClient: MongoClient =
    new MongoClient(List(new ServerAddress(config.host)))

  val dbCollection = mongoClient
    .getDB(config.database)
    .getCollection(config.collection)

  def save (dbObject: DBObject): Unit = {
    dbCollection.save(dbObject,writeConcern)
  }

  def close(): Unit = {
    mongoClient.close()
  }

}