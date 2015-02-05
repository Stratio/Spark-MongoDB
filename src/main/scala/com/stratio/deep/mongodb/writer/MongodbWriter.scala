package com.stratio.deep.mongodb.writer

import com.mongodb._
import com.stratio.deep.mongodb.Config
import scala.collection.JavaConversions._

/**
 * Created by jsantos on 5/02/15.
 *
 * Abstract Mongodb writer.
 * Used for saving a bunch of mongodb objects
 * into specified database and collection
 *
 * @param config Configuration parameters (host,database,collection,...)
 */
abstract class MongodbWriter(config:Config) extends Serializable{

  protected val mongoClient: MongoClient =
    new MongoClient(config.host.map(add => new ServerAddress(add)).toList)

  protected val dbCollection: DBCollection =
    mongoClient
      .getDB(config.database)
      .getCollection(config.collection)

  /**
   * Abstract method for storing a bunch of dbobjects
   *
   * @param it Iterator of mongodb objects.
   */
  def save(it: Iterator[DBObject]): Unit

  def close(): Unit = {
    mongoClient.close()
  }

}