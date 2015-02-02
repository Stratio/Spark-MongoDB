package com.stratio.deep.mongodb.reader

import java.net.UnknownHostException

import com.mongodb._
import org.apache.spark.Partition

import scala.collection.JavaConversions._

/**
 * Created by rmorandeira on 29/01/15.
 */
class MongodbReader {
  /**
   * The Mongo client.
   */
  private var mongoClient: MongoClient = null
  /**
   * The Db cursor.
   */
  private var dbCursor: DBCursor = null

  /**
   * Close void.
   */
  def close(): Unit = {
    if (dbCursor != null) {
      dbCursor.close
    }
    if (mongoClient != null) {
      mongoClient.close
    }
  }

  /**
   * Has next.
   *
   * @return the boolean
   */
  def hasNext(): Boolean = {
    return dbCursor.hasNext
  }

  /**
   * Next row.
   *
   * @return the cells
   */
  def next(): DBObject = {
    return dbCursor.next
  }

  /**
   * Init void.
   *
   * @param partition the partition
   */
  def init(partition: Partition): Unit = {
    try {
      val addressList: List[ServerAddress] = List[ServerAddress](new ServerAddress("172.28.128.3:27017"))
      val mongoCredentials: List[MongoCredential] = List.empty

      mongoClient = new MongoClient(addressList, mongoCredentials)
      val db = mongoClient.getDB("football")
      val collection = db.getCollection("teams")
      dbCursor = collection.find(createQueryPartition(partition))
    }
    catch {
      case e: UnknownHostException => {
        throw e
      }
    }
  }

  /**
   * Create query partition.
   *
   * @param partition the partition
   * @return the dB object
   */
  private def createQueryPartition(partition: Partition): DBObject = {
//    val queryBuilderMin: QueryBuilder = QueryBuilder.start()
//    val bsonObjectMin: DBObject = queryBuilderMin.get
//    val queryBuilderMax: QueryBuilder = QueryBuilder.start()
//    val bsonObjectMax: DBObject = queryBuilderMax.get
    val queryBuilder: QueryBuilder = QueryBuilder.start

    queryBuilder.get
  }
}
