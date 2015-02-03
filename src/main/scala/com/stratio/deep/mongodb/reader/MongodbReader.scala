package com.stratio.deep.mongodb.reader

import java.net.UnknownHostException

import com.mongodb._
import com.stratio.deep.mongodb.Config
import org.apache.spark.Partition

import scala.collection.JavaConversions._

/**
 * Created by rmorandeira on 29/01/15.
 */
class MongodbReader {
  /**
   * The Mongo client.
   */
  private var mongoClient: Option[MongoClient] = None

  /**
   * The Db cursor.
   */
  private var dbCursor: Option[DBCursor] = None

  /**
   * Close void.
   */
  def close(): Unit = {
    dbCursor.fold(ifEmpty=())(_.close)
    mongoClient.fold(ifEmpty = ())(_.close)
  }

  /**
   * Has next.
   *
   * @return the boolean
   */
  def hasNext(): Boolean = {
    dbCursor.fold(ifEmpty = false)(_.hasNext())
  }

  /**
   * Next row.
   *
   * @return the cells
   */
  def next(): DBObject = {
    dbCursor.fold(ifEmpty=
      throw new IllegalStateException("DbCursor is not initialized"))(_.next())
  }

  /**
   * Init void.
   *
   * @param partition the partition
   */
  def init(partition: Partition)(config: Config): Unit = {
    //TODO: Catch any exception and throw a specific exception type

    val addressList: List[ServerAddress] = List(new ServerAddress(config.host))
    val mongoCredentials: List[MongoCredential] = List.empty

    Some(new MongoClient(addressList, mongoCredentials)).foreach{client =>
      val db = client.getDB(config.database)
      val collection = db.getCollection(config.collection)
      dbCursor = Some(collection.find(createQueryPartition(partition)))
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
