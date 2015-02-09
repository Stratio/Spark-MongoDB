package com.stratio.deep.mongodb.reader

import com.mongodb._
import com.stratio.deep.DeepConfig
import com.stratio.deep.mongodb.MongodbConfig
import org.apache.spark.Partition

import scala.collection.JavaConversions._
import scala.util.Try

/**
 * Created by rmorandeira on 29/01/15.
 */
class MongodbReader(config: DeepConfig) {

  private val mongoClient: MongoClient =
    new MongoClient(config[List[String]](MongodbConfig.Host)
      .map(add => new ServerAddress(add)).toList,
      List.empty[MongoCredential])

  private val db = mongoClient.getDB(config(MongodbConfig.Database))

  private val collection = db.getCollection(config(MongodbConfig.Collection))

  private var dbCursor: Option[DBCursor] = None

  /**
   * Close void.
   */
  def close(): Unit = {
    dbCursor.fold(ifEmpty = ()){cursor =>
      cursor.close
      dbCursor = None
    }
    mongoClient.close()
  }

  /**
   * Has next.
   *
   * @return the boolean
   */
  def hasNext: Boolean = {
    dbCursor.fold(ifEmpty = false)(_.hasNext)
  }

  /**
   * Next row.
   *
   * @return the cells
   */
  def next(): DBObject = {
    dbCursor.fold(ifEmpty =
      throw new IllegalStateException("DbCursor is not initialized"))(_.next())
  }

  /**
   * Init void.
   *
   * @param partition the partition
   */
  def init(partition: Partition): Unit =
    Try {
      dbCursor = Option(collection.find(createQueryPartition(partition)))
    }.recover{
      case throwable => throw MongodbReadException(throwable.getMessage,throwable)
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

case class MongodbReadException(
  msg: String,
  causedBy: Throwable) extends RuntimeException(msg,causedBy)
