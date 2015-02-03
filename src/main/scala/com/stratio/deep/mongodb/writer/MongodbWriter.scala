package com.stratio.deep.mongodb.writer

import com.mongodb._
import com.stratio.deep.mongodb.Config
import org.apache.spark.Partition


class MongodbWriter {
	/**
	 * The Mongo client.
	 */
	private var mongoClient: Option[MongoClient] = None

	val writeConcern: Option[WriteConcern] = None



/**
 * Save void.
 * 
 * @param dbObject
 *            the db object
 */

def save (DBObject): Unit = {
		val db = 
}

/**
 * Close void.
 */
def close(): Unit = {
		mongoClient.fold(ifEmpty = ())(_.close)
}

}