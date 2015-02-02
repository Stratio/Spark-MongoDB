package com.stratio.deep

import org.apache.spark.sql.{SQLContext, SchemaRDD}

/**
 * Created by rmorandeira on 28/01/15.
 */
package object mongodb {
  /**
   * Adds a method, fromMongodb, to SQLContext that allows reading data stored in Mongodb.
   */
  implicit class MongodbContext(sqlContext: SQLContext) {
    def fromMongoDB(host: String, database: String, collection: String, samplingRation: Double = 1.0): SchemaRDD = {
      sqlContext.baseRelationToSchemaRDD(MongodbRelation(host, database, collection, samplingRation)(sqlContext))
    }
  }

  /**
   * Adds a method, fromMongodb, to schemaRDD that allows storing data in Mongodb.
   */
  // TODO:
  implicit class MongodbSchemaRDD(schemaRDD: SchemaRDD) {
    def saveToMongodb(parameters: Map[String, String]): Unit = ???
  }

}
