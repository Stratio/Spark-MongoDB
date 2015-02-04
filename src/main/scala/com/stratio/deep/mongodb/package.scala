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
    def fromMongoDB(config: Config): SchemaRDD = {
      sqlContext.baseRelationToSchemaRDD(MongodbRelation(config, None)(sqlContext))
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
