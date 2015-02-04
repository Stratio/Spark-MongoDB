package com.stratio.deep

import com.mongodb.{BasicDBObject, DBObject, WriteConcern}
import com.stratio.deep.mongodb.Config
import com.stratio.deep.mongodb.schema.MongodbRowConverter
import com.stratio.deep.mongodb.writer.MongodbWriter
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.types.StructType
import org.apache.spark.sql.{SQLContext, SchemaRDD, Row}
import Config._

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
  implicit class MongodbSchemaRDD(schemaRDD: SchemaRDD) {

    def saveToMongodb(parameters: Map[String, String]): Unit = {
      val config = Config(
        parameters.getOrElse(Host, notFound(Host)),
        parameters.getOrElse(Database, notFound(Database)),
        parameters.getOrElse(Collection, notFound(Collection)))
      //TODO It's necessary to implement a helper for translating from String to WriteConcern constant.
      schemaRDD.foreachPartition(it => {
        val writer = new MongodbWriter(config, WriteConcern.NORMAL)
        writer.save(it.map(row =>
          MongodbRowConverter.rowAsDBObject(row, schemaRDD.schema)))
        writer.close()
      })
    }

  }

}
