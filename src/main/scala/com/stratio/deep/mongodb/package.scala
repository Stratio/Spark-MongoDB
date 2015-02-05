package com.stratio.deep

import com.stratio.deep.mongodb.schema.MongodbRowConverter
import com.stratio.deep.mongodb.writer.MongodbWriter
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
  implicit class MongodbSchemaRDD(schemaRDD: SchemaRDD) extends Serializable {

    def saveToMongodb(writer: => MongodbWriter): Unit = {
      schemaRDD.foreachPartition(it => {
        writer.save(it.map(row =>
          MongodbRowConverter.rowAsDBObject(row, schemaRDD.schema)))
        writer.close()
      })
    }

  }

}
