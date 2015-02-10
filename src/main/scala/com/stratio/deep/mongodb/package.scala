package com.stratio.deep

import com.stratio.deep.mongodb.schema.MongodbRowConverter
import com.stratio.deep.mongodb.writer.{MongodbSimpleWriter, MongodbBatchWriter}
import org.apache.spark.sql.{SQLContext, SchemaRDD}

/**
 * Created by rmorandeira on 28/01/15.
 */
package object mongodb {

  /**
   * Adds a method, fromMongodb, to SQLContext that allows reading data stored in Mongodb.
   */
  implicit class MongodbContext(sqlContext: SQLContext) {
    def fromMongoDB(config: DeepConfig): SchemaRDD = {
      sqlContext.baseRelationToSchemaRDD(MongodbRelation(config, None)(sqlContext))
    }

  }

  /**
   * Adds a method, fromMongodb, to schemaRDD that allows storing data in Mongodb.
   */
  implicit class MongodbSchemaRDD(schemaRDD: SchemaRDD) extends Serializable {

    def saveToMongodb(config: DeepConfig,batch: Boolean = true): Unit = {
      schemaRDD.foreachPartition(it => {
        val writer =
          if (batch) new MongodbBatchWriter(config)
          else new MongodbSimpleWriter(config)
        writer.save(it.map(row =>
          MongodbRowConverter.rowAsDBObject(row, schemaRDD.schema)))
        writer.close()
      })
    }

  }

}
