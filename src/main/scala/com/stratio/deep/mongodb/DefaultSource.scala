package com.stratio.deep.mongodb

import com.mongodb.{BasicDBList, BasicDBObject}
import com.stratio.deep.mongodb.rdd.MongodbRowRDD
import com.stratio.deep.mongodb.schema.MongodbSchema
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, RelationProvider, TableScan}
import org.apache.spark.sql.{Row, SQLContext, StructType}

/**
 * Created by rmorandeira on 29/01/15.
 */

class DefaultSource extends RelationProvider {
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    val host = parameters.getOrElse("mongodb.host", sys.error("Option 'mongodb.host' not specified"))
    val database = parameters.getOrElse("mongodb.database", sys.error("Option 'mongodb.database' not specified"))
    val collection = parameters.getOrElse("mongodb.collection", sys.error("Option 'mongodb.collection' not specified"))
    val samplingRatio = parameters.get("mongodb.schema.samplingRatio").map(_.toDouble).getOrElse(1.0)

    MongodbRelation(host, database, collection, samplingRatio)(sqlContext)
  }
}

case class MongodbRelation(host: String, database: String, collection: String, samplingRation: Double = 1.0)
                          (@transient val sqlContext: SQLContext) extends TableScan {

  override def schema: StructType = lazySchema.toStructType()

  @transient lazy val lazySchema = {
    val list = new BasicDBList()
    list.add("one")
    MongodbSchema.schema(new BasicDBObject("_id", 1).append("name", "name").append("players", list).append("other",
    new BasicDBObject("pepe", 3)))
  }

  override def buildScan(): RDD[Row] = {
    new MongodbRowRDD(sqlContext, lazySchema, host, database, collection)
  }
}
