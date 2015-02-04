package com.stratio.deep.mongodb

import com.stratio.deep.mongodb.rdd.MongodbRDD
import com.stratio.deep.mongodb.schema.{MongodbRowConverter, MongodbSchema}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, RelationProvider, TableScan}
import org.apache.spark.sql.{Row, SQLContext, StructType}

/**
 * Created by rmorandeira on 29/01/15.
 */

class DefaultSource extends RelationProvider {

  override def createRelation(
    sqlContext: SQLContext,
    parameters: Map[String, String]): BaseRelation = {

    val host = parameters.getOrElse(
      "mongodb.host",
      sys.error("Option 'mongodb.host' not specified"))

    val database = parameters.getOrElse(
      "mongodb.database",
      sys.error("Option 'mongodb.database' not specified"))

    val collection = parameters.getOrElse(
      "mongodb.collection",
      sys.error("Option 'mongodb.collection' not specified"))

    val samplingRatio = parameters
      .get("mongodb.schema.samplingRatio")
      .map(_.toDouble).getOrElse(1.0)

    MongodbRelation(Config(host, database, collection, samplingRatio), None)(sqlContext)
  }

}

case class MongodbRelation(
  config: Config,
  schemaProvided: Option[StructType])(
  @transient val sqlContext: SQLContext) extends TableScan {

  private lazy val baseRDD =
    new MongodbRDD(sqlContext, config)

  override val schema: StructType = schemaProvided.getOrElse(lazySchema)

  @transient lazy val lazySchema = {
    MongodbSchema(baseRDD, config.samplingRatio).schema
  }

  override def buildScan(): RDD[Row] = {
    new MongodbRowConverter().asRow(schema, baseRDD)
  }

}
