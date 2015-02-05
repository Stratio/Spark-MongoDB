package com.stratio.deep.mongodb

import com.stratio.deep.mongodb.rdd.MongodbRDD
import com.stratio.deep.mongodb.schema.{MongodbRowConverter, MongodbSchema}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, RelationProvider, TableScan}
import org.apache.spark.sql.{Row, SQLContext, StructType}
import Config._

/**
 * Created by rmorandeira on 29/01/15.
 */

class DefaultSource extends RelationProvider {

  override def createRelation(
    sqlContext: SQLContext,
    parameters: Map[String, String]): BaseRelation = {

    /** We will assume hosts are provided like 'host:port,host2:port2,...'*/
    val host = parameters.getOrElse(Host, notFound(Host)).split(",")

    val database = parameters.getOrElse(Database, notFound(Database))

    val collection = parameters.getOrElse(Collection, notFound(Collection))

    val samplingRatio = parameters
      .get(SamplingRatio)
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
    MongodbSchema(baseRDD, config.samplingRatio).schema()
  }

  override def buildScan(): RDD[Row] = {
    MongodbRowConverter.asRow(schema, baseRDD)
  }

}
