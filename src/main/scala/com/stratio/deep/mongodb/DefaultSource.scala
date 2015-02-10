package com.stratio.deep.mongodb

import com.stratio.deep.DeepConfig
import com.stratio.deep.DeepConfig._
import com.stratio.deep.mongodb.rdd.MongodbRDD
import com.stratio.deep.mongodb.schema.{MongodbRowConverter, MongodbSchema}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.{Row, SQLContext, StructType}
import MongodbConfig._

/**
 * Created by rmorandeira on 29/01/15.
 */

class DefaultSource extends RelationProvider {

  override def createRelation(
    sqlContext: SQLContext,
    parameters: Map[String, String]): BaseRelation = {

    /** We will assume hosts are provided like 'host:port,host2:port2,...'*/
    val host = parameters.getOrElse(Host, notFound(Host)).split(",").toList

    val database = parameters.getOrElse(Database, notFound(Database))

    val collection = parameters.getOrElse(Collection, notFound(Collection))

    val samplingRatio = parameters
      .get(SamplingRatio)
      .map(_.toDouble).getOrElse(1.0)

    MongodbRelation(
      MongodbConfigBuilder()
        .set(Host,host)
        .set(Database,database)
        .set(Collection,collection)
        .set(SamplingRatio,samplingRatio).build())(sqlContext)

  }

}

case class MongodbRelation(
  config: DeepConfig,
  schemaProvided: Option[StructType]=None)(
  @transient val sqlContext: SQLContext) extends PrunedFilteredScan {

  @transient lazy val lazySchema = {
    MongodbSchema(
      new MongodbRDD(sqlContext, config),
      config[Double](SamplingRatio)).schema()
  }

  override val schema: StructType = schemaProvided.getOrElse(lazySchema)

  override def buildScan(
    requiredColumns : Array[String],
    filters : Array[Filter]): RDD[Row] = {
    val rdd = new MongodbRDD(sqlContext,config,requiredColumns,filters)
    MongodbRowConverter.asRow(schema, rdd)
  }

}
