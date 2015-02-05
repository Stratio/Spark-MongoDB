package com.stratio.deep.mongodb

import com.stratio.deep.DeepConfig
import com.stratio.deep.DeepConfig._
import com.stratio.deep.mongodb.rdd.MongodbRDD
import com.stratio.deep.mongodb.schema.{MongodbRowConverter, MongodbSchema}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources.{BaseRelation, RelationProvider, TableScan}
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
      DeepConfig()
        .set(Host,host)
        .set(Database,database)
        .set(Collection,collection)
        .set(SamplingRatio,samplingRatio))(sqlContext)
    
  }

}

case class MongodbRelation(
  config: DeepConfig,
  schemaProvided: Option[StructType]=None)(
  @transient val sqlContext: SQLContext) extends TableScan {

  private lazy val baseRDD =
    new MongodbRDD(sqlContext, config)

  override val schema: StructType = schemaProvided.getOrElse(lazySchema)

  @transient lazy val lazySchema = {
    MongodbSchema(baseRDD, config[Double](SamplingRatio)).schema()
  }

  override def buildScan(): RDD[Row] = {
    MongodbRowConverter.asRow(schema, baseRDD)
  }

}
