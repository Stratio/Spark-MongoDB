package com.stratio.deep.mongodb

import com.mongodb
import com.stratio.deep.{DeepConfigBuilder, DeepConfig}
import DeepConfig._
import MongodbConfig._

/**
 * Created by jsantos on 5/02/15.
 *
 * A specialized deep configuration builder.
 * It focuses on mongodb config parameters
 * such as host,database,collection, samplingRatio (for schema infer)
 * @param properties
 */
case class MongodbConfigBuilder(
  override val properties: Map[Property,Any]=Map(
    //default values
    SamplingRatio -> 1.0,
    WriteConcern -> mongodb.WriteConcern.NORMAL
  )) extends DeepConfigBuilder[MongodbConfigBuilder](properties) {

  val requiredProperties: List[Property] = MongodbConfig.all

  def apply(props: Map[Property,Any]) =
    MongodbConfigBuilder(props)
}

object MongodbConfig {
  //  Parameter names

  val Host = "mongodb.host"
  val Database = "mongodb.database"
  val Collection = "mongodb.collection"
  val SamplingRatio = "mongodb.schema.samplingRatio"
  val WriteConcern = "mongodb.writeConcern"

  val all = List(Host,Database,Collection,SamplingRatio,WriteConcern)

}
