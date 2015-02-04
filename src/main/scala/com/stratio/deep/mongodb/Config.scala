package com.stratio.deep.mongodb


/**
 * Created by jsantos on 3/02/15.
 */
case class Config(
  host: String,
  database: String,
  collection: String,
  samplingRatio: Double = 1.0)

object Config extends Serializable {
  val Host = "mongodb.host"
  val Database = "mongodb.database"
  val Collection = "mongodb.collection"
  val SamplingRatio = "mongodb.schema.samplingRatio"

  /** Defines how to act in case any parameter is not set*/
  def notFound(key: String) = sys.error(s"Parameter $key not specified")

}
