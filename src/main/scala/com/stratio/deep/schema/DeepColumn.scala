package com.stratio.deep.schema

import org.apache.spark.sql.catalyst.types._

/**
 * Created by rmorandeira on 30/01/15.
 */
trait DeepColumn extends Serializable {

  def toSqlVal(value: Any): Any

  def toStructField(): StructField
}
