package com.stratio.deep.schema

import org.apache.spark.sql._

/**
 * Created by rmorandeira on 30/01/15.
 */
trait DeepSchema {
  def toStructType(): StructType
}
