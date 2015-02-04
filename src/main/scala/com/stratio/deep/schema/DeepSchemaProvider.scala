package com.stratio.deep.schema

import org.apache.spark.sql._

/**
 * Created by rmorandeira on 3/02/15.
 */
trait DeepSchemaProvider {
  def schema(): StructType
}
