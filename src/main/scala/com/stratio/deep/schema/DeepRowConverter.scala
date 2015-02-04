package com.stratio.deep.schema

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._

/**
 * Created by rmorandeira on 3/02/15.
 */
trait DeepRowConverter[T] {
  def asRow(schema: StructType, rdd: RDD[T]): RDD[Row]
}

