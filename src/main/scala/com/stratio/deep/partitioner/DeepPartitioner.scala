package com.stratio.deep.partitioner

import org.apache.spark.Partition

/**
 * Created by rmorandeira on 6/02/15.
 */
trait DeepPartitioner {

  def computePartitions(): Array[Partition]

}
