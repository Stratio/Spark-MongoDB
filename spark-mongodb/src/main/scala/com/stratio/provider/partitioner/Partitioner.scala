package com.stratio.provider.partitioner

import org.apache.spark.Partition

/**
 * Provides the way to compute and get spark partitions over
 * some Data Source.
 * @tparam T
 */
trait Partitioner[T <: Partition] extends Serializable {

  /**
   * Retrieves some Data Source partitions
   * @return An array with computed partitions
   */
  def computePartitions(): Array[T]

}
