package com.stratio.deep.mongodb.rdd

import org.apache.spark.Partition

/**
 * Created by rmorandeira on 29/01/15.
 */
class MongodbPartition(rddId: Int, idx: Int) extends Partition {

  override def hashCode(): Int = 41 * (41 * (41 + rddId) + idx)

  override val index: Int = idx

}