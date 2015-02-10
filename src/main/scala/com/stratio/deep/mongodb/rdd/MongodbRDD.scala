/*
 *  Licensed to STRATIO (C) under one or more contributor license agreements.
 *  See the NOTICE file distributed with this work for additional information
 *  regarding copyright ownership. The STRATIO (C) licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied. See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package com.stratio.deep.mongodb.rdd

import com.mongodb.DBObject
import com.stratio.deep.DeepConfig
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{Partition, TaskContext}

/**
 * Created by rmorandeira on 29/01/15.
 */


class MongodbRDD(
  sc: SQLContext,
  config: DeepConfig)
  extends RDD[DBObject](sc.sparkContext, deps = Nil) {

  override def getPartitions: Array[Partition] = {
    val sparkPartitions = new Array[Partition](1)
    val idx: Int = 0
    sparkPartitions(idx) = new MongodbPartition(id, idx)
    sparkPartitions
  }

  override def getPreferredLocations(split: Partition): Seq[String] =
    super.getPreferredLocations(split)

  override def compute(
    split: Partition,
    context: TaskContext): MongodbRDDIterator =
    new MongodbRDDIterator(context, split.asInstanceOf[MongodbPartition], config)

}
