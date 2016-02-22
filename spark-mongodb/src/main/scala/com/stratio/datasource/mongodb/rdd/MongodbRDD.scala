/**
 * Copyright (C) 2015 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.datasource.mongodb.rdd

import com.mongodb.casbah.Imports._
import com.stratio.datasource.mongodb.partitioner.{MongodbPartition, MongodbPartitioner}
import com.stratio.datasource.util.Config
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{Partition, TaskContext}

import com.stratio.datasource.mongodb.query.{NoFilters, FilterSection}

/**
 * @param sc Spark SQLContext
 * @param config Config parameters
 * @param requiredColumns Fields to project
 * @param filters Query filters
 */
class MongodbRDD(
  sc: SQLContext,
  config: Config,
  partitioner: MongodbPartitioner,
  requiredColumns: Array[String] = Array(),
  filters: FilterSection = NoFilters)
  extends RDD[DBObject](sc.sparkContext, deps = Nil) {

  override def getPartitions: Array[Partition] =
    partitioner.computePartitions().asInstanceOf[Array[Partition]]

  override def getPreferredLocations(split: Partition): Seq[String] =
    split.asInstanceOf[MongodbPartition].hosts.map(new ServerAddress(_).getHost)

  override def compute(
    split: Partition,
    context: TaskContext): MongodbRDDIterator =
    new MongodbRDDIterator(
      context,
      split.asInstanceOf[MongodbPartition],
      config,
      requiredColumns,
      filters)

}
