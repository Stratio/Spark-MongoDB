/*
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
package com.stratio.datasource.mongodb.partitioner

import com.mongodb.casbah.Imports._
import com.stratio.datasource.partitioner.PartitionRange
import org.apache.spark.Partition

/**
 * @param index Partition index
 * @param hosts Hosts that hold partition data
 * @param partitionRange Partition range
 */
case class MongodbPartition(
  index: Int,
  hosts: Seq[String],
  partitionRange: PartitionRange[DBObject]) extends Partition