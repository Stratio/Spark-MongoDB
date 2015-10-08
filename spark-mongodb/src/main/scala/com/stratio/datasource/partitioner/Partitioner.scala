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
package com.stratio.datasource.partitioner

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
