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
package org.apache.spark.sql.mongodb

import org.apache.spark.sql.{SQLConf, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}


/**
 * A special [[SQLContext]] prepared for testing.
 */
class TestSQLContext(sc: SparkContext) extends SQLContext(sc) { self =>

  def this() {
    this(new SparkContext("local[2]", "test-sql-context",
      new SparkConf().set("spark.sql.testkey", "true")))
  }

  protected[sql] override lazy val conf: SQLConf = new SQLConf {

    clear()

    override def clear(): Unit = {
      super.clear()

      // Make sure we start with the default test configs even after clear
      TestSQLContext.overrideConfs.map {
        case (key, value) => setConfString(key, value)
      }
    }
  }

}

object TestSQLContext {

  /**
   * A map used to store all confs that need to be overridden in sql/core unit tests.
   */
  val overrideConfs: Map[String, String] =
    Map(
      // Fewer shuffle partitions to speed up testing.
      SQLConf.SHUFFLE_PARTITIONS.key -> "5")
}

object TemporaryTestSQLContext extends TestSQLContext