package com.stratio.provider.mongodb.reader

import com.stratio.provider.DeepConfig
import com.stratio.provider.mongodb.{MongodbConfigBuilder, MongodbConfig}
import org.apache.spark.sql.sources
import org.apache.spark.sql.sources.EqualTo
import org.apache.spark.sql.types.UTF8String
import org.scalatest.{Matchers, FlatSpec}

/**
 * Created by pmadrigal on 25/06/15.
 */
class UTF8String extends FlatSpec
with Matchers {

  private val host: String = "localhost"
  private val port: Int = 12345
  private val database: String = "testDb"
  private val collection: String = "testCol"
  val utf8string = UTF8String("fdsgds")

  val testConfig = MongodbConfigBuilder()
    .set(MongodbConfig.Host, List(12 + ":" + port))
    .set(MongodbConfig.Database, database)
    .set(MongodbConfig.Collection, collection)
    .set(MongodbConfig.SamplingRatio, 1.0)
    .build()
  val reader = new MongodbReader(testConfig,
  Array("hola"),
    Array (new EqualTo("a", "b")))
  reader.queryPartition(Array (new EqualTo(utf8string.toString(), utf8string.toString())))

}
