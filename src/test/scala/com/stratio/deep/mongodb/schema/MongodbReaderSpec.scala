package com.stratio.deep.mongodb.schema

import com.stratio.deep.DeepConfig
import com.stratio.deep.mongodb.MongodbConfig
import com.stratio.deep.mongodb.reader.MongodbReader
import org.apache.spark.Partition
import org.scalatest.{Matchers, FlatSpec}

/**
 * Created by lfernandez on 9/02/15.
 */
class MongodbReaderSpec extends FlatSpec
with Matchers
with MongoEmbedDatabase
with TestBsonData {

  private val host: String = "localhost"
  private val port: Int = 12345
  private val database: String = "testDb"
  private val collection: String = "testCol"

  val testConfig = DeepConfig()
    .set(MongodbConfig.Host, List(host + ":" + port))
    .set(MongodbConfig.Database, database)
    .set(MongodbConfig.Collection, collection)
    .set(MongodbConfig.SamplingRatio, 1.0)

  behavior of "A reader"

  it should "throw NoSuchElementException if an empty stack is popped" in {
    val mongoReader = new MongodbReader(testConfig)
    mongoReader.init(new Partition {
      override def index: Int = 0
    })
    mongoReader.close()

    a[IllegalStateException] should be thrownBy {
      mongoReader.next()
    }
  }
}
