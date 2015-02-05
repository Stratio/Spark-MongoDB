package com.stratio.deep.mongodb.schema

import com.stratio.deep.mongodb.Config
import com.stratio.deep.mongodb.rdd.MongodbRDD
import org.apache.spark.sql.test.TestSQLContext
import org.scalatest._

/**
 * Created by rmorandeira on 4/02/15.
 */
class MongodbSchemaSpec extends FlatSpec
  with Matchers
  with MongoEmbedDatabase
  with TestBsonData {

  private val host: String = "localhost"
  private val port: Int = 12345
  private val database: String = "testDb"
  private val collection: String = "testCol"

  behavior of "A schema"

  it should "be inferred from rdd with primitives" in {
    withEmbedMongoFixture(primitiveFieldAndType) { mongodProc =>
      val mongodbRDD = new MongodbRDD(TestSQLContext, Config(List(host + ":" + port), database, collection))
      val schema = MongodbSchema(mongodbRDD, 1.0).schema()

      schema.fields should have size 7
      schema.fieldNames should contain allOf("string", "integer", "long", "double", "boolean", "null")

      schema.printTreeString()
    }
  }

  it should "be inferred from rdd with complex fields" in {
    withEmbedMongoFixture(complexFieldAndType1) { mongodProc =>
      val mongodbRDD = new MongodbRDD(TestSQLContext, Config(List(host + ":" + port), database, collection))
      val schema = MongodbSchema(mongodbRDD, 1.0).schema()

      schema.fields should have size 12

      schema.printTreeString()
    }
  }

  it should "resolve type conflicts between fields" in {
    withEmbedMongoFixture(primitiveFieldValueTypeConflict) { mongodProc =>
      val mongodbRDD = new MongodbRDD(TestSQLContext, Config(List(host + ":" + port), database, collection))
      val schema = MongodbSchema(mongodbRDD, 1.0).schema()

      schema.fields should have size 7

      schema.printTreeString()
    }
  }

  it should "be inferred from rdd with more complex fields" in {
    withEmbedMongoFixture(complexFieldAndType2) { mongodProc =>
      val mongodbRDD = new MongodbRDD(TestSQLContext, Config(List(host + ":" + port), database, collection))
      val schema = MongodbSchema(mongodbRDD, 1.0).schema()

      schema.fields should have size 5

      schema.printTreeString()
    }
  }
}
