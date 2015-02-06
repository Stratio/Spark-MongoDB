package com.stratio.deep.mongodb.schema

import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.catalyst.expressions.{GenericRow, Row}
import org.apache.spark.sql.catalyst.types.StructType
import org.scalatest.{Matchers, FlatSpec }
import com.stratio.deep.DeepConfig
import com.stratio.deep.mongodb.MongodbConfig
import MongodbRowConverter._

import scala.collection.mutable.ArrayBuffer


/**
 * Created by jsantos on 6/02/15.
 */
class MongodbRowConverterSpec extends FlatSpec
with Matchers
with MongoEmbedDatabase
with TestBsonData {

  private val host: String = "localhost"
  private val port: Int = 12345
  private val database: String = "testDb"
  private val collection: String = "testCol"

  val testConfig = DeepConfig()
    .set(MongodbConfig.Host,List(host + ":" + port))
    .set(MongodbConfig.Database,database)
    .set(MongodbConfig.Collection,collection)
    .set(MongodbConfig.SamplingRatio,1.0)

  behavior of "The MongodbRowConverter"

  ignore should "be able to convert any value from a row into a dbobject field" in{
    val valueWithType: Map[Any, StructField] = Map(
      1 -> new StructField(
        "att1",IntegerType,false),
      2.0 -> new StructField(
        "att2",DoubleType,false),
      "hi" -> new StructField(
        "att3",IntegerType,false),
      null.asInstanceOf[Any] -> new StructField(
        "att1",StringType,true),
      new ArrayBuffer[Int]().+=(1).+=(2).+=(3) -> new StructField(
        "att1",new ArrayType(IntegerType,false),false),
      new GenericRow(List(1,null).toArray) -> new StructField(
        "att6",new StructType(List(
          new StructField("att61",IntegerType ,false),
          new StructField("att62",IntegerType,true)
        )),false))

    //toDBObject(valueWithType.keys,new StructType(valueWithType.values.toList))
    //TODO

  }

  ignore should "be able to convert any value from a dbobject field  into a row field" in{
    ()//TODO
  }

}
