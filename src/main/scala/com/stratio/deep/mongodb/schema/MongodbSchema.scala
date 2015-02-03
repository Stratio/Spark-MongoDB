package com.stratio.deep.mongodb.schema

import com.mongodb.DBObject
import com.stratio.deep.schema.DeepSchema
import org.apache.spark.sql.StructType
import org.bson.BasicBSONObject

/**
 * Created by rmorandeira on 29/01/15.
 */

case class MongodbSchema(columns: Array[MongodbColumn]) extends DeepSchema with Serializable {
  override def toStructType(): StructType = {
    val fields = columns.map {
      column => column.toStructField()
    }
    StructType(fields)
  }
}


object MongodbSchema {

  def schema(json: DBObject): MongodbSchema = {
    import scala.collection.mutable
    import scala.collection.JavaConversions._
    val doc: mutable.Map[String, AnyRef] = json.asInstanceOf[BasicBSONObject]
    val columns = for ((k,v) <- doc) yield MongodbColumn(k,v)
    MongodbSchema(columns.toArray)
  }
}
