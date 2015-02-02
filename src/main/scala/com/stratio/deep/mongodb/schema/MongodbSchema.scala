package com.stratio.deep.mongodb.schema

import com.mongodb.DBObject
import com.stratio.deep.schema.DeepSchema
import org.apache.spark.sql.StructType
import org.bson.BasicBSONObject

/**
 * Created by rmorandeira on 29/01/15.
 */

class MongodbSchema(val columns: Array[MongodbColumn]) extends DeepSchema with Serializable {
  override def toStructType(): StructType = {
    val fields = columns.map {
      column => column.toStructField()
    }
    StructType(fields)
  }
}


object MongodbSchema {

  def schema(json: DBObject): MongodbSchema = {
    val doc = scala.collection.JavaConversions.mapAsScalaMap(json.asInstanceOf[BasicBSONObject])
    val columns = for (k <- doc) yield (MongodbColumn(k._1, k._2))
    new MongodbSchema(columns.toArray)
  }
}
