package com.stratio.deep.mongodb.schema

import com.mongodb.DBObject
import com.stratio.deep.schema.DeepRecord

/**
 * Created by rmorandeira on 30/01/15.
 */
class MongodbRecord (val schema: MongodbSchema, val rs: DBObject)
  extends DeepRecord with Serializable {

  override def values(): Array[Any] = {
    schema.columns.map {
      column =>
        rs.get(column.field) match {
          case a: Any => column.toSqlVal(a)
          case _ => null
        }
    }
  }
}

object MongodbRecord {
  def apply(schema: MongodbSchema, rs: DBObject) = {
    new MongodbRecord(schema, rs)
  }
}
