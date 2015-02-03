package com.stratio.deep.mongodb.schema

import com.mongodb.DBObject
import com.stratio.deep.schema.DeepRowConverter
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.expressions.GenericMutableRow
import org.apache.spark.sql.catalyst.types.{ArrayType, DataType, StructField}
import org.apache.spark.sql.{Row, StructType}
import org.bson.BasicBSONObject
import org.bson.types.BasicBSONList

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

/**
 * Created by rmorandeira on 3/02/15.
 */
class MongodbRowConverter extends DeepRowConverter[DBObject] with Serializable {
  override def asRow(schema: StructType, rdd: RDD[DBObject]): RDD[Row] = {
    rdd.map { record =>
      recordAsRow(dbObjectToMap(record), schema)
    }
  }

  private def recordAsRow(json: Map[String,AnyRef], schema: StructType): Row = {
    // TODO: Reuse the row instead of creating a new one for every record.
    val row = new GenericMutableRow(schema.fields.length)
    schema.fields.zipWithIndex.foreach {
      case (StructField(name, dataType, _, _), i) =>
        row.update(i, json.get(name).flatMap(v => Option(v)).map(
          toSQL(_, dataType)).orNull)
    }
    row
  }

  private def toSQL(value: Any, dataType: DataType): Any = {
    if (value == null) {
      null
    } else {
      dataType match {
        case ArrayType(elementType, _) => value.asInstanceOf[BasicBSONList].asScala.map(toSQL(_,
          elementType))
        case struct: StructType => recordAsRow(dbObjectToMap(value.asInstanceOf[DBObject]), struct)
        case _ => ScalaReflection.convertToScala(value, dataType)
      }
    }
  }

  private def dbObjectToMap(dBObject: DBObject): Map[String, AnyRef] = {
    mapAsScalaMap(dBObject.asInstanceOf[BasicBSONObject]).toMap
  }
}
