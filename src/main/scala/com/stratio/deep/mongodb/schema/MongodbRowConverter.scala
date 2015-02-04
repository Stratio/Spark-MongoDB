package com.stratio.deep.mongodb.schema

import com.mongodb.{BasicDBObject, DBObject}
import com.stratio.deep.schema.DeepRowConverter
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.StructType
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.types.{StructType, ArrayType, DataType, StructField}
import org.apache.spark.sql.{Row, StructType}
import org.bson.BasicBSONObject
import org.bson.types.BasicBSONList

import scala.collection.JavaConverters._

/**
 * Created by rmorandeira on 3/02/15.
 */
object MongodbRowConverter extends DeepRowConverter[DBObject] with Serializable {

  override def asRow(schema: StructType, rdd: RDD[DBObject]): RDD[Row] = {
    rdd.map { record =>
      recordAsRow(dbObjectToMap(record), schema)
    }
  }

  private def recordAsRow(
    json: Map[String, AnyRef],
    schema: StructType): Row = {
    val values: Seq[Any] = schema.fields.map {
      case StructField(name, dataType, _, _) =>
        json.get(name).flatMap(v => Option(v)).map(
          toSQL(_, dataType)).orNull
    }
    Row.fromSeq(values)
  }

  def rowAsDBObject(row: Row, schema: StructType): DBObject = {
    import scala.collection.JavaConversions._
    val attMap: Map[String, Any] = schema.fieldNames.zipWithIndex.map {
      case (att, idx) => (att, row(idx))
    }.toMap
    new BasicDBObject(attMap)
  }

  private def toSQL(value: Any, dataType: DataType): Any = {
    if (value == null) {
      null
    } else {
      dataType match {
        case ArrayType(elementType, _) =>
          value.asInstanceOf[BasicBSONList].asScala.map(toSQL(_, elementType))
        case struct: StructType =>
          recordAsRow(dbObjectToMap(value.asInstanceOf[DBObject]), struct)
        case _ =>
          ScalaReflection.convertToScala(value, dataType)
      }
    }
  }

  private def dbObjectToMap(dBObject: DBObject): Map[String, AnyRef] = {
    dBObject.asInstanceOf[BasicBSONObject].asScala.toMap
  }
}
