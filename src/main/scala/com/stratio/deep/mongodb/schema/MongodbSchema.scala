package com.stratio.deep.mongodb.schema

import com.stratio.deep.mongodb.rdd.MongodbRDD
import com.stratio.deep.schema.DeepSchemaProvider
import org.apache.spark.SparkContext._
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.analysis.HiveTypeCoercion
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.{ArrayType, DataType}
import org.bson.BasicBSONObject
import org.bson.types.BasicBSONList

import scala.collection.JavaConverters._

/**
 * Created by rmorandeira on 29/01/15.
 */

case class MongodbSchema(
  rdd: MongodbRDD,
  samplingRatio: Double) extends DeepSchemaProvider with Serializable {

  override def schema(): StructType = {
    val schemaData =
      if (samplingRatio > 0.99) rdd
      else rdd.sample(withReplacement = false, samplingRatio, 1)

    val structFields = schemaData.flatMap {
      dbo => {
        val doc: Map[String, AnyRef] = dbo.asInstanceOf[BasicBSONObject].asScala.toMap
        val fields = doc.mapValues(f => convertToStruct(f))
        fields
      }
    }.reduceByKey(compatibleType).aggregate(Seq[StructField]())(
        (fields, newField) => fields :+ StructField(newField._1, newField._2),
        (oldFields, newFields) => oldFields ++ newFields)
    StructType(structFields)
  }

  private def convertToStruct(dataType: Any): DataType = dataType match {
    case bl: BasicBSONList =>
      typeOfArray(bl.asScala)

    case bo: BasicBSONObject => {
      val fields = bo.asScala.map {
        case (k, v) =>
          StructField(k, convertToStruct(v))
      }.toSeq
      StructType(fields)
    }

    case elem: Any => {
      val elemType: PartialFunction[Any, DataType] =
        ScalaReflection.typeOfObject.orElse { case _ => StringType}
      elemType(elem)
    }
  }

  private def compatibleType(t1: DataType, t2: DataType): DataType = {
    HiveTypeCoercion.findTightestCommonType(t1, t2) match {
      case Some(commonType) => commonType

      case None =>
        // t1 or t2 is a StructType, ArrayType, or an unexpected type.
        (t1, t2) match {
          case (other: DataType, NullType) => other
          case (NullType, other: DataType) => other
          case (StructType(fields1), StructType(fields2)) =>
            val newFields = (fields1 ++ fields2)
              .groupBy(field => field.name)
              .map { case (name, fieldTypes) =>
              val dataType = fieldTypes
                .map(field => field.dataType)
                .reduce(compatibleType)
              StructField(name, dataType, nullable = true)

            }
            StructType(newFields.toSeq.sortBy(_.name))

          case (ArrayType(elementType1, containsNull1), ArrayType(elementType2, containsNull2)) =>
            ArrayType(
              compatibleType(elementType1, elementType2),
              containsNull1 || containsNull2)

          case (_, _) => StringType
        }
    }
  }

  private def typeOfArray(l: Seq[Any]): ArrayType = {
    val containsNull = l.contains(null)
    val elements = l.flatMap(v => Option(v))
    if (elements.isEmpty) {
      // If this JSON array is empty, we use NullType as a placeholder.
      // If this array is not empty in other JSON objects, we can resolve
      // the type after we have passed through all JSON objects.
      ArrayType(NullType, containsNull)
    } else {
      val elementType = elements
        .map(convertToStruct)
        .reduce(compatibleType)
      ArrayType(elementType, containsNull)
    }
  }
}
