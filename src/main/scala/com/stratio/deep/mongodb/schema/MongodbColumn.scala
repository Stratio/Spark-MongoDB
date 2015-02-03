package com.stratio.deep.mongodb.schema

import com.stratio.deep.schema.DeepColumn
import org.apache.spark.sql.DataType
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.analysis.HiveTypeCoercion
import org.apache.spark.sql.catalyst.types._
import org.bson.BasicBSONObject
import org.bson.types.BasicBSONList

import scala.collection.JavaConverters._

/**
 * Created by rmorandeira on 30/01/15.
 */
class MongodbColumn(val field: String, val dataType: DataType)
  extends DeepColumn with Serializable {

  override def toSqlVal(value: Any): Any = {

    ScalaReflection.convertToScala(value, dataType)
//    val convertedVal = dataType match {
//      case BooleanType => value.asInstanceOf[Boolean]
//      case ShortType => value.asInstanceOf[Short]
//      case IntegerType => value.asInstanceOf[Long]
//      case FloatType => value.asInstanceOf[Float]
//      case DoubleType => value.asInstanceOf[Double]
//      case LongType => value.asInstanceOf[Long]
//      case StringType => value.asInstanceOf[String]
//      case ArrayType(_, _) =>
//        value.asInstanceOf[BasicBSONList].asScala.map {
//          _.toString
//        }.toSeq
//      case StructType(_) => value.asInstanceOf[Map[String, Any]].map { case (k, v) => toSqlVal(v) }
//      case _ => throw new IOException("Unsupported data type")
//    }
//    convertedVal
  }

  override def toStructField(): StructField = {
    StructField(field, dataType)
  }
}

object MongodbColumn  {
  def apply(field: String, dataType: Any) = {
    new MongodbColumn(field, convertToStruct(dataType))
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
      val elemType: PartialFunction[Any, DataType] = ScalaReflection.typeOfObject.orElse{case _ => StringType}
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
          case (StructType(fields1), StructType(fields2)) => {
            val newFields = (fields1 ++ fields2).groupBy(field => field.name).map {
              case (name, fieldTypes) => {
                val dataType = fieldTypes.map(field => field.dataType).reduce(
                  (type1: DataType, type2: DataType) => compatibleType(type1, type2))
                StructField(name, dataType, true)
              }
            }
            StructType(newFields.toSeq.sortBy(_.name))
          }
          case (ArrayType(elementType1, containsNull1), ArrayType(elementType2, containsNull2)) =>
            ArrayType(compatibleType(elementType1, elementType2), containsNull1 || containsNull2)
          case (_, _) => StringType
        }
    }
  }

  private def typeOfArray(l: Seq[Any]): ArrayType = {
    val containsNull = l.exists(v => v == null)
    val elements = l.flatMap(v => Option(v))
    if (elements.isEmpty) {
      // If this JSON array is empty, we use NullType as a placeholder.
      // If this array is not empty in other JSON objects, we can resolve
      // the type after we have passed through all JSON objects.
      ArrayType(NullType, containsNull)
    } else {
      val elementType = elements.map(convertToStruct _)
        .reduce((type1: DataType, type2: DataType) => compatibleType(type1, type2))
      ArrayType(elementType, containsNull)
    }
  }
}