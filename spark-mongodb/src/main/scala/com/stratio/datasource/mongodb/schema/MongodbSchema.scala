/*
 * Copyright (C) 2015 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.datasource.mongodb.schema

import com.mongodb.casbah.Imports._
import com.stratio.datasource.schema.SchemaProvider
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.analysis.TypeCoercion
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * A custom RDD schema for MongoDB.
 * @param rdd RDD used to infer the schema
 * @param samplingRatio Sampling ratio used to scan the RDD and extract
 *                      used fields.
 */
case class MongodbSchema[T <: RDD[DBObject]](
  rdd: T,
  samplingRatio: Double) extends SchemaProvider with Serializable {

  override def schema(): StructType = {
    val schemaData =
      if (samplingRatio > 0.99) rdd
      else rdd.sample(withReplacement = false, samplingRatio, 1)

    val structFields = schemaData.flatMap {
      dbo => {
        val doc: Map[String, AnyRef] = dbo.seq.toMap
        val fields = doc.mapValues(f => convertToStruct(f))
        fields
      }
    }.reduceByKey(compatibleType).aggregate(Seq[StructField]())({
      case (fields, (name, tpe)) =>
        val newType = tpe match {
          case ArrayType(NullType, containsNull) => ArrayType(StringType, containsNull)
          case other => other
        }
        fields :+ StructField(name, newType)
    }, (oldFields, newFields) => oldFields ++ newFields)
    StructType(structFields)
  }

  private def convertToStruct(dataType: Any): DataType = dataType match {
    case bl: BasicDBList =>
      typeOfArray(bl)

    case bo: DBObject =>
      val fields = bo.map {
        case (k, v) =>
          StructField(k, convertToStruct(v))
      }.toSeq
      StructType(fields)

    case elem =>
      elemType(elem)

  }

  /**
   * It looks for the most compatible type between two given DataTypes.
   * i.e.: {{{
   *   val dataType1 = IntegerType
   *   val dataType2 = DoubleType
   *   assert(compatibleType(dataType1,dataType2)==DoubleType)
   * }}}
   * @param t1 First DataType to compare
   * @param t2 Second DataType to compare
   * @return Compatible type for both t1 and t2
   */
  private def compatibleType(t1: DataType, t2: DataType): DataType = {
    TypeCoercion.findTightestCommonTypeOfTwo(t1, t2) match {
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
  
  private def elemType: PartialFunction[Any, DataType] = {
    case obj: Boolean => BooleanType
    case obj: Array[Byte] => BinaryType
    case obj: String => StringType
    case obj: UTF8String => StringType
    case obj: Byte => ByteType
    case obj: Short => ShortType
    case obj: Int => IntegerType
    case obj: Long => LongType
    case obj: Float => FloatType
    case obj: Double => DoubleType
    case obj: java.sql.Date => DateType
    case obj: java.math.BigDecimal => DecimalType.SYSTEM_DEFAULT
    case obj: Decimal => DecimalType.SYSTEM_DEFAULT
    case obj: java.sql.Timestamp => TimestampType
    case null => NullType
    case date: java.util.Date => TimestampType
    case _ => StringType
  }
}
