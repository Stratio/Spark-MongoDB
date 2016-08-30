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

import java.sql.{Date, Timestamp}

import org.apache.spark.sql.types._

/**
 * Json - Scala object transformation support.
 * Used to convert from DBObjects to Spark SQL Row field types.
 * Disclaimer: As explained in NOTICE.md, some of this product includes
 * software developed by The Apache Software Foundation (http://www.apache.org/).
 */
trait JsonSupport {

  /**
   * Tries to convert some scala value to another compatible given type
   * @param value Value to be converted
   * @param desiredType Destiny type
   * @return Converted value
   */


  protected def enforceCorrectType(value: Any, desiredType: DataType): Any =
    Option(value).map{ _ => desiredType match {
      case StringType => toString(value)
      case _ if value == "" => null // guard the non string type
      case ByteType => toByte(value)
      case BinaryType => toBinary(value)
      case ShortType => toShort(value)
      case IntegerType => toInt(value)
      case LongType => toLong(value)
      case DoubleType => toDouble(value)
      case DecimalType() => toDecimal(value)
      case FloatType => toFloat(value)
      case BooleanType => value.asInstanceOf[Boolean]
      case DateType => toDate(value)
      case TimestampType => toTimestamp(value)
      case NullType => null
      case _ =>
        sys.error(s"Unsupported datatype conversion [Value: ${value}] of ${value.getClass}] to ${desiredType}]")
        value
    }
    }.getOrElse(null)

  private def toBinary(value: Any): Array[Byte] = {
    value match {
      case value: org.bson.types.Binary => value.getData
      case value: Array[Byte] => value
    }
  }

  private def toByte(value: Any): Byte = {
    value match {
      case value: java.lang.Integer => value.byteValue()
      case value: java.lang.Long => value.byteValue()
    }
  }

  private def toShort(value: Any): Short = {
    value match {
      case value: java.lang.Integer => value.toShort
      case value: java.lang.Long => value.toShort
    }
  }

  private def toInt(value: Any): Int = {
    import scala.language.reflectiveCalls
    value match {
      case value: String => value.toInt
      case _ => value.asInstanceOf[ {def toInt: Int}].toInt
    }
  }

  private def toLong(value: Any): Long = {
    value match {
      case value: java.lang.Integer => value.asInstanceOf[Int].toLong
      case value: java.lang.Long => value.asInstanceOf[Long]
      case value: java.lang.Double => value.asInstanceOf[Double].toLong
    }
  }

  private def toDouble(value: Any): Double = {
    value match {
      case value: java.lang.Integer => value.asInstanceOf[Int].toDouble
      case value: java.lang.Long => value.asInstanceOf[Long].toDouble
      case value: java.lang.Double => value.asInstanceOf[Double]
    }
  }

  private def toDecimal(value: Any): java.math.BigDecimal = {
    value match {
      case value: java.lang.Integer => new java.math.BigDecimal(value)
      case value: java.lang.Long => new java.math.BigDecimal(value)
      case value: java.lang.Double => new java.math.BigDecimal(value)
      case value: java.math.BigInteger => new java.math.BigDecimal(value)
      case value: java.math.BigDecimal => value
    }
  }

  private def toFloat(value: Any): Float = {
    value match {
      case value: java.lang.Integer => value.toFloat
      case value: java.lang.Long => value.toFloat
      case value: java.lang.Double => value.toFloat
    }
  }

  private def toTimestamp(value: Any): Timestamp = {
    value match {
      case value: java.util.Date => new Timestamp(value.getTime)
    }
  }

  private def toDate(value: Any): Date = {
    value match {
      case value: java.util.Date => new Date(value.getTime)
      // TODO Parse string to date when a String type arrives
      // case value: java.lang.String => ???
    }
  }

  private def toJsonArrayString(seq: Seq[Any]): String = {
    val builder = new StringBuilder
    builder.append("[")
    var count = 0
    seq.foreach {
      element =>
        if (count > 0) builder.append(",")
        count += 1
        builder.append(toString(element))
    }
    builder.append("]")

    builder.toString()
  }

  private def toJsonObjectString(map: Map[String, Any]): String = {
    val builder = new StringBuilder
    builder.append("{")
    var count = 0
    map.foreach {
      case (key, value) =>
        if (count > 0) builder.append(",")
        count += 1
        val stringValue = if (value.isInstanceOf[String]) s"""\"$value\"""" else toString(value)
        builder.append(s"""\"$key\":$stringValue""")
    }
    builder.append("}")

    builder.toString()
  }

  private def toString(value: Any): String = {
    value match {
      case value: Map[_, _] => toJsonObjectString(value.asInstanceOf[Map[String, Any]])
      case value: Seq[_] => toJsonArrayString(value)
      case v => Option(v).map(_.toString).orNull
    }
  }

}