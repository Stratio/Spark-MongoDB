package com.stratio.deep.mongodb.schema

import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.catalyst.types.decimal.Decimal

trait JsonSupport {

  protected def enforceCorrectType(value: Any, desiredType: DataType): Any ={
    if (value == null) {
      null
    } else {
      desiredType match {
        case StringType => toString(value)
        case _ if value == null || value == "" => null // guard the non string type
        case IntegerType => toInt(value)
        case LongType => toLong(value)
        case DoubleType => toDouble(value)
        case DecimalType() => toDecimal(value)
        case BooleanType => value.asInstanceOf[Boolean]
        case NullType => null
        case _ =>
          sys.error(s"Unsupported datatype conversion [${value.getClass}},$desiredType]")
          value
      }
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
    }
  }

  private def toDouble(value: Any): Double = {
    value match {
      case value: java.lang.Integer => value.asInstanceOf[Int].toDouble
      case value: java.lang.Long => value.asInstanceOf[Long].toDouble
      case value: java.lang.Double => value.asInstanceOf[Double]
    }
  }

  private def toDecimal(value: Any): Decimal = {
    value match {
      case value: java.lang.Integer => Decimal(value)
      case value: java.lang.Long => Decimal(value)
      case value: java.math.BigInteger => Decimal(new java.math.BigDecimal(value))
      case value: java.lang.Double => Decimal(value)
      case value: java.math.BigDecimal => Decimal(value)
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
