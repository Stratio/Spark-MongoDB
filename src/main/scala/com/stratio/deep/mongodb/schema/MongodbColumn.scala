package com.stratio.deep.mongodb.schema

import java.io.IOException

import com.mongodb.Bytes
import com.stratio.deep.schema.DeepColumn
import org.apache.spark.sql.DataType
import org.apache.spark.sql.catalyst.types._
import org.bson.{BasicBSONObject, BSON}
import org.bson.types.BasicBSONList
import scala.collection.JavaConverters._

/**
 * Created by rmorandeira on 30/01/15.
 */
class MongodbColumn(val field: String, val dataType: DataType)
  extends DeepColumn with Serializable {

  override def toSqlVal(value: Any): Any = {
    val convertedVal = dataType match {
      case BooleanType => value.asInstanceOf[Boolean]
      case ShortType => value.asInstanceOf[Short]
      case IntegerType => value.asInstanceOf[Long]
      case FloatType => value.asInstanceOf[Float]
      case DoubleType => value.asInstanceOf[Double]
      case LongType => value.asInstanceOf[Long]
      case StringType => value.asInstanceOf[String]
      case ArrayType(_, _) =>
        value.asInstanceOf[BasicBSONList].asScala.map {
          _.toString
        }.toSeq
      case StructType(_) => value.asInstanceOf[Map[String, Any]].map { case (k, v) => toSqlVal(v) }
      case _ => throw new IOException("Unsupported data type")
    }
    convertedVal
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
    case bl: BasicBSONList => {
      val iter = bl.iterator()
      ArrayType(convertToStruct(iter.next()))
    }

    case bo: BasicBSONObject => {
      val doc = scala.collection.JavaConversions.mapAsScalaMap(bo)
      val fields = doc.map {
        case (k, v) =>
          StructField(k, convertToStruct(v))
      }.toSeq
      StructType(fields)
    }

    case x => Bytes.getType(dataType) match {
      case BSON.NULL => NullType
      case BSON.BINARY => BinaryType
      case BSON.BOOLEAN => BooleanType
      case BSON.NUMBER_INT => IntegerType
      case BSON.NUMBER_LONG => LongType
      case BSON.NUMBER => DoubleType
      case BSON.STRING => StringType
      case BSON.DATE => TimestampType
      case BSON.TIMESTAMP => TimestampType
      // fall back to String
      case _ => StringType
    }
  }
}