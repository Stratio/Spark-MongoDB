/**
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
import com.stratio.datasource.schema.RowConverter
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.{GenericRowWithSchema, GenericRow}
import org.apache.spark.sql.types._

import scala.collection.mutable.ArrayBuffer

/**
 * MongodbRowConverter support RDD transformations
 * from DBObject to Row and vice versa
 */
object MongodbRowConverter extends RowConverter[DBObject]
  with JsonSupport
  with Serializable {

  /**
   *
   * @param schema RDD native schema
   * @param rdd Current native RDD
   * @return A brand new RDD of Spark SQL Row type.
   */
  def asRow(schema: StructType, rdd: RDD[DBObject]): RDD[Row] = {
    rdd.map { record =>
      recordAsRow(dbObjectToMap(record), schema)
    }
  }

  /**
   *
   * @param schema native schema
   * @param array Current mongodb result
   * @return An array of Spark SQL Row type.
   */
  def asRow(schema: StructType, array: Array[DBObject]): Array[Row] = {
    array.map { record =>
      recordAsRow(dbObjectToMap(record), schema)
    }
  }

  /**
   * Given a schema, it converts a JSON object (as map) into a Row
   * @param json DBObject map
   * @param schema Schema
   * @return The converted row
   */
  def recordAsRow(
    json: Map[String, AnyRef],
    schema: StructType): Row = {

    val values: Seq[Any] = schema.fields.map {
      case StructField(name, et, _, mdata)
        if(mdata.contains("idx") && mdata.contains("colname")) =>
        val colName = mdata.getString("colname")
        val idx = mdata.getLong("idx").toInt
        json.get(colName).flatMap(v => Option(v)).map(toSQL(_, ArrayType(et, true))).collect {
          case elemsList: Seq[_] if((0 until elemsList.size) contains idx) => elemsList(idx)
        } orNull
      case StructField(name, dataType, _, _) =>
        json.get(name).flatMap(v => Option(v)).map(
          toSQL(_, dataType)).orNull
    }
    new GenericRowWithSchema(values.toArray, schema)
  }


  /**
   * Given a schema, it converts a Row into a DBObject
   * @param row Row to be converted
   * @param schema Schema
   * @return The converted DBObject
   */
  def rowAsDBObject(row: Row, schema: StructType): DBObject = {
    val attMap: Map[String, Any] = schema.fields.zipWithIndex.map {
      case (att, idx) => (att.name, toDBObject(row(idx),att.dataType))
    }.toMap
    attMap
  }

  /**
   * It converts some Row attribute value into
   * a DBObject field
   * @param value Row attribute
   * @param dataType Attribute type
   * @return The converted value into a DBObject field.
   */
  def toDBObject(value: Any, dataType: DataType): Any = {
    Option(value).map{v =>
      (dataType,v) match {
        case (ArrayType(elementType, _),array: ArrayBuffer[Any@unchecked]) =>
          val list: List[Any] = array.map{
            case obj => toDBObject(obj,elementType)
          }.toList
          list
        case (struct: StructType,value: GenericRow) =>
          rowAsDBObject(value,struct)
        case _ => v
      }
    }.orNull
  }

  /**
   * It converts some DBObject attribute value into
   * a Row field
   * @param value DBObject attribute
   * @param dataType Attribute type
   * @return The converted value into a Row field.
   */
  def toSQL(value: Any, dataType: DataType): Any = {
    import scala.collection.JavaConversions._
    Option(value).map{value =>
      (value,dataType) match {
        case (dbList: BasicDBList,ArrayType(elementType, _)) =>
          dbList.map(toSQL(_, elementType))
        case (list: List[AnyRef  @unchecked],ArrayType(elementType, _)) =>
          val dbList = new BasicDBList
          dbList.addAll(list)
          toSQL(dbList,dataType)
        case (_,struct: StructType) =>
          recordAsRow(dbObjectToMap(value.asInstanceOf[DBObject]), struct)
        case _ =>
          //Assure value is mapped to schema constrained type.
          enforceCorrectType(value, dataType)
      }
    }.orNull
  }

  /**
   * It creates a map with dbObject attribute values.
   * @param dBObject Object to be splitted into attribute tuples.
   * @return A map with dbObject attributes.
   */
  def dbObjectToMap(dBObject: DBObject): Map[String, AnyRef] = {
    dBObject.seq.toMap
  }

}
