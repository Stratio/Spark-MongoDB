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
package com.stratio.datasource.mongodb.query

import java.util.regex.Pattern

import com.mongodb.QueryBuilder
import com.mongodb.casbah.Imports
import com.mongodb.casbah.Imports._
import com.stratio.datasource.mongodb.sources.{NearSphere, Near}
import org.apache.spark.sql.sources._
import com.stratio.datasource.Config
import com.stratio.datasource.mongodb.MongodbConfig

object FilterSection {

  /**
    * Implicit conversion to pass from an array of [[Filter]] to [[FilterSection]] filter description object.
    *
    * @param sFilters
    * @param config
    * @return [[FilterSection]] built from `sFilters`
    */
  implicit def srcFilArr2filSel(sFilters: Array[Filter])(implicit config: Config): FilterSection =
    new SourceFilters(sFilters)

  //Factory methods

  def apply(sFilters: Array[Filter])(implicit config: Config): FilterSection =
    srcFilArr2filSel(sFilters)

  def apply(): FilterSection = NoFilters
}

/**
  * Trait to be implemented to those classes describing the Filter section of a MongoDB query.
  */
trait FilterSection {
  /**
    * @return a [[DBObject]] describing the filters to apply to a partition.
    */
  def filtersToDBObject(): DBObject
}

/**
  * Filter described by a [[DBObject]] as it is used by Casbah (https://mongodb.github.io/casbah/)
  * @param filterDesc
  */
case class RawFilter(filterDesc: DBObject) extends FilterSection {
  override def filtersToDBObject(): Imports.DBObject = filterDesc
}

/**
  * No filter to be applied
  */
case object NoFilters extends FilterSection {
  override def filtersToDBObject(): Imports.DBObject = QueryBuilder.start.get()
}

/**
  * This [[FilterSection]] is described by an array of [[org.apache.spark.sql.sources.Filter]] where each
  * element is a restriction.
  *
  * @param sFilters All filters to be applied (AND)
  * @param parentFilterIsNot `true` iff the filter is negated: NOT (sFilters[0] AND ... AND sFilters[n-1])
  * @param config Access configuration
  */
case class SourceFilters(
                     sFilters: Array[Filter],
                     parentFilterIsNot: Boolean = false
                   )(implicit config: Config) extends FilterSection {

  override def filtersToDBObject: DBObject = {
    val queryBuilder: QueryBuilder = QueryBuilder.start

    if (parentFilterIsNot) queryBuilder.not()

    sFilters.foreach {
      case EqualTo(attribute, value) =>
        queryBuilder.put(attribute).is(checkObjectID(attribute, value))
      case GreaterThan(attribute, value) =>
        queryBuilder.put(attribute).greaterThan(checkObjectID(attribute, value))
      case GreaterThanOrEqual(attribute, value) =>
        queryBuilder.put(attribute).greaterThanEquals(checkObjectID(attribute, value))
      case In(attribute, values) =>
        queryBuilder.put(attribute).in(values.map(value => checkObjectID(attribute, value)))
      case LessThan(attribute, value) =>
        queryBuilder.put(attribute).lessThan(checkObjectID(attribute, value))
      case LessThanOrEqual(attribute, value) =>
        queryBuilder.put(attribute).lessThanEquals(checkObjectID(attribute, value))
      case IsNull(attribute) =>
        queryBuilder.put(attribute).is(null)
      case IsNotNull(attribute) =>
        queryBuilder.put(attribute).notEquals(null)
      case And(leftFilter, rightFilter) if !parentFilterIsNot =>
        queryBuilder.and(
          SourceFilters(Array(leftFilter)).filtersToDBObject(),
          SourceFilters(Array(rightFilter)).filtersToDBObject()
        )
      case Or(leftFilter, rightFilter)  if !parentFilterIsNot =>
        queryBuilder.or(
          SourceFilters(Array(leftFilter)).filtersToDBObject(),
          SourceFilters(Array(rightFilter)).filtersToDBObject()
        )
      case StringStartsWith(attribute, value) if !parentFilterIsNot =>
        queryBuilder.put(attribute).regex(Pattern.compile("^" + value + ".*$"))
      case StringEndsWith(attribute, value) if !parentFilterIsNot =>
        queryBuilder.put(attribute).regex(Pattern.compile("^.*" + value + "$"))
      case StringContains(attribute, value) if !parentFilterIsNot =>
        queryBuilder.put(attribute).regex(Pattern.compile(".*" + value + ".*"))
      case Near(attribute, x, y, None) =>
        queryBuilder.put(attribute).near(x, y)
      case Near(attribute, x, y, Some(max)) =>
        queryBuilder.put(attribute).near(x, y, max)
      case NearSphere(attribute, longitude, latitude, None) =>
        queryBuilder.put(attribute).nearSphere(longitude, latitude)
      case NearSphere(attribute, longitude, latitude, Some(maxDistance)) =>
        queryBuilder.put(attribute).nearSphere(longitude, latitude, maxDistance)
      case Not(filter) =>
        SourceFilters(Array(filter), true).filtersToDBObject()
    }

    queryBuilder.get
  }

  /**
    * Check if the field is "_id" and if the user wants to filter by this field as an ObjectId
    *
    * @param attribute Name of the file
    * @param value Value for the attribute
    * @return The value in the correct data type
    */
  private def checkObjectID(attribute: String, value: Any)(implicit config: Config) : Any = attribute  match {
    case "_id" if idAsObjectId => new ObjectId(value.toString)
    case _ => value
  }

  lazy val idAsObjectId: Boolean =
    config.getOrElse[String](MongodbConfig.IdAsObjectId, MongodbConfig.DefaultIdAsObjectId).equalsIgnoreCase("true")

}