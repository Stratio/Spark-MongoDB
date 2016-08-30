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
package com.stratio.datasource.util

import com.stratio.datasource.util.Config.Property

import scala.reflect.ClassTag

/**
 * Abstract config builder, used to set a bunch of properties a build
 * a config object from them.
 *
 * @param properties Map of any-type properties.
 * @tparam Builder Current Builder implementation type.
 */
abstract class ConfigBuilder[Builder<:ConfigBuilder[Builder] ](
  val properties: Map[Property,Any] = Map()) extends Serializable { builder =>

  /**
   * Required properties to build a Mongo config object.
   * At build time, if these properties are not set, an assert
   * exception will be thrown.
   */
  val requiredProperties: List[Property]

  /**
   * Instantiate a brand new Builder from given properties map
   *
   * @param props Map of any-type properties.
   * @return The new builder
   */
  def apply(props: Map[Property,Any]): Builder

  /**
   * Set (override if exists) a single property value given a new one.
   *
   * @param property Property to be set
   * @param value New value for given property
   * @tparam T Property type
   * @return A new builder that includes new value of the specified property
   */
  def set[T](property: Property, value: T): Builder =
    apply(properties + (property -> value))

  /**
   * Build the config object from current builder properties.
   *
   * @return The Mongo configuration object.
   */
  def build(): Config = new Config {

    // TODO Review when refactoring config
    val properties = builder.properties.map { case (k, v) => k.toLowerCase -> v }
    val reqProperties = requiredProperties.map(_.toLowerCase)

    require(
      reqProperties.forall(properties.isDefinedAt),
      s"Not all properties are defined! : ${
        reqProperties.diff(
          properties.keys.toList.intersect(requiredProperties))
      }")

    /**
     * Compare if two Configs have the same properties.
     * @param other Object to compare
     * @return Boolean
     */
    override def equals(other: Any): Boolean = other match {
      case that: Config =>
        properties == that.properties
      case _ => false
    }

    override def hashCode(): Int = {
      val state = Seq(properties)
      state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
    }

  }

}

/**
 * Mongo standard configuration object
 */
trait Config extends Serializable {

  /**
   * Contained properties in configuration object
   */
  val properties: Map[Property, Any]

  /**  Returns the value associated with a key, or a default value if the key is not contained in the configuration object.

   *   @param   key Desired property.
   *   @param   default Value in case no binding for `key` is found in the map.
   *   @tparam  T Result type of the default computation.
   *   @return  the value associated with `key` if it exists,
   *            otherwise the result of the `default` computation.
   */
  def getOrElse[T](key: Property, default: => T): T =
    properties.get(key.toLowerCase) collect { case v: T => v } getOrElse default

  /**
   * Gets specified property from current configuration object
   * @param property Desired property
   * @tparam T Property expected value type.
   * @return An optional value of expected type
   */
  def get[T: ClassTag](property: Property): Option[T] =
    properties.get(property.toLowerCase).map(_.asInstanceOf[T])

  /**
   * Gets specified property from current configuration object.
   * It will fail if property is not previously set.
   * @param property Desired property
   * @tparam T Property expected value type
   * @return Expected type value
   */
  def apply[T: ClassTag](property: Property): T = {
    get[T](property).get
  }

}

object Config {

  type Property = String

  /**
   * Defines how to act in case any parameter is not set
   * @param key Key that couldn't be obtained
   * @tparam T Expected type (used to fit in 'getOrElse' cases).
   * @return Throws an IllegalStateException.
   */
  def notFound[T](key: String): T =
    throw new IllegalStateException(s"Parameter $key not specified")

}
