/*
 *  Licensed to STRATIO (C) under one or more contributor license agreements.
 *  See the NOTICE file distributed with this work for additional information
 *  regarding copyright ownership. The STRATIO (C) licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied. See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package com.stratio.provider

import com.stratio.provider.Config.Property

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
   * Required properties to build a Deep config object.
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
  def set[T](property: Property,value: T): Builder =
    apply(properties + (property -> value))

  /**
   * Build the config object from current builder properties.
   *
   * @return The Deep configuration object.
   */
  def build(): Config = new Config {

    val properties = builder.properties

    require(
      requiredProperties.forall(properties.isDefinedAt),
      s"Not all properties are defined! : ${
        requiredProperties.diff(
          properties.keys.toList.intersect(requiredProperties))
      }")

  }

}

/**
 * Deep standard configuration object
 */
trait Config extends Serializable {

  /**
   * Contained properties in configuration object
   */
  val properties: Map[Property, Any]

  /**
   * Gets specified property from current configuration object
   * @param property Desired property
   * @tparam T Property expected value type.
   * @return An optional value of expected type
   */
  def get[T: ClassTag](property: Property): Option[T] =
    properties.get(property).map(_.asInstanceOf[T])

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
