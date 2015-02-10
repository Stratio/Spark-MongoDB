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

package com.stratio.deep

import com.stratio.deep.DeepConfig.Property

import scala.reflect.ClassTag

/**
 * Created by jsantos on 3/02/15.
 */
abstract class DeepConfigBuilder[Builder<:DeepConfigBuilder[Builder] ](
  val properties: Map[Property,Any] = Map()) extends Serializable { builder =>

  val requiredProperties: List[Property]

  def apply(props: Map[Property,Any]): Builder

  def set[T](property: Property,value: T): Builder =
    apply(properties + (property -> value))

  def build(): DeepConfig = new DeepConfig {
    val properties = builder.properties
    require(
      requiredProperties.forall(properties.isDefinedAt),
      s"Not all properties are defined! : ${
        requiredProperties.diff(
          properties.keys.toList.intersect(requiredProperties))
      }")
  }

}

trait DeepConfig extends Serializable{

  val properties: Map[Property,Any]

  def get[T:ClassTag](property: Property): Option[T] =
    properties.get(property).map(_.asInstanceOf[T])

  def apply[T:ClassTag](property: Property): T =
    get[T](property).get

}

object DeepConfig {

  type Property = String

  /** Defines how to act in case any parameter is not set */
  def notFound[T](key: String): T =
    throw new IllegalStateException(s"Parameter $key not specified")

}
