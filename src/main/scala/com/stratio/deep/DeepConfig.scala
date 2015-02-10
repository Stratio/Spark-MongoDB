/*
 *
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
 * /
 */

package com.stratio.deep

import scala.reflect.ClassTag

/**
 * Created by jsantos on 3/02/15.
 */
case class DeepConfig(
  properties: Map[String,Any] = Map()){

  def set[T](property: String,value: T): DeepConfig =
    DeepConfig(properties + (property -> value))

  def get[T:ClassTag](property: String): Option[T] =
    properties.get(property).map(_.asInstanceOf[T])

  def apply[T:ClassTag](property: String): T =
    get[T](property).get
}

object DeepConfig extends Serializable {

  /** Defines how to act in case any parameter is not set */
  def notFound(key: String) = sys.error(s"Parameter $key not specified")

}
