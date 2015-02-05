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
