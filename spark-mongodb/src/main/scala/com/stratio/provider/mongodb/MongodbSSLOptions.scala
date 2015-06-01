package com.stratio.provider.mongodb

import scala.language.implicitConversions

/**
 * Case class with the SSL options.
 */
case class MongodbSSLOptions(
keyStore: Option[String] = None,
keyStorePassword: Option[String] = None,
trustStore: String,
trustStorePassword: Option[String] = None
)

object MongodbSSLOptions{
  implicit def stringToOption(parameter : String): Option[String]= Some(parameter)

}
