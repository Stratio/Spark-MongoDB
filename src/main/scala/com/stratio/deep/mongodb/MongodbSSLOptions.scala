package com.stratio.deep.mongodb

/**
 * Created by pmadrigal on 20/05/15.
 */
case class MongodbSSLOptions(
keyStore: Option[String] = None,
keyStorePassword: Option[String] = None,
trustStore: String,
trustStorePassword: Option[String] = None
)
{
  implicit def stringToOption(parameter : String): Option[String]= Some(parameter)

}
