package com.stratio.provider.mongodb

case class MongodbCredentials(
  user: String,
  database: String,
  password: Array[Char])
