package com.stratio.deep.mongodb

case class MongodbCredentials(
  user: String,
  database: String,
  password: Array[Char])

