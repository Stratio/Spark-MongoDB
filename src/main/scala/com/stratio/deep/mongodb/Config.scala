package com.stratio.deep.mongodb

/**
 * Created by jsantos on 3/02/15.
 */
case class Config(
  host: String,
  database: String,
  collection: String,
  samplingRatio: Double = 1.0)
