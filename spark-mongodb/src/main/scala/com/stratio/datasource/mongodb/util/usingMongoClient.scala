package com.stratio.datasource.mongodb.util

import com.mongodb.casbah.MongoClient
import com.stratio.datasource.mongodb.client.MongodbClientFactory

import scala.util.Try

  object usingMongoClient {

    def apply[A](mongoClient: MongoClient)(code: MongoClient => A): A =
      try {
        code(mongoClient)
      } finally {
        Try(MongodbClientFactory.closeByClient(mongoClient))
      }
  }

