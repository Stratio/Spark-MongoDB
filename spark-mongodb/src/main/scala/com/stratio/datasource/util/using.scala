/*
 * Copyright (C) 2015 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.datasource.util

import scala.language.reflectiveCalls
import scala.util.Try

/**
 * DSL helper for enclosing some functionality into a closeable type.
 * Helper will be responsible of closing the object.
 * i.e.:{{{
 *   import java.io._
 *   val writer = new PrintWriter(new File("test.txt" ))
 *   using(writer){ w =>
 *    w.append("hi!")
 *   }
 * }}}
 */
object using {

  type AutoClosable = { def close(): Unit }

  def apply[A <: AutoClosable, B](resource: A)(code: A => B): B =
    try {
      code(resource)
    }
    finally {
      Try(resource.close())
    }

}
