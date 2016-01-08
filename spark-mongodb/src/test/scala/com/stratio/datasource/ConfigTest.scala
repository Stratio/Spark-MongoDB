/**
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
package com.stratio.datasource

import com.stratio.datasource.Config.Property
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpec, Matchers}

@RunWith(classOf[JUnitRunner])
class ConfigTest extends FlatSpec
with Matchers
with ConfigHelpers
with MongodbTestConstants{

  behavior of "ConfigBuilder"

  it should "config a builder with any kind of property types" + scalaBinaryVersion in {

    val b = (Builder() /: desiredProps.toList){
      case (builder,(property,propValue)) => builder.set(property,propValue)
    }

    b.properties.toList.diff(desiredProps.toList) should equal(Nil)

  }

  it should "build a deep config with configured properties" + scalaBinaryVersion in {

    val b = (Builder() /: desiredProps.toList){
      case (builder,(property,propValue)) => builder.set(property,propValue)
    }

    b.build().properties.toList.diff(desiredProps.toList) should equal(Nil)

  }

  it should "fail at getting any property with the wrong expected type" + scalaBinaryVersion in {

    val config = Builder().set("prop1",1).set("prop2",new { val x = 1}).build()

    a [ClassCastException] should be thrownBy {
      config[Int]("prop2")
    }

  }

  it should "fail at building time if any required property is not defined" + scalaBinaryVersion in {

    a [IllegalArgumentException] should be thrownBy {
      Builder()
        .set("prop1",1)
        .build()
    }

  }

}
trait ConfigHelpers {

  case class Builder(
    override val properties: Map[Property,Any]=Map()) extends ConfigBuilder[Builder]{
    val requiredProperties: List[Property] = List("prop1","prop2")
    def apply(props: Map[Property, Any]): Builder =
      new Builder(props)
  }

  //  sample values

  val desiredProps = Map(
    "prop1" ->1,
    "prop2" -> "hi",
    "prop3" -> 1.0d,
    "prop4" -> new { val x = 5 })

}