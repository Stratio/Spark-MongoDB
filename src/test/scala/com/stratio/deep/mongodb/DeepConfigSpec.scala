package com.stratio.deep.mongodb

import com.stratio.deep.DeepConfig.Property
import com.stratio.deep.DeepConfigBuilder
import org.scalatest.{Matchers, FlatSpec}

/**
 * Created by jsantos on 10/02/15.
 */
class DeepConfigSpec extends FlatSpec
with Matchers
with ConfigHelpers {

  behavior of "DeepConfigBuilder"

  it should "config a builder with any kind of property types" in {

    val b = (Builder() /: desiredProps.toList){
      case (builder,(property,propValue)) => builder.set(property,propValue)
    }

    b.properties.toList.diff(desiredProps.toList) should equal(Nil)

  }

  it should "build a deep config with configured properties" in {

    val b = (Builder() /: desiredProps.toList){
      case (builder,(property,propValue)) => builder.set(property,propValue)
    }

    b.build().properties.toList.diff(desiredProps.toList) should equal(Nil)

  }

  it should "fail at getting any property with the wrong expected type" in {

    val config = Builder().set("prop1",1).set("prop2",new { val x = 1}).build()

    a [ClassCastException] should be thrownBy {
      config[Int]("prop2")
    }

  }

  it should "fail at building time if any required property is not defined" in {

    a [IllegalArgumentException] should be thrownBy {
      Builder()
        .set("prop1",1)
        .build()
    }

  }

}
trait ConfigHelpers {

  case class Builder(override val properties: Map[Property,Any]=Map()) extends DeepConfigBuilder[Builder]{
    override val requiredProperties: List[Property] = List("prop1","prop2")
    override def apply(props: Map[Property, Any]): Builder =
      new Builder(props)
  }

  //  sample values

  val desiredProps = Map(
    "prop1" ->1,
    "prop2" -> "hi",
    "prop3" -> 1.0d,
    "prop4" -> new { val x = 5 })

}