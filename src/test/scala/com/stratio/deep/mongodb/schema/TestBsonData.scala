package com.stratio.deep.mongodb.schema

import com.mongodb.DBObject
import com.mongodb.util.JSON

/**
 * Created by rmorandeira on 4/02/15.
 */
trait TestBsonData {

  val primitiveFieldAndType =
    JSON.parse(
      """{"string":"this is a simple string.",
          "integer":10,
          "long":21474836470,
          "double":1.7976931348623157E308,
          "boolean":true,
          "null":null
      }""").asInstanceOf[DBObject] :: Nil

  val primitiveFieldValueTypeConflict =
    JSON.parse(
      """{"num_num_1":11, "num_num_2":null, "num_num_3": 1.1,
          "num_bool":true, "num_str":13.1, "str_bool":"str1"}""").asInstanceOf[DBObject] ::
      JSON.parse(
        """{"num_num_1":null, "num_num_2":21474836470.9, "num_num_3": null,
          "num_bool":12, "num_str":null, "str_bool":true}""" ).asInstanceOf[DBObject] ::
      JSON.parse(
        """{"num_num_1":21474836470, "num_num_2":9223372036854, "num_num_3": 100,
          "num_bool":false, "num_str":"str1", "str_bool":false}""" ).asInstanceOf[DBObject] ::
      JSON.parse(
        """{"num_num_1":21474836570, "num_num_2":1.1, "num_num_3": 21474836470,
          "num_bool":null, "num_str":9223372036854775807, "str_bool":null}""").asInstanceOf[DBObject] :: Nil

  val complexFieldAndType1 =
    JSON.parse(
      """{"struct":{"field1": true, "field2": 9223372036854775807},
          "structWithArrayFields":{"field1":[4, 5, 6], "field2":["str1", "str2"]},
          "arrayOfString":["str1", "str2"],
          "arrayOfInteger":[1, 2147483647, -2147483648],
          "arrayOfLong":[21474836470, 9223372036854775807, -9223372036854775808],
          "arrayOfDouble":[1.2, 1.7976931348623157E308, 4.9E-324, 2.2250738585072014E-308],
          "arrayOfBoolean":[true, false, true],
          "arrayOfNull":[null, null, null, null],
          "arrayOfStruct":[{"field1": true, "field2": "str1"}, {"field1": false}, {"field3": null}],
          "arrayOfArray1":[[1, 2, 3], ["str1", "str2"]],
          "arrayOfArray2":[[1, 2, 3], [1.1, 2.1, 3.1]]
      }""").asInstanceOf[DBObject] :: Nil

  val complexFieldAndType2 =
    JSON.parse(
      """{"arrayOfStruct":[{"field1": true, "field2": "str1"}, {"field1": false}, {"field3": null}],
          "complexArrayOfStruct": [
          {
            "field1": [
            {
              "inner1": "str1"
            },
            {
              "inner2": ["str2", "str22"]
            }],
            "field2": [[1, 2], [3, 4]]
          },
          {
            "field1": [
            {
              "inner2": ["str3", "str33"]
            },
            {
              "inner1": "str4"
            }],
            "field2": [[5, 6], [7, 8]]
          }],
          "arrayOfArray1": [
          [
            [5]
          ],
          [
            [6, 7],
            [8]
          ]],
          "arrayOfArray2": [
          [
            [
              {
                "inner1": "str1"
              }
            ]
          ],
          [
            [],
            [
              {"inner2": ["str3", "str33"]},
              {"inner2": ["str4"], "inner1": "str11"}
            ]
          ],
          [
            [
              {"inner3": [[{"inner4": 2}]]}
            ]
          ]]
      }""").asInstanceOf[DBObject] :: Nil
}
