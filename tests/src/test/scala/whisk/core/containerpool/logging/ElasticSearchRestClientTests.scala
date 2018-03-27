/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package whisk.core.containerpool.logging

import spray.json._

import org.junit.runner.RunWith
import org.scalatest.{FlatSpecLike, Matchers}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.junit.JUnitRunner

import common.StreamLogging

import whisk.core.containerpool.logging.ElasticSearchJsonProtocol._

@RunWith(classOf[JUnitRunner])
class ElasticSearchRestClientTests extends FlatSpecLike with Matchers with ScalaFutures with StreamLogging {

  it should "construct a query with must" in {
    val queryTerms = Array(EsQueryBoolMatch("someKey1", "someValue1"), EsQueryBoolMatch("someKey2", "someValue2"))
    val queryMust = EsQueryMust(queryTerms)

    EsQuery(queryMust).toJson shouldBe JsObject(
      "query" ->
        JsObject(
          "bool" ->
            JsObject(
              "must" ->
                JsArray(
                  JsObject("match" -> JsObject("someKey1" -> JsString("someValue1"))),
                  JsObject("match" -> JsObject("someKey2" -> JsString("someValue2")))))))

    // Test must with ranges
    Seq((EsRangeGte, "gte"), (EsRangeGt, "gt"), (EsRangeLte, "lte"), (EsRangeLt, "lt")).foreach {
      case (rangeArg, rangeValue) =>
        val queryRange = EsQueryRange("someKey", rangeArg, "someValue")
        val queryTerms = Array(EsQueryBoolMatch("someKey1", "someValue1"), EsQueryBoolMatch("someKey2", "someValue2"))
        val queryMust = EsQueryMust(queryTerms, Some(queryRange))

        EsQuery(queryMust).toJson shouldBe JsObject(
          "query" ->
            JsObject(
              "bool" ->
                JsObject(
                  "must" ->
                    JsArray(
                      JsObject("match" -> JsObject("someKey1" -> JsString("someValue1"))),
                      JsObject("match" -> JsObject("someKey2" -> JsString("someValue2")))),
                  "filter" ->
                    JsObject("range" ->
                      JsObject("someKey" ->
                        JsObject(rangeValue -> "someValue".toJson))))))
    }
  }

  it should "construct a query with aggregations" in {
    Seq((EsAggMax, "max"), (EsAggMin, "min")).foreach {
      case (aggArg, aggValue) =>
        val queryAgg = EsQueryAggs("someAgg", aggArg, "someField")

        EsQuery(EsQueryAll(), aggs = Some(queryAgg)).toJson shouldBe JsObject(
          "query" -> JsObject("match_all" -> JsObject()),
          "aggs" -> JsObject("someAgg" -> JsObject(aggValue -> JsObject("field" -> "someField".toJson))))
    }
  }

  it should "construct a query with match" in {
    val queryMatch = EsQueryMatch("someField", "someValue")

    EsQuery(queryMatch).toJson shouldBe JsObject(
      "query" -> JsObject("match" -> JsObject("someField" -> JsObject("query" -> "someValue".toJson))))

    // Test match with types
    Seq((EsMatchPhrase, "phrase"), (EsMatchPhrasePrefix, "phrase_prefix")).foreach {
      case (typeArg, typeValue) =>
        val queryMatch = EsQueryMatch("someField", "someValue", Some(typeArg))

        EsQuery(queryMatch).toJson shouldBe JsObject(
          "query" -> JsObject(
            "match" -> JsObject("someField" -> JsObject("query" -> "someValue".toJson, "type" -> typeValue.toJson))))
    }
  }

  it should "construct a query with term" in {
    val queryTerm = EsQueryTerm("user", "someUser")

    EsQuery(queryTerm).toJson shouldBe JsObject("query" -> JsObject("term" -> JsObject("user" -> JsString("someUser"))))
  }

  it should "construct a query with query string" in {
    val queryString = EsQueryString("_type: someType")

    EsQuery(queryString).toJson shouldBe JsObject(
      "query" -> JsObject("query_string" -> JsObject("query" -> JsString("_type: someType"))))
  }

  it should "create a query with order" in {
    Seq((EsOrderAsc, "asc"), (EsOrderDesc, "desc")).foreach {
      case (orderArg, orderValue) =>
        val queryOrder = EsQueryOrder("someField", orderArg)

        EsQuery(EsQueryAll(), Some(queryOrder)).toJson shouldBe JsObject(
          "query" -> JsObject("match_all" -> JsObject()),
          "sort" -> JsArray(JsObject("someField" -> JsObject("order" -> orderValue.toJson))))
    }
  }

  it should "create query with size" in {
    val querySize = EsQuerySize(1)

    EsQuery(EsQueryAll(), size = Some(querySize)).toJson shouldBe JsObject(
      "query" -> JsObject("match_all" -> JsObject()),
      "size" -> JsNumber(1))
  }
}
