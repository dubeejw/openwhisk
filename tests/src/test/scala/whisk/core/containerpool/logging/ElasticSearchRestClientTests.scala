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

  it should "construct a query with must with range" in {
    val queryRange = EsQueryRange("someKey", EsRangeGte, "someValue")
    val queryTerms = Array(EsQueryBoolMatch("someKey1", "someValue1"), EsQueryBoolMatch("someKey2", "someValue2"))
    val queryMust = EsQueryMust(queryTerms, Some(queryRange))
    val query = EsQuery(queryMust)

    query.toJson shouldBe JsObject(
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
                    JsObject("gte" -> "someValue".toJson))))))
  }

  it should "construct a query with must without range" in {
    val queryTerms = Array(EsQueryBoolMatch("someKey1", "someValue1"), EsQueryBoolMatch("someKey2", "someValue2"))
    val queryMust = EsQueryMust(queryTerms)
    val query = EsQuery(queryMust)

    query.toJson shouldBe JsObject(
      "query" ->
        JsObject(
          "bool" ->
            JsObject(
              "must" ->
                JsArray(
                  JsObject("match" -> JsObject("someKey1" -> JsString("someValue1"))),
                  JsObject("match" -> JsObject("someKey2" -> JsString("someValue2")))))))
  }

  it should "construct a query with aggregation" in {
    val queryAgg = EsQueryAggs("someAgg", EsAggMax, "someField")
    val query = EsQuery(EsQueryAll(), aggs = Some(queryAgg))

    query.toJson shouldBe JsObject(
      "query" -> JsObject("match_all" -> JsObject()),
      "aggs" -> JsObject("someAgg" -> JsObject("max" -> JsObject("field" -> "someField".toJson))))
  }

  it should "construct a query with match" in {
    val queryMatch = EsQueryMatch("someField", "someValue")
    val query = EsQuery(queryMatch)

    query.toJson shouldBe JsObject(
      "query" -> JsObject("match" -> JsObject("someField" -> JsObject("query" -> "someValue".toJson))))
  }

  it should "construct a query with match with type" in {
    val queryMatch = EsQueryMatch("someField", "someValue", Some(EsMatchPhrase))
    val query = EsQuery(queryMatch)

    query.toJson shouldBe JsObject(
      "query" -> JsObject(
        "match" -> JsObject("someField" -> JsObject("query" -> "someValue".toJson, "type" -> "phrase".toJson))))
  }

  it should "construct a query with term" in {
    val queryTerm = EsQueryTerm("user", "someUser")
    val query = EsQuery(queryTerm)

    query.toJson shouldBe JsObject("query" -> JsObject("term" -> JsObject("user" -> JsString("someUser"))))
  }

  it should "construct a query with query string" in {
    val queryString = EsQueryString("_type: someType")
    val query = EsQuery(queryString)

    query.toJson shouldBe JsObject(
      "query" -> JsObject("query_string" -> JsObject("query" -> JsString("_type: someType"))))
  }

  it should "construct a query with match and order" in {
    val queryMatch = EsQueryMatch("someField", "someValue")
    val queryOrder = EsQueryOrder("time_date", EsOrderDesc)
    val query = EsQuery(queryMatch, Some(queryOrder))

    query.toJson shouldBe JsObject(
      "query" -> JsObject("match" -> JsObject("someField" -> JsObject("query" -> "someValue".toJson))),
      "sort" -> JsArray(JsObject("time_date" -> JsObject("order" -> JsString("desc")))))
  }

  it should "construct a query with term and order" in {
    val queryTerm = EsQueryTerm("user", "someUser")
    val queryOrder = EsQueryOrder("time_date", EsOrderDesc)
    val query = EsQuery(queryTerm, Some(queryOrder))

    query.toJson shouldBe JsObject(
      "query" -> JsObject("term" -> JsObject("user" -> JsString("someUser"))),
      "sort" -> JsArray(JsObject("time_date" -> JsObject("order" -> JsString("desc")))))
  }

  it should "construct a query with query string and order" in {
    val queryString = EsQueryString("_type: someType")
    val queryOrder = EsQueryOrder("time_date", EsOrderAsc)
    val query = EsQuery(queryString, Some(queryOrder))

    query.toJson shouldBe JsObject(
      "query" -> JsObject("query_string" -> JsObject("query" -> JsString("_type: someType"))),
      "sort" -> JsArray(JsObject("time_date" -> JsObject("order" -> JsString("asc")))))
  }

  it should "construct a query with match, order, and size" in {
    val queryMatch = EsQueryMatch("someField", "someValue")
    val queryOrder = EsQueryOrder("time_date", EsOrderDesc)
    val querySize = EsQuerySize(1)
    val query = EsQuery(queryMatch, Some(queryOrder), Some(querySize))

    query.toJson shouldBe JsObject(
      "query" -> JsObject("match" -> JsObject("someField" -> JsObject("query" -> "someValue".toJson))),
      "sort" -> JsArray(JsObject("time_date" -> JsObject("order" -> JsString("desc")))),
      "size" -> JsNumber(1))
  }
}
