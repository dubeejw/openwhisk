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

import scala.concurrent.Future

import akka.actor.ActorSystem
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.Accept
import akka.stream.scaladsl.Flow

import scala.concurrent.Promise
import scala.util.Try

import spray.json._
import spray.json.DefaultJsonProtocol._

import whisk.common.Logging
import whisk.http.PoolingRestClient

// Schema of query result order in ES
case class EsQueryOrder(order: String)
case class EsQueryTimestamp(timestamp: EsQueryOrder)

// Schema of queries in ES
case class EsQueryTerm(term: JsObject)
case class EsQueryValue(query: String)
case class EsQueryString(query_string: EsQueryValue)
case class EsQuery(query: JsValue, sort: Option[Array[JsValue]] = None)

object ElasticSearchJsonProtocol extends DefaultJsonProtocol {
  implicit val esQueryOrderFormat = jsonFormat1(EsQueryOrder.apply)
  implicit val esQueryTimestampFormat = jsonFormat(EsQueryTimestamp.apply _, "@timestamp")
  implicit val esQueryTermFormat = jsonFormat1(EsQueryTerm.apply)
  implicit val esQueryValueFormat = jsonFormat1(EsQueryValue.apply)
  implicit val esQueryStringFormat = jsonFormat1(EsQueryString.apply)
  implicit val esQueryFormat = jsonFormat(EsQuery.apply, "query", "sort")
}

// Schema of logs in ES
case class UserLogEntry(message: String, tenantId: String, stream: String, time: String, action: String)

// Schema of search requests in ES
case class EsSearchHit(source: JsObject)
case class EsSearchHits(hits: Vector[EsSearchHit])
case class EsSearchResult(hits: EsSearchHits)

object UserLogEntry extends DefaultJsonProtocol {
  implicit val serdes =
    jsonFormat(UserLogEntry.apply, "message", "ALCH_TENANT_ID", "stream_str", "time_date", "action_str")
}

object EsSearchHit extends DefaultJsonProtocol { implicit val serdes = jsonFormat(EsSearchHit.apply _, "_source") }
object EsSearchHits extends DefaultJsonProtocol { implicit val serdes = jsonFormat1(EsSearchHits.apply) }
object EsSearchResult extends DefaultJsonProtocol { implicit val serdes = jsonFormat1(EsSearchResult.apply) }

class ElasticSearchRestClient(
  protocol: String,
  host: String,
  port: Int,
  httpFlow: Option[Flow[(HttpRequest, Promise[HttpResponse]), (Try[HttpResponse], Promise[HttpResponse]), Any]] = None)(
  implicit system: ActorSystem,
  logging: Logging)
    extends PoolingRestClient(protocol, host, port, 16 * 1024, httpFlow) {

  private val baseHeaders: List[HttpHeader] = List(Accept(MediaTypes.`application/json`))

  /**
   * method to perform an http GET request that expects json response
   *
   * @param uri     relative path to be used in the request to Elasticsearch
   * @param headers list of HTTP request headers
   * @return Future with either the JSON response or the status code of the request, if the request is unsuccessful
   */
  def get(uri: Uri, headers: List[HttpHeader] = List.empty): Future[Either[StatusCode, JsObject]] =
    requestJson[JsObject](mkRequest(HttpMethods.GET, uri, baseHeaders ++ headers))

  /**
   * method to perform an http POST request that expects json response
   *
   * @param uri     relative path to be used in the request to Elasticsearch
   * @param headers list of HTTP request headers
   * @param payload JSON to be sent in the request
   * @return Future with either the JSON response or the status code of the request, if the request is unsuccessful
   */
  def post(uri: Uri,
           headers: List[HttpHeader] = List.empty,
           payload: JsObject): Future[Either[StatusCode, JsObject]] = {
    requestJson[JsObject](mkJsonRequest(HttpMethods.POST, uri, payload, baseHeaders ++ headers))
  }
}
