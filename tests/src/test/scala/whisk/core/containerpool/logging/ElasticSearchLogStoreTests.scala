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

import java.time.ZonedDateTime
import java.time.LocalDate
import java.time.format.DateTimeFormatter

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.model.HttpMethods.GET
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.RawHeader
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.scaladsl.Flow
import akka.stream.{ActorMaterializer, StreamTcpException}
import akka.testkit.TestKit
import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.headers.Accept

import common.StreamLogging

import org.junit.runner.RunWith
import org.scalatest.{FlatSpecLike, Matchers}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.junit.JUnitRunner
import org.scalatest.BeforeAndAfterEach

import pureconfig.error.ConfigReaderException

import spray.json._

import whisk.core.entity._
import whisk.core.entity.size._

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.util.{Failure, Success, Try}

@RunWith(classOf[JUnitRunner])
class ElasticSearchLogStoreTests
    extends TestKit(ActorSystem("ElasticSearchLogStore"))
    with FlatSpecLike
    with Matchers
    with ScalaFutures
    with StreamLogging
    with BeforeAndAfterEach {

  implicit val ec: ExecutionContext = system.dispatcher
  implicit val materializer: ActorMaterializer = ActorMaterializer()

  private val testConfig =
    ElasticSearchLogStoreConfig(
      "https",
      "host",
      443,
      "/whisk_user_logs/_search",
      "user_logs",
      "activationId_str",
      "stream_str",
      "action_str",
      "x-auth-token,x-auth-project-id")

  private val user = Identity(Subject(), EntityName("testSpace"), AuthKey(), Set())
  private val authToken = "token"
  private val authProjectId = "projectId"
  private val activationId = ActivationId.generate()
  private val date = LocalDate.now.format(DateTimeFormatter.ofPattern("yyyy.MM.dd"))

  private val expectedLogs = ActivationLogs(
    Vector("2018-03-05T02:10:38.196689522Z stdout: some log stuff", "2018-03-05T02:10:38.196754258Z stdout: more logs"))
  private val expectedHeaders: List[HttpHeader] = List(
    Accept(MediaTypes.`application/json`),
    RawHeader("x-auth-token", authToken),
    RawHeader("x-auth-project-id", authProjectId))
  private val expectedPayload = JsObject(
    "query" -> JsObject("query_string" -> JsObject(
      "query" -> JsString(s"_type: ${testConfig.logMessageField} AND ${testConfig.activationIdField}: $activationId"))),
    "sort" -> JsArray(JsObject("time_date" -> JsObject("order" -> JsString("asc"))))).compactPrint
  private val expectedUri = Uri(s"/whisk_user_logs/_search")
  //private val expectedUri = Uri(s"/elasticsearch/logstash-${user.uuid.asString}-$date/_search") TODO: test dynamic replacements

  private val requestWithoutRequiredHeaders = HttpRequest(
    method = GET,
    uri = "https://some.url",
    headers = List(RawHeader("key", "value")),
    entity = HttpEntity(MediaTypes.`application/json`, JsObject().compactPrint))
  private val requestWithRequiredHeaders = HttpRequest(
    method = GET,
    uri = "https://some.url",
    headers = List(RawHeader("x-auth-token", authToken), RawHeader("x-auth-project-id", authProjectId)),
    entity = HttpEntity(MediaTypes.`application/json`, JsObject().compactPrint))

  private val activation = WhiskActivation(
    namespace = EntityPath("ns"),
    name = EntityName("a"),
    Subject(),
    activationId = activationId,
    start = ZonedDateTime.now.toInstant,
    end = ZonedDateTime.now.toInstant,
    response = ActivationResponse.success(Some(JsObject("res" -> JsNumber(1)))),
    logs = expectedLogs,
    annotations = Parameters("limits", ActionLimits(TimeLimit(1.second), MemoryLimit(128.MB), LogLimit(1.MB)).toJson),
    duration = Some(123))

  private val mockHttpResponse = HttpResponse(
    StatusCodes.OK,
    entity = HttpEntity(
      ContentTypes.`application/json`,
      s"""{"took":799,"timed_out":false,"_shards":{"total":204,"successful":204,"failed":0},"hits":{"total":2,"max_score":null,"hits":[{"_index":"logstash-2018.03.05.02","_type":"user_logs","_id":"1c00007f-ecb9-4083-8d2e-4d5e2849621f","_score":null,"_source":{"time_date":"2018-03-05T02:10:38.196689522Z","accountId":null,"message":"some log stuff\\n","type":"user_logs","event_uuid":"1c00007f-ecb9-4083-8d2e-4d5e2849621f","activationId_str":"$activationId","action_str":"user@email.com/logs","tenantId":"tenantId","logmet_cluster":"topic1-elasticsearch_1","@timestamp":"2018-03-05T02:11:37.687Z","@version":"1","stream_str":"stdout","timestamp":"2018-03-05T02:10:39.131Z"},"sort":[1520215897687]},{"_index":"logstash-2018.03.05.02","_type":"user_logs","_id":"14c2a5b7-8cad-4ec0-992e-70fab1996465","_score":null,"_source":{"time_date":"2018-03-05T02:10:38.196754258Z","accountId":null,"message":"more logs\\n","type":"user_logs","event_uuid":"14c2a5b7-8cad-4ec0-992e-70fab1996465","activationId_str":"$activationId","action_str":"user@email.com/logs","tenantId":"tenant","logmet_cluster":"topic1-elasticsearch_1","@timestamp":"2018-03-05T02:11:37.701Z","@version":"1","stream_str":"stdout","timestamp":"2018-03-05T02:10:39.131Z"},"sort":[1520215897701]}]}}"""))

  private val testFlow
    : Flow[(HttpRequest, Promise[HttpResponse]), (Try[HttpResponse], Promise[HttpResponse]), NotUsed] =
    Flow[(HttpRequest, Promise[HttpResponse])]
      .mapAsyncUnordered(1) {
        case (request, userContext) =>
          Unmarshal(request.entity)
            .to[String]
            .map { payload =>
              request.uri shouldBe expectedUri
              request.headers shouldBe expectedHeaders
              payload shouldBe expectedPayload
              (Success(requestHttpResponse), userContext)
            }
            .recover {
              case e =>
                (Failure(e), userContext)
            }
      }

  private var requestHttpResponse = mockHttpResponse
  private var requestUri = expectedUri
  private var requestHeaders = expectedHeaders
  private var requestPayload = expectedPayload

  override def beforeEach = {
    requestHttpResponse = mockHttpResponse
    requestUri = expectedUri
    requestHeaders = expectedHeaders
    requestPayload = expectedPayload
  }

  private def await[T](awaitable: Future[T], timeout: FiniteDuration = 10.seconds) = Await.result(awaitable, timeout)

  behavior of "ElasticSearch Log Store"

  it should "fail when loading out of box configs (because whisk.logstore.elasticsearch doesn't exist)" in {
    a[ConfigReaderException[_]] should be thrownBy new ElasticSearchLogStore(system)
  }

  it should "get logs from supplied activation record when required headers are not present" in {
    val esLogStore = new ElasticSearchLogStore(system, Some(testFlow), elasticSearchConfig = testConfig)
    await(esLogStore.fetchLogs(user, activation, requestWithoutRequiredHeaders)) shouldBe expectedLogs
  }

  it should "get user logs from ElasticSearch when required headers are present" in {
    val esLogStore = new ElasticSearchLogStore(system, Some(testFlow), elasticSearchConfig = testConfig)
    await(esLogStore.fetchLogs(user, activation, requestWithRequiredHeaders)) shouldBe expectedLogs
  }

  it should "fail to connect to invalid host" in {
    val esLogStore = new ElasticSearchLogStore(system, elasticSearchConfig = testConfig)
    a[StreamTcpException] should be thrownBy await(esLogStore.fetchLogs(user, activation, requestWithRequiredHeaders))
  }

  it should "display an error if API cannot be reached" in {
    requestHttpResponse = HttpResponse(StatusCodes.InternalServerError)
    val esLogStore = new ElasticSearchLogStore(system, Some(testFlow), elasticSearchConfig = testConfig)
    a[RuntimeException] should be thrownBy await(esLogStore.fetchLogs(user, activation, requestWithRequiredHeaders))
  }

  it should "error when configuration protocol is invalid" in {
    val invalidHostConfig =
      ElasticSearchLogStoreConfig(
        "protocol",
        "host",
        443,
        "/whisk_user_logs",
        "user_logs",
        "activationId_str",
        "stream_str",
        "action_str",
        "none")
    a[IllegalArgumentException] should be thrownBy new ElasticSearchLogStore(
      system,
      Some(testFlow),
      elasticSearchConfig = invalidHostConfig)
  }

}
