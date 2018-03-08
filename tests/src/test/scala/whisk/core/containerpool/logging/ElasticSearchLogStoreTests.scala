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

import pureconfig.error.ConfigReaderException

import spray.json.{JsNumber, JsObject, _}
import spray.json.DefaultJsonProtocol._

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
    with StreamLogging {

  behavior of "ElasticSearch Log Store"

  val testConfig = ElasticSearchLogStoreConfig("https", "host", 443, "user_logs", "activationId_str")
  val user = Identity(Subject(), EntityName("testSpace"), AuthKey(), Set())
  val activationId = ActivationId.generate()
  val authToken = "token"
  val authProjectId = user.uuid.asString
  val logs = ActivationLogs(
    Vector("2018-03-05T02:10:38.196689522Z stdout: some log stuff", "2018-03-05T02:10:38.196754258Z stdout: more logs"))

  val startTime = "2007-12-03T10:15:30Z"
  val endTime = "2007-12-03T10:15:45Z"

  val request = HttpRequest(
    method = GET,
    uri = "https://some.url",
    headers = List(RawHeader("key", "value")),
    entity = HttpEntity(MediaTypes.`application/json`, JsObject().compactPrint))

  val request2 = HttpRequest(
    method = GET,
    uri = "https://some.url",
    headers = List(RawHeader("x-auth-token", authToken)),
    entity = HttpEntity(MediaTypes.`application/json`, JsObject().compactPrint))

  val activation = WhiskActivation(
    namespace = EntityPath("ns"),
    name = EntityName("a"),
    Subject(),
    activationId = activationId,
    start = ZonedDateTime.parse(startTime).toInstant,
    end = ZonedDateTime.parse(endTime).toInstant,
    response = ActivationResponse.success(Some(JsObject("res" -> JsNumber(1)))),
    logs = logs,
    annotations = Parameters("limits", ActionLimits(TimeLimit(1.second), MemoryLimit(128.MB), LogLimit(1.MB)).toJson),
    duration = Some(123))

  private val date = LocalDate.now.format(DateTimeFormatter.ofPattern("yyyy.MM.dd"))
  private val expectedHeaders: List[HttpHeader] = List(
    Accept(MediaTypes.`application/json`),
    RawHeader("x-auth-token", authToken),
    RawHeader("x-auth-project-id", authProjectId))
  private val expectedPayload = JsObject(
    "query" -> JsObject("query_string" -> JsObject(
      "query" -> JsString(s"_type: ${testConfig.logMessageField} AND ${testConfig.activationIdField}: $activationId"))),
    "sort" -> JsArray(JsObject("@timestamp" -> JsObject("order" -> JsString("asc")))))

  implicit val ec: ExecutionContext = system.dispatcher
  implicit val materializer: ActorMaterializer = ActorMaterializer()

  val testFlow: Flow[(HttpRequest, Promise[HttpResponse]), (Try[HttpResponse], Promise[HttpResponse]), NotUsed] =
    Flow[(HttpRequest, Promise[HttpResponse])]
      .mapAsyncUnordered(1) {
        case (request, userContext) =>
          Unmarshal(request.entity)
            .to[JsObject]
            .map { payload =>
              request.uri.path.toString() shouldBe s"/elasticsearch/logstash-$authProjectId-$date/_search"
              request.headers shouldBe expectedHeaders
              payload shouldBe expectedPayload

              (
                Success(
                  HttpResponse(
                    StatusCodes.OK,
                    entity = HttpEntity(
                      ContentTypes.`application/json`,
                      s"""{"took":799,"timed_out":false,"_shards":{"total":204,"successful":204,"failed":0},"hits":{"total":2,"max_score":null,"hits":[{"_index":"logstash-2018.03.05.02","_type":"user_logs","_id":"1c00007f-ecb9-4083-8d2e-4d5e2849621f","_score":null,"_source":{"time_date":"2018-03-05T02:10:38.196689522Z","ALCH_ACCOUNT_ID":null,"message":"some log stuff\\n","type":"user_logs","event_uuid":"1c00007f-ecb9-4083-8d2e-4d5e2849621f","activationId_str":"$activationId","action_str":"lime@us.ibm.com_dev/logs","ALCH_TENANT_ID":"$authProjectId","logmet_cluster":"topic1-elasticsearch_1","@timestamp":"2018-03-05T02:11:37.687Z","@version":"1","stream_str":"stdout","timestamp":"2018-03-05T02:10:39.131Z"},"sort":[1520215897687]},{"_index":"logstash-2018.03.05.02","_type":"user_logs","_id":"14c2a5b7-8cad-4ec0-992e-70fab1996465","_score":null,"_source":{"time_date":"2018-03-05T02:10:38.196754258Z","ALCH_ACCOUNT_ID":null,"message":"more logs\\n","type":"user_logs","event_uuid":"14c2a5b7-8cad-4ec0-992e-70fab1996465","activationId_str":"$activationId","action_str":"lime@us.ibm.com_dev/logs","ALCH_TENANT_ID":"$authProjectId","logmet_cluster":"topic1-elasticsearch_1","@timestamp":"2018-03-05T02:11:37.701Z","@version":"1","stream_str":"stdout","timestamp":"2018-03-05T02:10:39.131Z"},"sort":[1520215897701]}]}}"""))),
                userContext)
            }
            .recover {
              case e =>
                println("failed")
                (Failure(e), userContext)
            }
      }

  val failFlow: Flow[(HttpRequest, Promise[HttpResponse]), (Try[HttpResponse], Promise[HttpResponse]), NotUsed] =
    Flow[(HttpRequest, Promise[HttpResponse])]
      .map {
        case (request, userContext) =>
          (Success(HttpResponse(StatusCodes.InternalServerError)), userContext)
      }

  def await[T](awaitable: Future[T], timeout: FiniteDuration = 10.seconds) = Await.result(awaitable, timeout)

  it should "fail when loading out of box configs (because whisk.logstore.elasticsearch doesn't exist)" in {
    a[ConfigReaderException[_]] should be thrownBy new DockerToActivationLogmetLogStore(system)
  }

  it should "get logs from supplied activation record when special header is not present" in {
    val esLogStore = new DockerToActivationLogmetLogStore(system, Some(testFlow), elasticSearchConfig = testConfig)
    await(esLogStore.fetchLogs(user, activation, request)) shouldBe logs
  }

  it should "get user logs associated with an activation ID" in {
    val esLogStore = new DockerToActivationLogmetLogStore(system, Some(testFlow), elasticSearchConfig = testConfig)
    await(esLogStore.fetchLogs(user, activation, request2)) shouldBe logs
  }

  it should "fail to connect to invalid host" in {
    val esLogStore = new DockerToActivationLogmetLogStore(system, elasticSearchConfig = testConfig)
    a[StreamTcpException] should be thrownBy await(esLogStore.fetchLogs(user, activation, request2))
  }

  it should "display an error if API cannot be reached" in {
    val esLogStore = new DockerToActivationLogmetLogStore(system, Some(failFlow), elasticSearchConfig = testConfig)
    a[RuntimeException] should be thrownBy await(esLogStore.fetchLogs(user, activation, request2))
  }

  it should "error when configuration protocol is invalid" in {
    val invalidHostConfig = ElasticSearchLogStoreConfig("protocol", "host", 443, "user_logs", "activationId_str")
    a[IllegalArgumentException] should be thrownBy new DockerToActivationLogmetLogStore(
      system,
      Some(failFlow),
      elasticSearchConfig = invalidHostConfig)
  }

}
