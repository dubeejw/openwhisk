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

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.model.HttpMethods.{GET, POST}
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{Accept /*, RawHeader*/}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Flow
import akka.testkit.TestKit
import common.StreamLogging
import org.junit.runner.RunWith
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpecLike, Matchers}
import pureconfig.error.ConfigReaderException
import spray.json._
import whisk.core.entity._
import whisk.core.entity.size._
import whisk.common.TransactionId

import scala.concurrent.duration._
import scala.concurrent.{Await /*, ExecutionContext*/, Future, Promise}
import scala.util.{Success, Try}

@RunWith(classOf[JUnitRunner])
class ElasticSearchActivationStoreTests
    extends TestKit(ActorSystem("ElasticSearchActivationStore"))
    with FlatSpecLike
    with Matchers
    with ScalaFutures
    with StreamLogging {

  val materializer = ActorMaterializer()

  implicit val transid: TransactionId = TransactionId.testing

  private val uuid = UUID()
  private val user =
    Identity(Subject(), Namespace(EntityName("testSpace"), uuid), BasicAuthenticationAuthKey(uuid, Secret()), Set())
  private val activationId = ActivationId.generate()

  private val defaultSchema =
    ElasticSearchActivationFieldConfig("message", "activationId_str", "activation_record", "stream_str", "time_date")
  private val defaultConfig =
    ElasticSearchActivationStoreConfig("https", "host", 443, "/whisk_user_logs/_search", defaultSchema)

  /*private val defaultPayload = JsObject(
    "query" -> JsObject(
      "query_string" -> JsObject("query" -> JsString(
        s"_type: ${defaultConfig.schema.activationRecord} AND ${defaultConfig.schema.activationId}: $activationId"))),
    "sort" -> JsArray(JsObject(defaultConfig.schema.time -> JsObject("order" -> JsString("asc")))),
    "from" -> JsNumber(0)).compactPrint*/

  private val defaultPayload = JsObject(
    "query" -> JsObject(
      "query_string" -> JsObject("query" -> JsString(
        s"_type: ${defaultConfig.schema.activationRecord} AND ${defaultConfig.schema.activationId}: $activationId"))),
    "from" -> JsNumber(0)).compactPrint

  private val defaultHttpRequest = HttpRequest(
    POST,
    Uri(s"/whisk_user_logs/_search"),
    List(Accept(MediaTypes.`application/json`)),
    HttpEntity(ContentTypes.`application/json`, defaultPayload))
  private val defaultLogStoreHttpRequest =
    HttpRequest(method = GET, uri = "https://some.url", entity = HttpEntity.Empty)

  private val expectedLogs = ActivationLogs(Vector.empty)

  private val activation = WhiskActivation(
    namespace = EntityPath("namespace"),
    name = EntityName("name"),
    Subject(),
    activationId = activationId,
    start = ZonedDateTime.now.toInstant,
    end = ZonedDateTime.now.toInstant,
    response = ActivationResponse.success(Some(JsObject("res" -> JsNumber(1)))),
    logs = expectedLogs,
    annotations = Parameters("limits", ActionLimits(TimeLimit(1.second), MemoryLimit(128.MB), LogLimit(1.MB)).toJson))

  private def testFlow(httpResponse: HttpResponse = HttpResponse(), httpRequest: HttpRequest = HttpRequest())
    : Flow[(HttpRequest, Promise[HttpResponse]), (Try[HttpResponse], Promise[HttpResponse]), NotUsed] =
    Flow[(HttpRequest, Promise[HttpResponse])]
      .mapAsyncUnordered(1) {
        case (request, userContext) =>
          request shouldBe httpRequest
          Future.successful((Success(httpResponse), userContext))
      }

  private def await[T](awaitable: Future[T], timeout: FiniteDuration = 10.seconds) = Await.result(awaitable, timeout)

  behavior of "ElasticSearch Activation Store"

  it should "fail to connect to invalid host" in {
    val esActivationStore =
      new ArtifactElasticSearchActivationStore(system, materializer, logging, elasticSearchConfig = defaultConfig)

    a[Throwable] should be thrownBy await(
      esActivationStore.get(activation.activationId, Some(user), Some(defaultLogStoreHttpRequest)))
  }

  it should "forward errors from ElasticSearch" in {
    val httpResponse = HttpResponse(StatusCodes.InternalServerError)
    val esActivationStore =
      new ArtifactElasticSearchActivationStore(
        system,
        materializer,
        logging,
        Some(testFlow(httpResponse, defaultHttpRequest)),
        elasticSearchConfig = defaultConfig)

    a[RuntimeException] should be thrownBy await(
      esActivationStore.get(activation.activationId, Some(user), Some(defaultLogStoreHttpRequest)))
  }

  it should "fail when loading out of box configs since whisk.activationstore.elasticsearch does not exist" in {
    a[ConfigReaderException[_]] should be thrownBy new ArtifactElasticSearchActivationStore(
      system,
      materializer,
      logging)
  }

  it should "error when configuration protocol is invalid" in {
    val invalidHostConfig =
      ElasticSearchActivationStoreConfig("protocol", "host", 443, "/whisk_user_logs", defaultSchema, Seq.empty)

    a[IllegalArgumentException] should be thrownBy new ArtifactElasticSearchActivationStore(
      system,
      materializer,
      logging,
      elasticSearchConfig = invalidHostConfig)
  }

}
