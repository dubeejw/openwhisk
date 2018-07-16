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

import java.time.{Instant, ZonedDateTime}

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

//import whisk.core.entity.size._
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
  private val subject = Subject()
  private val user =
    Identity(subject, Namespace(EntityName("testSpace"), uuid), BasicAuthenticationAuthKey(uuid, Secret()), Set())
  private val activationId = ActivationId.generate()
  private val namespace = EntityPath("namespace")
  private val name = EntityName("name")
  private val message = JsObject("result key" -> JsString("result value"))
  private val start = ZonedDateTime.now.toInstant
  private val end = ZonedDateTime.now.toInstant

  private val defaultSchema =
    ElasticSearchActivationFieldConfig("message", "activationId_str", "activation_record", "stream_str", "time_date")
  private val defaultConfig =
    ElasticSearchActivationStoreConfig("https", "host", 443, "/whisk_user_logs/_search", defaultSchema)

  private val defaultHttpResponse = HttpResponse(
    StatusCodes.OK,
    entity = HttpEntity(
      ContentTypes.`application/json`,
      s"""{"took":5,"timed_out":false,"_shards":{"total":5,"successful":5,"failed":0},"hits":{"total":2,"max_score":null,"hits":[{"_index":"whisk_user_logs","_type":"activation_record","_id":"AWSWtbKiYCyG38HxigNS","_score":null,"_source":{"name":"$name","subject":"$subject","activationId":"$activationId","version":"0.0.1","namespace":"$namespace","@version":"1","@timestamp":"2018-07-14T02:54:06.844Z","type":"activation_record","time_date":"$start","end_date":"$end","ALCH_TENANT_ID":"9cfe57a0-7ac1-4bf4-9026-d7e9e591271f","status":"0","message":"{\\"result key\\":\\"result value\\"}","duration_int":101},"sort":[1531536846075]},{"_index":"whisk_user_logs","_type":"activation_record","_id":"AWSWtZ54YCyG38HxigMb","_score":null,"_source":{"name":"$name","subject":"$subject","activationId":"$activationId","version":"0.0.1","namespace":"$namespace","@version":"1","@timestamp":"2018-07-14T02:54:01.817Z","type":"activation_record","time_date":"$start","end_date":"$end","ALCH_TENANT_ID":"9cfe57a0-7ac1-4bf4-9026-d7e9e591271f","status":"0","message":"{\\"result key\\":\\"result value\\"}","duration_int":101},"sort":[1531536841193]}]}}"""))

  private val defaultHttpResponse2 = HttpResponse(
    StatusCodes.OK,
    entity = HttpEntity(
      ContentTypes.`application/json`,
      s"""{"took":5,"timed_out":false,"_shards":{"total":5,"successful":5,"failed":0},"hits":{"total":1,"max_score":null,"hits":[{"_index":"whisk_user_logs","_type":"activation_record","_id":"AWSWtbKiYCyG38HxigNS","_score":null,"_source":{"name":"$name","subject":"$subject","activationId":"$activationId","version":"0.0.1","namespace":"$namespace","@version":"1","@timestamp":"2018-07-14T02:54:06.844Z","type":"activation_record","time_date":"$start","end_date":"$end","ALCH_TENANT_ID":"9cfe57a0-7ac1-4bf4-9026-d7e9e591271f","status":"0","message":"{\\"result key\\":\\"result value\\"}","duration_int":101},"sort":[1531536846075]}]}}"""))

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
    namespace = namespace,
    name = name,
    subject,
    activationId = activationId,
    start = start,
    end = end,
    response = ActivationResponse.success(Some(message)),
    logs = expectedLogs,
    duration = Some(101L))
  //annotations = Parameters("limits", ActionLimits(TimeLimit(1.second), MemoryLimit(128.MB), LogLimit(1.MB)).toJson))

  private def testFlow(httpResponse: HttpResponse = HttpResponse(), httpRequest: HttpRequest = HttpRequest())
    : Flow[(HttpRequest, Promise[HttpResponse]), (Try[HttpResponse], Promise[HttpResponse]), NotUsed] =
    Flow[(HttpRequest, Promise[HttpResponse])]
      .mapAsyncUnordered(1) {
        case (request, userContext) =>
          println(request)
          println(httpRequest)
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

  it should "count activations in namespace" in {
    val since = Instant.now
    val upto = Instant.now
    val payload = JsObject(
      "query" -> JsObject(
        "bool" -> JsObject(
          "must" -> JsArray(
            JsObject("match" -> JsObject("_type" -> JsString("activation_record"))),
            JsObject("match" -> JsObject("name" -> JsString(name.name)))),
          "filter" -> JsArray(
            JsObject("range" -> JsObject("@timestamp" -> JsObject("gt" -> JsString(since.toString)))),
            JsObject("range" -> JsObject("@timestamp" -> JsObject("lt" -> JsString(upto.toString))))))),
      "sort" -> JsArray(JsObject("time_date" -> JsObject("order" -> JsString("desc")))),
      "from" -> JsNumber(1)).compactPrint
    val httpRequest = HttpRequest(
      POST,
      Uri(s"/whisk_user_logs/_search"),
      List(Accept(MediaTypes.`application/json`)),
      HttpEntity(ContentTypes.`application/json`, payload))
    val esActivationStore =
      new ArtifactElasticSearchActivationStore(
        system,
        materializer,
        logging,
        Some(testFlow(defaultHttpResponse, httpRequest)),
        elasticSearchConfig = defaultConfig)

    await(
      esActivationStore.countActivationsInNamespace(
        user.namespace.name.toPath,
        Some(name.toPath),
        1,
        since = Some(since),
        upto = Some(upto),
        user = Some(user),
        request = Some(defaultLogStoreHttpRequest))) shouldBe JsObject("activations" -> JsNumber(1))
  }

  it should "list activations matching entity name" in {
    val since = Instant.now
    val upto = Instant.now
    val payload = JsObject(
      "query" -> JsObject(
        "bool" -> JsObject(
          "must" -> JsArray(
            JsObject("match" -> JsObject("_type" -> JsString("activation_record"))),
            JsObject("match" -> JsObject("name" -> JsString(name.name)))),
          "filter" -> JsArray(
            JsObject("range" -> JsObject("@timestamp" -> JsObject("gt" -> JsString(since.toString)))),
            JsObject("range" -> JsObject("@timestamp" -> JsObject("lt" -> JsString(upto.toString))))))),
      "sort" -> JsArray(JsObject("time_date" -> JsObject("order" -> JsString("desc")))),
      "size" -> JsNumber(2),
      "from" -> JsNumber(1)).compactPrint
    val httpRequest = HttpRequest(
      POST,
      Uri(s"/whisk_user_logs/_search"),
      List(Accept(MediaTypes.`application/json`)),
      HttpEntity(ContentTypes.`application/json`, payload))
    val esActivationStore =
      new ArtifactElasticSearchActivationStore(
        system,
        materializer,
        logging,
        Some(testFlow(defaultHttpResponse, httpRequest)),
        elasticSearchConfig = defaultConfig)

    await(
      esActivationStore.listActivationsMatchingName(
        user.namespace.name.toPath,
        name.toPath,
        1,
        2,
        since = Some(since),
        upto = Some(upto),
        user = Some(user),
        request = Some(defaultLogStoreHttpRequest))) shouldBe Right(List(activation, activation))
  }

  it should "list activations in namespace" in {
    val since = Instant.now
    val upto = Instant.now
    val payload = JsObject(
      "query" -> JsObject(
        "bool" -> JsObject(
          "must" -> JsArray(
            JsObject("match" -> JsObject("_type" -> JsString("activation_record"))),
            JsObject("match" -> JsObject("subject" -> JsString(user.namespace.name.asString)))),
          "filter" -> JsArray(
            JsObject("range" -> JsObject("@timestamp" -> JsObject("gt" -> JsString(since.toString)))),
            JsObject("range" -> JsObject("@timestamp" -> JsObject("lt" -> JsString(upto.toString))))))),
      "sort" -> JsArray(JsObject("time_date" -> JsObject("order" -> JsString("desc")))),
      "size" -> JsNumber(2),
      "from" -> JsNumber(1)).compactPrint
    val httpRequest = HttpRequest(
      POST,
      Uri(s"/whisk_user_logs/_search"),
      List(Accept(MediaTypes.`application/json`)),
      HttpEntity(ContentTypes.`application/json`, payload))
    val esActivationStore =
      new ArtifactElasticSearchActivationStore(
        system,
        materializer,
        logging,
        Some(testFlow(defaultHttpResponse, httpRequest)),
        elasticSearchConfig = defaultConfig)

    await(
      esActivationStore.listActivationsInNamespace(
        user.namespace.name.toPath,
        1,
        2,
        since = Some(since),
        upto = Some(upto),
        user = Some(user),
        request = Some(defaultLogStoreHttpRequest))) shouldBe Right(List(activation, activation))
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
