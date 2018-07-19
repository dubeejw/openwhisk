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

package whisk.core.entity

import java.time.Instant

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.http.scaladsl.model._
import akka.stream.scaladsl.Flow

import whisk.common.{Logging, TransactionId}
import whisk.core.ConfigKeys
import whisk.core.containerpool.logging.{ElasticSearchRestClient, EsQuery, EsQueryString, EsSearchResult}
import whisk.core.containerpool.logging._
import whisk.core.database.NoDocumentException
import whisk.core.containerpool.logging.ElasticSearchJsonProtocol._

import scala.util.Try
import scala.concurrent.{ExecutionContext, Future, Promise}

import pureconfig.loadConfigOrThrow

import spray.json.DefaultJsonProtocol
import spray.json._

case class ElasticSearchActivationFieldConfig(name: String,
                                              namespace: String,
                                              subject: String,
                                              version: String,
                                              start: String,
                                              end: String,
                                              status: String,
                                              duration: String,
                                              message: String,
                                              activationId: String,
                                              activationRecord: String,
                                              stream: String)

case class ElasticSearchActivationStoreConfig(protocol: String,
                                              host: String,
                                              port: Int,
                                              path: String,
                                              schema: ElasticSearchActivationFieldConfig,
                                              requiredHeaders: Seq[String] = Seq.empty)

// TODO:
// Annotations are not in Elasticsearch...
// Trigger, sequence, and conductor logs are not in ES
trait ElasticSearchActivationRestClient {

  implicit val executionContext: ExecutionContext
  implicit val system: ActorSystem

  val httpFlow2: Option[Flow[(HttpRequest, Promise[HttpResponse]), (Try[HttpResponse], Promise[HttpResponse]), Any]]
  val elasticSearchConfig2: ElasticSearchActivationStoreConfig

  protected val esActivationClient =
    new ElasticSearchRestClient(
      elasticSearchConfig2.protocol,
      elasticSearchConfig2.host,
      elasticSearchConfig2.port,
      httpFlow2)

  // Schema of resultant activations from ES
  case class ActivationEntry(name: String,
                             subject: String,
                             activationId: String,
                             version: String,
                             endDate: String,
                             status: String,
                             timeDate: String,
                             message: String,
                             duration: Int,
                             namespace: String) {

    def toActivation(logs: ActivationLogs = ActivationLogs()) = {
      val result = status match {
        case "0" => ActivationResponse.success(Some(message.parseJson.asJsObject))
        case "1" => ActivationResponse.applicationError(message.parseJson.asJsObject.fields("error"))
        case "2" => ActivationResponse.containerError(message.parseJson.asJsObject.fields("error"))
        case "3" => ActivationResponse.whiskError(message.parseJson.asJsObject.fields("error"))
      }

      WhiskActivation(
        EntityPath(namespace),
        EntityName(name),
        Subject(subject),
        ActivationId(activationId),
        Instant.parse(timeDate),
        Instant.parse(endDate),
        response = result,
        logs = logs,
        duration = Some(duration),
        version = SemVer(version))
    }
  }

  object ActivationEntry extends DefaultJsonProtocol {
    implicit val serdes =
      jsonFormat(
        ActivationEntry.apply,
        elasticSearchConfig2.schema.name,
        elasticSearchConfig2.schema.subject,
        elasticSearchConfig2.schema.activationId,
        elasticSearchConfig2.schema.version,
        elasticSearchConfig2.schema.end,
        elasticSearchConfig2.schema.status,
        elasticSearchConfig2.schema.start,
        elasticSearchConfig2.schema.message,
        elasticSearchConfig2.schema.duration,
        elasticSearchConfig2.schema.namespace)
  }

  protected def transcribeActivations(queryResult: EsSearchResult): List[ActivationEntry] = {
    queryResult.hits.hits.map(_.source.convertTo[ActivationEntry]).toList
  }

  protected def extractRequiredHeaders2(headers: Seq[HttpHeader]) =
    headers.filter(h => elasticSearchConfig2.requiredHeaders.contains(h.lowercaseName)).toList

  protected def getRanges(since: Option[Instant] = None, upto: Option[Instant] = None) = {
    val sinceRange: Option[EsQueryRange] = since.map { time =>
      Some(EsQueryRange("@timestamp", EsRangeGt, time.toString))
    } getOrElse None
    val uptoRange: Option[EsQueryRange] = upto.map { time =>
      Some(EsQueryRange("@timestamp", EsRangeLt, time.toString))
    } getOrElse None

    Vector(sinceRange, uptoRange).flatten
  }

  protected def generateLogPayload(activationId: ActivationId) = {
    val logQuery =
      s"_type: user_logs AND ${elasticSearchConfig2.schema.activationId}: ${activationId.asString}"
    val queryString = EsQueryString(logQuery)
    val queryOrder = EsQueryOrder(elasticSearchConfig2.schema.start, EsOrderAsc)

    EsQuery(queryString, Some(queryOrder))
  }

  protected def generateGetPayload(activationId: ActivationId) = {
    val query =
      s"_type: ${elasticSearchConfig2.schema.activationRecord} AND ${elasticSearchConfig2.schema.activationId}: ${activationId.asString
        .substring(activationId.asString.indexOf("/") + 1)}"

    EsQuery(EsQueryString(query))
  }

  protected def generateGetPayload2(activationId: String) = {
    val query =
      s"_type: ${elasticSearchConfig2.schema.activationRecord} AND ${elasticSearchConfig2.schema.activationId}: $activationId"

    EsQuery(EsQueryString(query))
  }

  protected def generateCountActivationsInNamespacePayload(name: Option[EntityPath],
                                                           skip: Int,
                                                           since: Option[Instant] = None,
                                                           upto: Option[Instant] = None) = {
    val queryRanges = getRanges(since, upto)
    val activationMatch = Some(EsQueryBoolMatch("_type", elasticSearchConfig2.schema.activationRecord))
    val entityMatch: Option[EsQueryBoolMatch] = name.map { n =>
      Some(EsQueryBoolMatch(elasticSearchConfig2.schema.name, n.toString))
    } getOrElse None
    val queryTerms = Vector(activationMatch, entityMatch).flatten
    val queryMust = EsQueryMust(queryTerms, queryRanges)
    val queryOrder = EsQueryOrder(elasticSearchConfig2.schema.start, EsOrderDesc)

    EsQuery(queryMust, Some(queryOrder), from = skip)
  }

  protected def generateListActiationsMatchNamePayload(name: String,
                                                       skip: Int,
                                                       limit: Int,
                                                       since: Option[Instant] = None,
                                                       upto: Option[Instant] = None) = {
    val queryRanges = getRanges(since, upto)
    val queryTerms = Vector(
      EsQueryBoolMatch("_type", elasticSearchConfig2.schema.activationRecord),
      EsQueryBoolMatch(elasticSearchConfig2.schema.name, name))
    val queryMust = EsQueryMust(queryTerms, queryRanges)
    val queryOrder = EsQueryOrder(elasticSearchConfig2.schema.start, EsOrderDesc)

    EsQuery(queryMust, Some(queryOrder), Some(limit), from = skip)
  }

  protected def generateListActivationsInNamespacePayload(namespace: String,
                                                          skip: Int,
                                                          limit: Int,
                                                          since: Option[Instant] = None,
                                                          upto: Option[Instant] = None) = {
    val queryRanges = getRanges(since, upto)
    val queryTerms = Vector(
      EsQueryBoolMatch("_type", elasticSearchConfig2.schema.activationRecord),
      EsQueryBoolMatch(elasticSearchConfig2.schema.subject, namespace))
    val queryMust = EsQueryMust(queryTerms, queryRanges)
    val queryOrder = EsQueryOrder(elasticSearchConfig2.schema.start, EsOrderDesc)

    EsQuery(queryMust, Some(queryOrder), Some(limit), from = skip)
  }

  def getActivation(activationId: String, uuid: String, headers: List[HttpHeader] = List.empty)(
    implicit transid: TransactionId): Future[ActivationEntry] = {
    val payload = generateGetPayload2(activationId)

    esActivationClient.search[EsSearchResult](uuid, payload, headers).flatMap {
      case Right(queryResult) =>
        val res = transcribeActivations(queryResult)

        if (res.nonEmpty) {
          Future.successful(res.head)
        } else {
          Future.failed(new NoDocumentException("Document not found"))
        }

      case Left(code) =>
        Future.failed(new RuntimeException(s"Status code '$code' was returned from activation store"))
    }
  }

  def listActivationMatching(
    uuid: String,
    name: String,
    skip: Int,
    limit: Int,
    since: Option[Instant] = None,
    upto: Option[Instant] = None,
    headers: List[HttpHeader] = List.empty)(implicit transid: TransactionId): Future[List[ActivationEntry]] = {
    val payload = generateListActiationsMatchNamePayload(name, skip, limit, since, upto)

    esActivationClient.search[EsSearchResult](uuid, payload, headers).flatMap {
      case Right(queryResult) =>
        Future.successful(transcribeActivations(queryResult))
      case Left(code) =>
        Future.failed(new RuntimeException(s"Status code '$code' was returned from activation store"))
    }
  }

  def listActivationsNamespace(
    uuid: String,
    namespace: String,
    skip: Int,
    limit: Int,
    since: Option[Instant] = None,
    upto: Option[Instant] = None,
    headers: List[HttpHeader] = List.empty)(implicit transid: TransactionId): Future[List[ActivationEntry]] = {
    val payload = generateListActivationsInNamespacePayload(namespace, skip, limit, since, upto)

    esActivationClient.search[EsSearchResult](uuid, payload, headers).flatMap {
      case Right(queryResult) =>
        Future.successful(transcribeActivations(queryResult))
      case Left(code) =>
        Future.failed(new RuntimeException(s"Status code '$code' was returned from activation store"))
    }
  }

}

class ArtifactElasticSearchActivationStore(
  override val system: ActorSystem,
  actorMaterializer: ActorMaterializer,
  logging: Logging,
  override val httpFlow2: Option[
    Flow[(HttpRequest, Promise[HttpResponse]), (Try[HttpResponse], Promise[HttpResponse]), Any]] = None,
  override val httpFlow: Option[
    Flow[(HttpRequest, Promise[HttpResponse]), (Try[HttpResponse], Promise[HttpResponse]), Any]] = None,
  override val elasticSearchConfig2: ElasticSearchActivationStoreConfig =
    loadConfigOrThrow[ElasticSearchActivationStoreConfig](ConfigKeys.elasticSearchActivationStore),
  override val elasticSearchConfig: ElasticSearchLogStoreConfig =
    loadConfigOrThrow[ElasticSearchLogStoreConfig](ConfigKeys.logStoreElasticSearch))
    extends ArtifactActivationStore(system, actorMaterializer, logging)
    with ElasticSearchActivationRestClient
    with ElasticSearchLogRestClient {

  override def get(activationId: ActivationId, user: Option[Identity] = None, request: Option[HttpRequest] = None)(
    implicit transid: TransactionId): Future[WhiskActivation] = {
    val headers = extractRequiredHeaders2(request.get.headers)

    // Return activation from ElasticSearch or from artifact store if required headers are not present
    if (headers.length == elasticSearchConfig2.requiredHeaders.length) {
      val uuid = elasticSearchConfig2.path.format(user.get.namespace.uuid.asString)
      val headers = extractRequiredHeaders2(request.get.headers)
      val id = activationId.asString.substring(activationId.asString.indexOf("/") + 1)

      getActivation(id, uuid, headers).flatMap(activation =>
        logs(uuid, id, headers).map(logs => activation.toActivation(ActivationLogs(logs))))
    } else {
      super.get(activationId, user, request)
    }
  }

  override def countActivationsInNamespace(
    namespace: EntityPath,
    name: Option[EntityPath] = None,
    skip: Int,
    since: Option[Instant] = None,
    upto: Option[Instant] = None,
    user: Option[Identity] = None,
    request: Option[HttpRequest] = None)(implicit transid: TransactionId): Future[JsObject] = {
    val payload = generateCountActivationsInNamespacePayload(name, skip, since, upto)
    val uuid = elasticSearchConfig2.path.format(user.get.namespace.uuid.asString)
    val headers = extractRequiredHeaders2(request.get.headers)

    if (headers.length == elasticSearchConfig2.requiredHeaders.length) {
      esActivationClient.search[EsSearchResult](uuid, payload, headers).flatMap {
        case Right(queryResult) =>
          val total = Math.max(0, queryResult.hits.total - skip)
          Future.successful(JsObject("activations" -> total.toJson))
        case Left(code) =>
          Future.failed(new RuntimeException(s"Status code '$code' was returned from activation store"))
      }
    } else {
      super.countActivationsInNamespace(namespace, name, skip, since, upto, user, request)
    }
  }

  override def listActivationsMatchingName(namespace: EntityPath,
                                           name: EntityPath,
                                           skip: Int,
                                           limit: Int,
                                           includeDocs: Boolean = false,
                                           since: Option[Instant] = None,
                                           upto: Option[Instant] = None,
                                           user: Option[Identity] = None,
                                           request: Option[HttpRequest] = None)(
    implicit transid: TransactionId): Future[Either[List[JsObject], List[WhiskActivation]]] = {
    val uuid = elasticSearchConfig2.path.format(user.get.namespace.uuid.asString)
    val headers = extractRequiredHeaders2(request.get.headers)

    if (headers.length == elasticSearchConfig2.requiredHeaders.length) {
      listActivationMatching(uuid, name.toString, skip, limit, since, upto, headers).flatMap { activationList =>
        Future
          .sequence(activationList.map { act =>
            logs(uuid, act.activationId, headers).map(logs => act.toActivation(ActivationLogs(logs)))
          })
          .map(Right(_))
      }
    } else {
      super.listActivationsMatchingName(namespace, name, skip, limit, includeDocs, since, upto, user, request)
    }
  }

  override def listActivationsInNamespace(namespace: EntityPath,
                                          skip: Int,
                                          limit: Int,
                                          includeDocs: Boolean = false,
                                          since: Option[Instant] = None,
                                          upto: Option[Instant] = None,
                                          user: Option[Identity] = None,
                                          request: Option[HttpRequest] = None)(
    implicit transid: TransactionId): Future[Either[List[JsObject], List[WhiskActivation]]] = {
    val uuid = elasticSearchConfig2.path.format(user.get.namespace.uuid.asString)
    val headers = extractRequiredHeaders2(request.get.headers)

    if (headers.length == elasticSearchConfig2.requiredHeaders.length) {
      listActivationsNamespace(uuid, namespace.asString, skip, limit, since, upto, headers).flatMap { activationList =>
        Future
          .sequence(activationList.map { act =>
            logs(uuid, act.activationId, headers).map(logs => act.toActivation(ActivationLogs(logs)))
          })
          .map(Right(_))
      }
    } else {
      super.listActivationsInNamespace(namespace, skip, limit, includeDocs, since, upto, user, request)
    }
  }

}

object ArtifactElasticSearchActivationStoreProvider extends ActivationStoreProvider {
  override def instance(actorSystem: ActorSystem, actorMaterializer: ActorMaterializer, logging: Logging) =
    new ArtifactElasticSearchActivationStore(actorSystem, actorMaterializer, logging)
}
