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

import pureconfig.loadConfigOrThrow

import spray.json.DefaultJsonProtocol
import spray.json._

import whisk.common.{Logging, TransactionId}
import whisk.core.ConfigKeys
import whisk.core.containerpool.logging.{ElasticSearchRestClient, EsQuery, EsQueryString, EsSearchResult}
import whisk.core.containerpool.logging._
import whisk.core.database.{ArtifactStore, NoDocumentException}
import whisk.core.containerpool.logging.ElasticSearchJsonProtocol._

import scala.util.Try
import scala.concurrent.{Future, Promise}

import akka.stream.scaladsl.Flow

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
// Splice logs in to activations
// Annotations are not in Elasticsearch...
class ArtifactElasticSearchActivationStore(
  actorSystem: ActorSystem,
  actorMaterializer: ActorMaterializer,
  logging: Logging,
  httpFlow: Option[Flow[(HttpRequest, Promise[HttpResponse]), (Try[HttpResponse], Promise[HttpResponse]), Any]] = None,
  elasticSearchConfig: ElasticSearchActivationStoreConfig =
    loadConfigOrThrow[ElasticSearchActivationStoreConfig](ConfigKeys.elasticSearchActivationStore))
    extends ArtifactActivationStore(actorSystem, actorMaterializer, logging) {

  implicit val system = actorSystem

  private val artifactStore: ArtifactStore[WhiskActivation] =
    WhiskActivationStore.datastore()(actorSystem, logging, actorMaterializer)
  private val esClient =
    new ElasticSearchRestClient(
      elasticSearchConfig.protocol,
      elasticSearchConfig.host,
      elasticSearchConfig.port,
      httpFlow)

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

    def toActivation = {
      val result = status match {
        case "0" => ActivationResponse.success(Some(message.parseJson.asJsObject))
        case "1" => ActivationResponse.applicationError(message.parseJson.asJsObject)
        case "2" => ActivationResponse.containerError(message.parseJson.asJsObject)
        case "3" => ActivationResponse.whiskError(message.parseJson.asJsObject)
      }

      WhiskActivation(
        EntityPath(namespace),
        EntityName(name),
        Subject(subject),
        ActivationId(activationId),
        Instant.parse(timeDate),
        Instant.parse(endDate),
        response = result,
        duration = Some(duration),
        version = SemVer(version))
    }
  }

  object ActivationEntry extends DefaultJsonProtocol {
    implicit val serdes =
      jsonFormat(
        ActivationEntry.apply,
        elasticSearchConfig.schema.name,
        elasticSearchConfig.schema.subject,
        elasticSearchConfig.schema.activationId,
        elasticSearchConfig.schema.version,
        elasticSearchConfig.schema.end,
        elasticSearchConfig.schema.status,
        elasticSearchConfig.schema.start,
        elasticSearchConfig.schema.message,
        elasticSearchConfig.schema.duration,
        elasticSearchConfig.schema.namespace)
  }

  private def transcribeActivations(queryResult: EsSearchResult): List[WhiskActivation] = {
    queryResult.hits.hits.map(_.source.convertTo[ActivationEntry].toActivation).toList
  }

  private def extractRequiredHeaders(headers: Seq[HttpHeader]) =
    headers.filter(h => elasticSearchConfig.requiredHeaders.contains(h.lowercaseName)).toList

  private def getRanges(since: Option[Instant] = None, upto: Option[Instant] = None) = {
    val sinceRange: Option[EsQueryRange] = since.map { time =>
      Some(EsQueryRange("@timestamp", EsRangeGt, time.toString))
    } getOrElse None
    val uptoRange: Option[EsQueryRange] = upto.map { time =>
      Some(EsQueryRange("@timestamp", EsRangeLt, time.toString))
    } getOrElse None

    Vector(sinceRange, uptoRange).flatten
  }

  private def generateGetPayload(activationId: ActivationId) = {
    val query =
      s"_type: ${elasticSearchConfig.schema.activationRecord} AND ${elasticSearchConfig.schema.activationId}: ${activationId.asString
        .substring(activationId.asString.indexOf("/") + 1)}"

    EsQuery(EsQueryString(query))
  }

  private def generateCountActivationsInNamespacePayload(name: Option[EntityPath],
                                                         skip: Int,
                                                         since: Option[Instant] = None,
                                                         upto: Option[Instant] = None) = {
    val queryRanges = getRanges(since, upto)
    val activationMatch = Some(EsQueryBoolMatch("_type", elasticSearchConfig.schema.activationRecord))
    val entityMatch: Option[EsQueryBoolMatch] = name.map { n =>
      Some(EsQueryBoolMatch(elasticSearchConfig.schema.name, n.toString))
    } getOrElse None
    val queryTerms = Vector(activationMatch, entityMatch).flatten
    val queryMust = EsQueryMust(queryTerms, queryRanges)
    val queryOrder = EsQueryOrder(elasticSearchConfig.schema.start, EsOrderDesc)

    EsQuery(queryMust, Some(queryOrder), from = skip)
  }

  private def generateListActiationsMatchNamePayload(name: EntityPath,
                                                     skip: Int,
                                                     limit: Int,
                                                     since: Option[Instant] = None,
                                                     upto: Option[Instant] = None) = {
    val queryRanges = getRanges(since, upto)
    val queryTerms = Vector(
      EsQueryBoolMatch("_type", elasticSearchConfig.schema.activationRecord),
      EsQueryBoolMatch(elasticSearchConfig.schema.name, name.toString))
    val queryMust = EsQueryMust(queryTerms, queryRanges)
    val queryOrder = EsQueryOrder(elasticSearchConfig.schema.start, EsOrderDesc)

    EsQuery(queryMust, Some(queryOrder), Some(limit), from = skip)
  }

  private def generateListActivationsInNamespacePayload(namespace: EntityPath,
                                                        skip: Int,
                                                        limit: Int,
                                                        since: Option[Instant] = None,
                                                        upto: Option[Instant] = None) = {
    val queryRanges = getRanges(since, upto)
    val queryTerms = Vector(
      EsQueryBoolMatch("_type", elasticSearchConfig.schema.activationRecord),
      EsQueryBoolMatch(elasticSearchConfig.schema.subject, namespace.asString))
    val queryMust = EsQueryMust(queryTerms, queryRanges)
    val queryOrder = EsQueryOrder(elasticSearchConfig.schema.start, EsOrderDesc)

    EsQuery(queryMust, Some(queryOrder), Some(limit), from = skip)
  }

  override def get(activationId: ActivationId, user: Option[Identity] = None, request: Option[HttpRequest] = None)(
    implicit transid: TransactionId): Future[WhiskActivation] = {
    val payload = generateGetPayload(activationId)
    val uuid = elasticSearchConfig.path.format(user.get.namespace.uuid.asString)
    val headers = extractRequiredHeaders(request.get.headers)

    // Return activation from ElasticSearch or from artifact store if required headers are not present
    if (headers.length == elasticSearchConfig.requiredHeaders.length) {
      esClient.search[EsSearchResult](uuid, payload, headers).flatMap {
        case Right(queryResult) =>
          logging.info(this, s"QUERY RESULT: $queryResult")
          val res = transcribeActivations(queryResult)

          if (res.nonEmpty) {
            Future.successful(res.head)
          } else {
            Future.failed(new NoDocumentException("Document not found"))
          }

        case Left(code) =>
          Future.failed(new RuntimeException(s"Status code '$code' was returned from activation store"))
      }
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
    val uuid = elasticSearchConfig.path.format(user.get.namespace.uuid.asString)
    val headers = extractRequiredHeaders(request.get.headers)

    if (headers.length == elasticSearchConfig.requiredHeaders.length) {
      esClient.search[EsSearchResult](uuid, payload, headers).flatMap {
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
    val payload = generateListActiationsMatchNamePayload(name, skip, limit, since, upto)
    val uuid = elasticSearchConfig.path.format(user.get.namespace.uuid.asString)
    val headers = extractRequiredHeaders(request.get.headers)

    if (headers.length == elasticSearchConfig.requiredHeaders.length) {
      esClient.search[EsSearchResult](uuid, payload, headers).flatMap {
        case Right(queryResult) =>
          Future.successful(Right(transcribeActivations(queryResult)))
        case Left(code) =>
          Future.failed(new RuntimeException(s"Status code '$code' was returned from activation store"))
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
    val payload = generateListActivationsInNamespacePayload(namespace, skip, limit, since, upto)
    val uuid = elasticSearchConfig.path.format(user.get.namespace.uuid.asString)
    val headers = extractRequiredHeaders(request.get.headers)

    if (headers.length == elasticSearchConfig.requiredHeaders.length) {
      esClient.search[EsSearchResult](uuid, payload, headers).flatMap {
        case Right(queryResult) =>
          Future.successful(Right(transcribeActivations(queryResult)))
        case Left(code) =>
          Future.failed(new RuntimeException(s"Status code '$code' was returned from activation store"))
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
