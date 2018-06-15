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
import whisk.core.database.{ArtifactStore, CacheChangeNotification, StaleParameter}
import whisk.core.containerpool.logging.ElasticSearchJsonProtocol._

import scala.concurrent.Future
import scala.util.{Failure, Success}

case class ElasticSearchLogFieldConfig(userLogs: String,
                                       message: String,
                                       activationId: String,
                                       activationRecord: String,
                                       stream: String,
                                       time: String)

case class ElasticSearchLogStoreConfig(protocol: String,
                                       host: String,
                                       port: Int,
                                       path: String,
                                       logSchema: ElasticSearchLogFieldConfig,
                                       requiredHeaders: Seq[String] = Seq.empty)

class ArtifactElasticSearchActivationStore(actorSystem: ActorSystem,
                                           actorMaterializer: ActorMaterializer,
                                           logging: Logging)
    extends ActivationStore {
  val elasticSearchConfig = loadConfigOrThrow[ElasticSearchLogStoreConfig](ConfigKeys.elasticSearch)

  implicit val executionContext = actorSystem.dispatcher
  implicit val system = actorSystem

  private val artifactStore: ArtifactStore[WhiskActivation] =
    WhiskActivationStore.datastore()(actorSystem, logging, actorMaterializer)
  private val esClient =
    new ElasticSearchRestClient(elasticSearchConfig.protocol, elasticSearchConfig.host, elasticSearchConfig.port)

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
    //def toFormattedString = s"$name $subject $activationId $version $endDate $status $timeDate $message $duration $namespace"

    def toActivation = {
      // TODO:
      // activation errors?
      // Start and end times?
      // Annotations are in Elasticsearch...

      val result = ActivationResponse.success(Some(message.parseJson.asJsObject))

      WhiskActivation(
        EntityPath(namespace),
        EntityName(name),
        Subject(subject),
        ActivationId(activationId),
        Instant.now,
        Instant.now,
        response = result,
        duration = Some(duration),
        version = SemVer(version))
    }
  }

  object ActivationEntry extends DefaultJsonProtocol {
    implicit val serdes =
      jsonFormat(
        ActivationEntry.apply,
        "name",
        "subject",
        "activationId",
        "version",
        "end_date",
        "status",
        "time_date",
        "message",
        "duration_int",
        "namespace")
  }

  private def transcribeActivation(queryResult: EsSearchResult): WhiskActivation = {
    queryResult.hits.hits.map(_.source.convertTo[ActivationEntry].toActivation).head
  }

  private def extractRequiredHeaders(headers: Seq[HttpHeader]) =
    headers.filter(h => elasticSearchConfig.requiredHeaders.contains(h.lowercaseName)).toList

  def store(activation: WhiskActivation)(implicit transid: TransactionId,
                                         notifier: Option[CacheChangeNotification]): Future[DocInfo] = {
    logging.debug(this, s"recording activation '${activation.activationId}'")

    val res = WhiskActivation.put(artifactStore, activation)

    res onComplete {
      case Success(id) => logging.debug(this, s"recorded activation")
      case Failure(t) =>
        logging.error(
          this,
          s"failed to record activation ${activation.activationId} with error ${t.getLocalizedMessage}")
    }

    res
  }

  def get(activationId: ActivationId, user: Option[Identity] = None, request: Option[HttpRequest] = None)(
    implicit transid: TransactionId): Future[WhiskActivation] = {
    println(s"ACTIVATION ID: $activationId")
    val query =
      s"_type: ${elasticSearchConfig.logSchema.activationRecord} AND ${elasticSearchConfig.logSchema.activationId}: 5ad1c25d24ee4ee691c25d24ee8ee649"
    logging.info(this, s"QUERY STRING: $query")
    val payload = EsQuery(EsQueryString(query))
    val uuid = elasticSearchConfig.path.format(user.get.uuid.asString)
    val headers = extractRequiredHeaders(request.get.headers)

    esClient.search[EsSearchResult](uuid, payload, headers).flatMap {
      case Right(queryResult) =>
        logging.info(this, s"QUERY RESULT: $queryResult")
        Future.successful(transcribeActivation(queryResult))
      case Left(code) =>
        Future.failed(new RuntimeException(s"Status code '$code' was returned from activation store"))
    }

    //WhiskActivation.get(artifactStore, DocId(activationId.asString))
  }

  /**
   * Here there is added overhead of retrieving the specified activation before deleting it, so this method should not
   * be used in production or performance related code.
   */
  def delete(activationId: ActivationId)(implicit transid: TransactionId,
                                         notifier: Option[CacheChangeNotification]): Future[Boolean] = {
    WhiskActivation.get(artifactStore, DocId(activationId.asString)) flatMap { doc =>
      WhiskActivation.del(artifactStore, doc.docinfo)
    }
  }

  def countActivationsInNamespace(namespace: EntityPath,
                                  name: Option[EntityPath] = None,
                                  skip: Int,
                                  since: Option[Instant] = None,
                                  upto: Option[Instant] = None)(implicit transid: TransactionId): Future[JsObject] = {
    WhiskActivation.countCollectionInNamespace(
      artifactStore,
      name.map(p => namespace.addPath(p)).getOrElse(namespace),
      skip,
      since,
      upto,
      StaleParameter.UpdateAfter,
      name.map(_ => WhiskActivation.filtersView).getOrElse(WhiskActivation.view))
  }

  def listActivationsMatchingName(namespace: EntityPath,
                                  name: EntityPath,
                                  skip: Int,
                                  limit: Int,
                                  includeDocs: Boolean = false,
                                  since: Option[Instant] = None,
                                  upto: Option[Instant] = None)(
    implicit transid: TransactionId): Future[Either[List[JsObject], List[WhiskActivation]]] = {
    WhiskActivation.listActivationsMatchingName(
      artifactStore,
      namespace,
      name,
      skip,
      limit,
      includeDocs,
      since,
      upto,
      StaleParameter.UpdateAfter)
  }

  def listActivationsInNamespace(namespace: EntityPath,
                                 skip: Int,
                                 limit: Int,
                                 includeDocs: Boolean = false,
                                 since: Option[Instant] = None,
                                 upto: Option[Instant] = None,
                                 user: Option[Identity] = None,
                                 request: Option[HttpRequest] = None)(
    implicit transid: TransactionId): Future[Either[List[JsObject], List[WhiskActivation]]] = {

    //val querySince = EsQueryRange("@timestamp", EsRangeGt, since.get.toString)
    //val queryUpto = EsQueryRange("@timestamp", EsRangeLt, upto.get.toString)

    val querySince = EsQueryRange("@timestamp", EsRangeGt, "2018-06-15T19:30:39.115Z")
    val queryUpto = EsQueryRange("@timestamp", EsRangeLt, "2018-06-15T19:50:39.115Z")
    val queryTerms = Vector(EsQueryBoolMatch("_type", elasticSearchConfig.logSchema.activationRecord))
    val queryMust = EsQueryMust(queryTerms, Some(Vector(querySince, queryUpto)))

    val payload = EsQuery(queryMust)
    logging.info(this, s"PAYLOAD: $payload")
    logging.info(this, s"PAYLOAD: ${payload.toJson}")
    //val payload = EsQuery(queryMust, Some(EsOrderAsc), Some(querySize), Some(queryFrom))

    val uuid = elasticSearchConfig.path.format(user.get.uuid.asString)
    val headers = extractRequiredHeaders(request.get.headers)

    esClient.search[EsSearchResult](uuid, payload, headers).flatMap {
      case Right(queryResult) =>
        logging.info(this, s"QUERY RESULT: $queryResult")
        Future.successful(transcribeActivation(queryResult))
      case Left(code) =>
        Future.failed(new RuntimeException(s"Status code '$code' was returned from activation store"))
    }

    WhiskActivation.listCollectionInNamespace(
      artifactStore,
      namespace,
      skip,
      limit,
      includeDocs,
      since,
      upto,
      StaleParameter.UpdateAfter)
  }

}

object ArtifactElasticSearchActivationStoreProvider extends ActivationStoreProvider {
  override def instance(actorSystem: ActorSystem, actorMaterializer: ActorMaterializer, logging: Logging) =
    new ArtifactElasticSearchActivationStore(actorSystem, actorMaterializer, logging)
}
