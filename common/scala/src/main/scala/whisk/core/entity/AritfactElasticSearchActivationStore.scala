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
import spray.json.{DefaultJsonProtocol, JsObject}

import whisk.common.{Logging, TransactionId}
import whisk.core.ConfigKeys
import whisk.core.containerpool.logging.{ElasticSearchRestClient, EsQuery, EsQueryString, EsSearchResult}
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

class ArtifactElasticSearchActivationStore(actorSystem: ActorSystem, actorMaterializer: ActorMaterializer, logging: Logging)
    extends ActivationStore {
  val elasticSearchConfig = loadConfigOrThrow[ElasticSearchLogStoreConfig](ConfigKeys.elasticSearch)

  // Schema of resultant logs from ES
  case class UserLogEntry(message: String, stream: String, time: String) {
    def toFormattedString = s"${time} ${stream}: ${message.stripLineEnd}"
  }

  object UserLogEntry extends DefaultJsonProtocol {
    implicit val serdes =
      jsonFormat(
        UserLogEntry.apply,
        elasticSearchConfig.logSchema.message,
        elasticSearchConfig.logSchema.stream,
        elasticSearchConfig.logSchema.time)
  }

  implicit val executionContext = actorSystem.dispatcher
  implicit val system = actorSystem

  private val artifactStore: ArtifactStore[WhiskActivation] =
    WhiskActivationStore.datastore()(actorSystem, logging, actorMaterializer)
  private val esClient = new ElasticSearchRestClient(
    elasticSearchConfig.protocol,
    elasticSearchConfig.host,
    elasticSearchConfig.port)

  /*
  {
    "namespace": "lime@us.ibm.com",
    "name": "logs",
    "version": "0.0.1",
    "subject": "lime@us.ibm.com",
    "activationId": "1b24d673e2524c70a4d673e252bc7061",
    "start": 1529001213883,
    "end": 1529001214403,
    "duration": 520,
    "response": {
        "status": "success",
        "statusCode": 0,
        "success": true,
        "result": {
            "yee": "haww"
        }
    },
    "logs": [
        "2018-06-14T18:33:34.392390114Z stdout: some log stuff",
        "2018-06-14T18:33:34.397493528Z stdout: more logs"
    ],
    "annotations": [
        {
            "key": "path",
            "value": "lime@us.ibm.com/logs"
        },
        {
            "key": "waitTime",
            "value": 68
        },
        {
            "key": "kind",
            "value": "nodejs:6"
        },
        {
            "key": "limits",
            "value": {
                "logs": 10,
                "memory": 256,
                "timeout": 60000
            }
        },
        {
            "key": "initTime",
            "value": 486
        }
    ],
    "publish": false
}

   */
  private def transcribeLogs(queryResult: EsSearchResult): WhiskActivation = {
    //val b: Seq[Any] = queryResult.hits.hits.map(_.source.convertTo[UserLogEntry])
    //val a: Seq[String] = queryResult.hits.hits.map(_.source.convertTo[UserLogEntry].toFormattedString)
    //println(b.message)
    /*
    case class WhiskActivation(namespace: EntityPath,
                           override val name: EntityName,
                           subject: Subject,
                           activationId: ActivationId,
                           start: Instant,
                           end: Instant,
                           cause: Option[ActivationId] = None,
                           response: ActivationResponse = ActivationResponse.success(),
                           logs: ActivationLogs = ActivationLogs(),
                           version: SemVer = SemVer(),
                           publish: Boolean = false,
                           annotations: Parameters = Parameters(),
                           duration: Option[Long] = None)
     */
    val namespace = EntityPath("namespace")
    val entity = EntityName("name")
    val subject = Subject()
    val activationId = ActivationId("1b24d673e2524c70a4d673e252bc7061")
    val start = Instant.now
    val end = Instant.now
    WhiskActivation(namespace, entity, subject, activationId, start, end)
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

  def get(activationId: ActivationId, user: Option[Identity] = None, request: Option[HttpRequest] = None)(implicit transid: TransactionId): Future[WhiskActivation] = {
    val query = s"_type: ${elasticSearchConfig.logSchema.activationRecord} AND ${elasticSearchConfig.logSchema.activationId}: 084102f943d949928102f943d9299276"
    logging.info(this, s"QUERY STRING: $query")
    val payload = EsQuery(EsQueryString(query))
    val uuid = elasticSearchConfig.path.format(user.get.uuid.asString)
    val headers = extractRequiredHeaders(request.get.headers)

    esClient.search[EsSearchResult](uuid, payload, headers).flatMap {
      case Right(queryResult) =>
        logging.info(this, s"QUERY RESULT: $queryResult")
        Future.successful(transcribeLogs(queryResult))
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
                                 upto: Option[Instant] = None)(
                                    implicit transid: TransactionId): Future[Either[List[JsObject], List[WhiskActivation]]] = {
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
