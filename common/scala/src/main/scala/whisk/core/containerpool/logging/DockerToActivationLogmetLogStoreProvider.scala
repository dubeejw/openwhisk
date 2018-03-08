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

import java.nio.file.{Path, Paths}
import java.time.LocalDate
import java.time.format.DateTimeFormatter

import akka.actor.ActorSystem
import akka.http.scaladsl.model.HttpRequest
import akka.http.scaladsl.model.headers._
import akka.http.scaladsl.model.Uri
import akka.stream.scaladsl.Flow
import akka.http.scaladsl.model.HttpResponse

import whisk.common.AkkaLogging
import whisk.core.entity.{ActivationLogs, Identity, WhiskActivation}
import whisk.core.containerpool.logging.ElasticSearchJsonProtocol._
import whisk.core.ConfigKeys

import scala.concurrent.Future
import scala.concurrent.Promise
import scala.util.Try

import spray.json._

import pureconfig._

case class ElasticSearchLogStoreConfig(protocol: String,
                                       host: String,
                                       port: Int,
                                       logMessageField: String,
                                       activationIdField: String)

/**
 * Docker based implementation of a LogStore.
 *
 * Relies on docker's implementation details with regards to the JSON log-driver. When using the JSON log-driver
 * docker writes stdout/stderr to a JSON formatted file which is read by this store. Logs are written in the
 * activation record itself.
 *
 * Additionally writes logs to a separate file which can be processed by any backend service asynchronously.
 */
class DockerToActivationLogmetLogStore(
  system: ActorSystem,
  httpFlow: Option[Flow[(HttpRequest, Promise[HttpResponse]), (Try[HttpResponse], Promise[HttpResponse]), Any]] = None,
  destinationDirectory: Path = Paths.get("logs"),
  elasticSearchConfig: ElasticSearchLogStoreConfig =
    loadConfigOrThrow[ElasticSearchLogStoreConfig](ConfigKeys.elasticSearch))
    extends DockerToActivationFileLogStore(system, destinationDirectory) {
  implicit val actorSystem = system
  implicit val logging = new AkkaLogging(system.log)

  val protocol = elasticSearchConfig.protocol
  val host = elasticSearchConfig.host
  val port = elasticSearchConfig.port
  val logMessageField = elasticSearchConfig.logMessageField
  val activationIdField = elasticSearchConfig.activationIdField

  private val esClient = new ElasticSearchRestClient(protocol, host, port, httpFlow)

  // TODO: error handling
  private def transcribeLogs(queryResult: EsSearchResult): ActivationLogs = {
    val logs = queryResult.hits.hits.map(hit => {
      val userLogEntry = hit.source.convertTo[UserLogEntry]
      s"${userLogEntry.time} ${userLogEntry.stream}: ${userLogEntry.message.stripLineEnd}"
    })

    ActivationLogs(logs)
  }

  // TODO: error handling
  override def fetchLogs(user: Identity, activation: WhiskActivation, request: HttpRequest): Future[ActivationLogs] = {
    //logging.info(this, s"Log API request: $request")

    request.headers.filter(_.lowercaseName == "x-auth-token").lift(0) match {
      case Some(authTokenHeader) =>
        //val projectId = "78bcd3f2-2e1e-4614-bef6-59ff316333a2"
        val projectId = user.uuid.asString
        val headers = List(authTokenHeader, RawHeader("x-auth-project-id", projectId))
        val date = LocalDate.now.format(DateTimeFormatter.ofPattern("yyyy.MM.dd"))
        val path = Uri(s"/elasticsearch/logstash-$projectId-$date/_search")
        val logQuery = s"_type: $logMessageField AND $activationIdField: ${activation.activationId}"
        //val logQuery = s"_type: $logMessageField"
        val queryString = EsQueryString(EsQueryValue(logQuery))
        val sort = Some(Array(EsQueryTimestamp(EsQueryOrder("asc")).toJson))
        val payload = EsQuery(queryString.toJson, sort).toJson.asJsObject

        esClient.post(path, headers, payload).map { e =>
          e match {
            case Right(queryResult) =>
              //logging.info(this, s"Log query response: ${queryResult}") // TODO: delete
              transcribeLogs(queryResult.convertTo[EsSearchResult])
            case Left(code) =>
              throw new RuntimeException(s"Status code '$code' was returned from log store")
          }
        }

      case None =>
        Future.successful(activation.logs)
    }
  }

}

object DockerToActivationLogmetLogStoreProvider extends LogStoreProvider {
  override def logStore(actorSystem: ActorSystem): LogStore = new DockerToActivationLogmetLogStore(actorSystem)
}
