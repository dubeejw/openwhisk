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
import akka.http.scaladsl.model.Uri
import akka.stream.scaladsl.Flow
import akka.http.scaladsl.model.HttpResponse
import akka.http.scaladsl.model._

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
                                       path: String,
                                       logMessageField: String,
                                       activationIdField: String,
                                       streamField: String,
                                       actionField: String,
                                       requiredHeaders: String = "")

/**
 * Docker based implementation of a LogStore.
 *
 * Relies on docker's implementation details with regards to the JSON log-driver. When using the JSON log-driver
 * docker writes stdout/stderr to a JSON formatted file which is read by this store. Logs are written in the
 * activation record itself.
 *
 * Additionally writes logs to a separate file which can be processed by any backend service asynchronously.
 */
class ElasticSearchLogStore(
  system: ActorSystem,
  httpFlow: Option[Flow[(HttpRequest, Promise[HttpResponse]), (Try[HttpResponse], Promise[HttpResponse]), Any]] = None,
  destinationDirectory: Path = Paths.get("logs"),
  elasticSearchConfig: ElasticSearchLogStoreConfig =
    loadConfigOrThrow[ElasticSearchLogStoreConfig](ConfigKeys.elasticSearch))
    extends DockerToActivationFileLogStore(system, destinationDirectory) {

  // Schema of logs in ES
  case class UserLogEntry(message: String, stream: String, time: String, action: String)

  object UserLogEntry extends DefaultJsonProtocol {
    implicit val serdes =
      jsonFormat(
        UserLogEntry.apply,
        "message",
        elasticSearchConfig.streamField,
        "time_date",
        elasticSearchConfig.actionField)
  }

  implicit val actorSystem = system
  implicit val logging = new AkkaLogging(system.log)

  private val esClient = new ElasticSearchRestClient(
    elasticSearchConfig.protocol,
    elasticSearchConfig.host,
    elasticSearchConfig.port,
    httpFlow)
  private val requiredHeaders =
    if (elasticSearchConfig.requiredHeaders.isEmpty)
      Array[String]()
    else
      elasticSearchConfig.requiredHeaders.split(",")
  private val logQuery =
    s"_type: ${elasticSearchConfig.logMessageField} AND ${elasticSearchConfig.activationIdField}: %s"

  private def transcribeLogs(queryResult: EsSearchResult): ActivationLogs = {
    val logs = queryResult.hits.hits.map(hit => {
      val userLogEntry = hit.source.convertTo[UserLogEntry]
      s"${userLogEntry.time} ${userLogEntry.stream}: ${userLogEntry.message.stripLineEnd}"
    })

    ActivationLogs(logs)
  }

  private def extractRequiredHeaders(request: HttpRequest) =
    request.headers.filter {
      case header: HttpHeader if requiredHeaders.contains(header.lowercaseName) => true
      case _                                                                    => false
    }.toList

  private def generatePayload(activation: WhiskActivation) = {
    val queryString = EsQueryString(logQuery.format(activation.activationId))
    val queryOrder = EsQueryOrder("time_date", EsOrderAsc)

    EsQuery(queryString, Some(queryOrder)).toJson.asJsObject
  }

  private def generatePath(user: Identity) = {
    Uri(
      elasticSearchConfig.path
        .replace("$UUID", user.uuid.asString)
        .replace("$DATE", LocalDate.now.format(DateTimeFormatter.ofPattern("yyyy.MM.dd"))))
  }

  override def fetchLogs(user: Identity, activation: WhiskActivation, request: HttpRequest): Future[ActivationLogs] = {
    val headers = extractRequiredHeaders(request)

    // Return logs from ElasticSearch, or return logs from activation if required headers are not present
    if (headers.length == requiredHeaders.length) {
      esClient.post(generatePath(user), headers, generatePayload(activation)).flatMap { response =>
        response match {
          case Right(queryResult) =>
            Future.successful(transcribeLogs(queryResult.convertTo[EsSearchResult]))
          case Left(code) =>
            Future.failed(new RuntimeException(s"Status code '$code' was returned from log store"))
        }
      }
    } else {
      Future.successful(activation.logs)
    }
  }
}

object ElasticSearchLogStoreProvider extends LogStoreProvider {
  override def logStore(actorSystem: ActorSystem): LogStore = new ElasticSearchLogStore(actorSystem)
}
