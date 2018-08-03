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

package whisk.core.database

import java.nio.file.{Path, Paths}
import java.time.Instant

import akka.NotUsed
import akka.actor.ActorSystem
import akka.http.scaladsl.model._
import akka.stream.alpakka.file.scaladsl.LogRotatorSink
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, Keep, MergeHub, Sink, Source}
import akka.stream._
import akka.util.ByteString
import pureconfig.loadConfigOrThrow
import spray.json.{DefaultJsonProtocol, _}
import whisk.common.{Logging, TransactionId}
import whisk.core.ConfigKeys
import whisk.core.containerpool.logging.ElasticSearchJsonProtocol._
import whisk.core.containerpool.logging.{ElasticSearchRestClient, EsQuery, EsQueryString, EsSearchResult, _}
import whisk.core.entity._
import whisk.core.entity.size._

import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.Try

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
                             duration: Option[Long] = None,
                             namespace: String,
                             kind: Option[String] = None,
                             cause: Option[String] = None,
                             causedBy: Option[String] = None,
                             limits: Option[ActionLimits] = None,
                             path: Option[String] = None,
                             components: Option[JsArray] = None,
                             components2: Option[JsArray] = None) {

    def toActivation(logs: ActivationLogs = ActivationLogs()) = {
      val result = status match {
        case "0" => ActivationResponse.success(Some(message.parseJson.asJsObject))
        case "1" => ActivationResponse.applicationError(message.parseJson.asJsObject.fields("error"))
        case "2" => ActivationResponse.containerError(message.parseJson.asJsObject.fields("error"))
        case "3" => ActivationResponse.whiskError(message.parseJson.asJsObject.fields("error"))
      }
      val causeByAnnotation: Parameters = causedBy match {
        case Some(value) => Parameters("causedBy", value)
        case None        => Parameters()
      }

      val memoryAnnotation: Parameters = limits match {
        case Some(value) =>
          Parameters(
            "limits",
            JsObject(
              "memory" -> value.memory.megabytes.toJson,
              "timeout" -> value.timeout.toJson,
              "logs" -> value.logs.toJson))
        case None => Parameters()
      }

      val kindAnnotation: Parameters = kind match {
        case Some(value) => Parameters("kind", value)
        case None        => Parameters()
      }

      val pathAnnotation = path match {
        case Some(value) => Parameters("path", value)
        case None        => Parameters()
      }

      val componentAnnotation = components match {
        case Some(value) => Parameters("components", value)
        case None        => Parameters()
      }

      val componentAnnotation2 = components2 match {
        case Some(value) => Parameters("components", value)
        case None        => Parameters()
      }

      val annotations = kindAnnotation ++ causeByAnnotation ++ memoryAnnotation ++ pathAnnotation ++ componentAnnotation ++ componentAnnotation2

      val c: Option[ActivationId] = cause match {
        case Some(value) => Some(ActivationId(value))
        case None        => None
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
        duration = duration,
        version = SemVer(version),
        annotations = annotations,
        cause = c)
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
        elasticSearchConfig2.schema.namespace,
        "kind",
        "cause",
        "causedBy",
        "limits",
        "path",
        "components",
        "components2")
  }

  protected def transcribeActivations(queryResult: EsSearchResult): List[ActivationEntry] = {
    val activations = queryResult.hits.hits.map(_.source.convertTo[ActivationEntry]).toList
    activations.sortWith(_.timeDate > _.timeDate)
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

  protected def generateGetPayload(activationId: String) = {
    val query =
      s"_type: ${elasticSearchConfig2.schema.activationRecord} AND ${elasticSearchConfig2.schema.activationId}: $activationId"

    EsQuery(EsQueryString(query))
  }

  protected def generateCountActivationsInNamespacePayload(name: Option[EntityPath] = None,
                                                           skip: Int,
                                                           since: Option[Instant] = None,
                                                           upto: Option[Instant] = None) = {
    val queryRanges = getRanges(since, upto)
    val activationMatch = Some(EsQueryBoolMatch("_type", elasticSearchConfig2.schema.activationRecord))
    val entityMatch: Option[EsQueryBoolMatch] = name.map { n =>
      Some(EsQueryBoolMatch(elasticSearchConfig2.schema.name, n.asString))
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
    val payload = generateGetPayload(activationId)

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

  def count(uuid: String,
            name: Option[EntityPath] = None,
            namespace: String,
            skip: Int,
            since: Option[Instant] = None,
            upto: Option[Instant] = None,
            headers: List[HttpHeader] = List.empty)(implicit transid: TransactionId): Future[JsObject] = {
    val payload = generateCountActivationsInNamespacePayload(name, skip, since, upto)

    esActivationClient.search[EsSearchResult](uuid, payload, headers).flatMap {
      case Right(queryResult) =>
        val total = Math.max(0, queryResult.hits.total - skip)
        Future.successful(JsObject("activations" -> total.toJson))
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

  implicit val m = actorMaterializer

  val destinationDirectory: Path = Paths.get("logs")
  val bufferSize = 100.MB

  protected val writeToFile: Sink[ByteString, _] = MergeHub
    .source[ByteString]
    .batchWeighted(bufferSize.toBytes, _.length, identity)(_ ++ _)
    .to(LogRotatorSink(() => {
      val maxSize = bufferSize.toBytes
      var bytesRead = maxSize
      element =>
        {
          val size = element.size
          if (bytesRead + size > maxSize) {
            bytesRead = size
            Some(destinationDirectory.resolve(s"userlogs-${Instant.now.toEpochMilli}.log"))
          } else {
            bytesRead += size
            None
          }
        }
    }))
    .run()(actorMaterializer)

  val toFormattedString: Flow[ByteString, String, NotUsed] =
    Flow[ByteString].map(_.utf8String.parseJson.convertTo[LogLine].toFormattedString)

  private def fieldsString(fields: Map[String, JsValue]) =
    fields
      .map {
        case (key, value) => s""""$key":${value.compactPrint}"""
      }
      .mkString(",")

  private val eventEnd = ByteString("}\n")

  // TODO: Need correct stream for stdout/stderr
  def logs(activation: WhiskActivation): Source[ByteString, NotUsed] = {
    val logLine = LogLine(Instant.now.toString, "stdout", activation.logs.toJson.compactPrint)
    val a = activation.logs.logs.map { log =>
      val logLine = LogLine(Instant.now.toString, "stdout", log.substring(39)).toJson.compactPrint
      ByteString(logLine)
    }

    Source.fromIterator(() => a.toIterator)
  }

  def writeLog(activation: WhiskActivation) = {
    // Adding the userId field to every written record, so any background process can properly correlate.
    val userIdField = Map("namespaceId" -> activation.namespace.toJson)

    // What todo with action name?
    val additionalMetadata = Map(
      "activationId" -> activation.activationId.asString.toJson,
      "entity" -> FullyQualifiedEntityName(activation.namespace, activation.name).asString.toJson) ++ userIdField

    val activationWithNoLogs = activation.withoutLogs
    val augmentedActivation = JsObject(activationWithNoLogs.toJson.fields ++ userIdField)

    // Manually construct JSON fields to omit parsing the whole structure
    val metadata = ByteString("," + fieldsString(additionalMetadata))

    val toSeq: Sink[ByteString, Future[immutable.Seq[String]]] =
      Flow[ByteString].via(toFormattedString).toMat(Sink.seq[String])(Keep.right)

    val toFile = Flow[ByteString]
    // As each element is a JSON-object, we know we can add the manually constructed fields to it by dropping
    // the closing "}", adding the fields and finally add "}\n" to the end again.
      .map(_.dropRight(1) ++ metadata ++ eventEnd)
      // As the last element of the stream, print the activation record.
      .concat(Source.single(ByteString(augmentedActivation.toJson.compactPrint + "\n")))
      .to(writeToFile)

    val combined: Sink[ByteString, (Future[immutable.Seq[String]], NotUsed)] =
      OwSink.combine(toSeq, toFile)(Broadcast[ByteString](_))

    logs(activation).runWith(combined)._1.flatMap { seq =>
      val possibleErrors = Set("Some error", "Some other error")
      val errored = seq.lastOption.exists(last => possibleErrors.exists(last.contains))
      val logs = ActivationLogs(seq.toVector)
      if (!errored) {
        Future.successful(logs)
      } else {
        Future.failed(new Exception("some error"))
      }
    }
  }

  override def store(activation: WhiskActivation)(implicit transid: TransactionId,
                                                  notifier: Option[CacheChangeNotification]): Future[DocInfo] = {
    writeLog(activation)
    super.store(activation)
  }

  override def get(activationId: ActivationId, user: Option[Identity] = None, request: Option[HttpRequest] = None)(
    implicit transid: TransactionId): Future[WhiskActivation] = {
    val headers = extractRequiredHeaders2(request.get.headers)

    // Return activation from ElasticSearch or from artifact store if required headers are not present
    if (headers.length == elasticSearchConfig2.requiredHeaders.length) {
      val uuid = elasticSearchConfig2.path.format(user.get.namespace.uuid.asString)
      val headers = extractRequiredHeaders2(request.get.headers)
      val id = activationId.asString.substring(activationId.asString.indexOf("/") + 1)

      getActivation(id, uuid, headers).flatMap(activation =>
        logs(uuid, id, headers).map(logs =>
          activation.toActivation(ActivationLogs(logs.map(l => l.toFormattedString)))))
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
      count(uuid, name, namespace.asString, skip, since, upto, headers)
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
            logs(uuid, act.activationId, headers).map { logs =>
              act.toActivation(ActivationLogs(logs.map(l => l.toFormattedString)))
            }
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
            logs(uuid, act.activationId, headers).map { logs =>
              act.toActivation(ActivationLogs(logs.map(l => l.toFormattedString)))
            }
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

object OwSink {

  /**
   * Combines two sinks into one sink using the given strategy. The materialized value is a Tuple2 of the materialized
   * values of either sink. Code basically copied from {@code Sink.combine}
   */
  def combine[T, U, M1, M2](first: Sink[U, M1], second: Sink[U, M2])(
    strategy: Int ⇒ Graph[UniformFanOutShape[T, U], NotUsed]): Sink[T, (M1, M2)] = {
    Sink.fromGraph(GraphDSL.create(first, second)((_, _)) { implicit b => (s1, s2) =>
      import GraphDSL.Implicits._
      val d = b.add(strategy(2))

      d ~> s1
      d ~> s2

      SinkShape(d.in)
    })
  }
}
