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

import java.nio.file.{Path, Paths}
import java.time.Instant

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.alpakka.file.scaladsl.LogRotatorSink
import akka.util.ByteString

import spray.json._
import spray.json.DefaultJsonProtocol._
import whisk.common.{Logging, TransactionId}
import whisk.core.containerpool.logging.LogLine
import whisk.core.database.{ArtifactStore, CacheChangeNotification, StaleParameter}
import whisk.core.entity.size._

import scala.concurrent.Future
import scala.util.{Failure, Success}

import akka.NotUsed
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, Keep, MergeHub, Sink, Source}
import akka.stream.{Graph, SinkShape, UniformFanOutShape}

class ArtifactActivationStore(actorSystem: ActorSystem, actorMaterializer: ActorMaterializer, logging: Logging)
    extends ActivationStore {

  implicit val executionContext = actorSystem.dispatcher
  implicit val m = actorMaterializer

  private val artifactStore: ArtifactStore[WhiskActivation] =
    WhiskActivationStore.datastore()(actorSystem, logging, actorMaterializer)

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

  def logs(activation: WhiskActivation): Source[ByteString, NotUsed] = {
    // What todo about stream?
    val logLine = LogLine(Instant.now.toString, "stdout", activation.logs.toJson.compactPrint)
    Source.single(ByteString(logLine.toJson.compactPrint))
  }

  def writeLog(activation: WhiskActivation) = {

    // Adding the userId field to every written record, so any background process can properly correlate.
    val userIdField = Map("namespaceId" -> activation.namespace.toJson)

    // What todo with action name?
    val additionalMetadata = Map(
      "activationId" -> activation.activationId.asString.toJson,
      "entity" -> FullyQualifiedEntityName(activation.namespace, activation.name).asString.toJson) ++ userIdField

    val augmentedActivation = JsObject(activation.toJson.fields ++ userIdField)

    // Manually construct JSON fields to omit parsing the whole structure
    val metadata = ByteString("," + fieldsString(additionalMetadata))

    val toSeq = Flow[ByteString].via(toFormattedString).toMat(Sink.seq[String])(Keep.right)

    val toFile = Flow[ByteString]
    // As each element is a JSON-object, we know we can add the manually constructed fields to it by dropping
    // the closing "}", adding the fields and finally add "}\n" to the end again.
      .map(_.dropRight(1) ++ metadata ++ eventEnd)
      // As the last element of the stream, print the activation record.
      .concat(Source.single(ByteString(augmentedActivation.toJson.compactPrint + "\n")))
      .to(writeToFile)

    val combined = OwSink.combine(toSeq, toFile)(Broadcast[ByteString](_))

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

    writeLog(activation)
    res
  }

  def get(activationId: ActivationId)(implicit transid: TransactionId): Future[WhiskActivation] = {
    WhiskActivation.get(artifactStore, DocId(activationId.asString))
  }

  def delete(activationId: ActivationId)(implicit transid: TransactionId,
                                         notifier: Option[CacheChangeNotification]): Future[Boolean] = {
    WhiskActivation.get(artifactStore, DocId(activationId.asString)) flatMap { doc =>
      WhiskActivation.del(artifactStore, doc.docinfo)
    }
  }

  def countActivationsInNamespace(name: Option[Option[EntityPath]] = None,
                                  namespace: EntityPath,
                                  skip: Int,
                                  since: Option[Instant] = None,
                                  upto: Option[Instant] = None)(implicit transid: TransactionId): Future[JsObject] = {
    WhiskActivation.countCollectionInNamespace(
      artifactStore,
      name.flatten.map(p => namespace.addPath(p)).getOrElse(namespace),
      skip,
      since,
      upto,
      StaleParameter.UpdateAfter,
      name.flatten.map(_ => WhiskActivation.filtersView).getOrElse(WhiskActivation.view))
  }

  def listActivationsMatchingName(namespace: EntityPath,
                                  path: EntityPath,
                                  skip: Int,
                                  limit: Int,
                                  includeDocs: Boolean = false,
                                  since: Option[Instant] = None,
                                  upto: Option[Instant] = None)(
    implicit transid: TransactionId): Future[Either[List[JsObject], List[WhiskActivation]]] = {
    WhiskActivation.listActivationsMatchingName(
      artifactStore,
      namespace,
      path,
      skip,
      limit,
      includeDocs,
      since,
      upto,
      StaleParameter.UpdateAfter)
  }

  def listActivationsInNamespace(path: EntityPath,
                                 skip: Int,
                                 limit: Int,
                                 includeDocs: Boolean = false,
                                 since: Option[Instant] = None,
                                 upto: Option[Instant] = None)(
    implicit transid: TransactionId): Future[Either[List[JsObject], List[WhiskActivation]]] = {
    WhiskActivation.listCollectionInNamespace(
      artifactStore,
      path,
      skip,
      limit,
      includeDocs,
      since,
      upto,
      StaleParameter.UpdateAfter)
  }

}

object ArtifactActivationStoreProvider extends ActivationStoreProvider {
  override def activationStore(actorSystem: ActorSystem, actorMaterializer: ActorMaterializer, logging: Logging) =
    new ArtifactActivationStore(actorSystem, actorMaterializer, logging)
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
