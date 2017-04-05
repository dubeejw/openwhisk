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

package whisk.http

import scala.concurrent.duration.DurationInt
import scala.language.postfixOps

import akka.actor.Actor
//import akka.actor.ActorContext
import akka.actor.ActorSystem
import akka.actor.Props
import akka.event.Logging
//import akka.io.IO
import akka.japi.Creator
//import akka.pattern.ask
import akka.util.Timeout

import akka.http.scaladsl.server.Directives
//import akka.http.scaladsl.server.directives.DebuggingDirectives
//import akka.http.scaladsl.server.directives.DebuggingDirectives._
//import akka.http.scaladsl.server.directives.LoggingMagnet.forMessageFromFullShow
import akka.http.scaladsl.server.directives.LogEntry

//import akka.http.scaladsl.model.ContentType
//import akka.http.scaladsl.model.HttpEntity
import akka.http.scaladsl.model.HttpResponse
//import akka.http.scaladsl.model.MediaTypes.`text/plain`
//import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport.sprayJsonMarshaller
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.model.HttpRequest
//import akka.http.scaladsl.server.RejectionHandler
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.RejectionHandler
/*import spray.can.Http
import spray.http.ContentType
import spray.http.HttpEntity
import spray.http.HttpRequest
import spray.http.HttpResponse
import spray.http.MediaTypes.`text/plain`
import spray.httpx.SprayJsonSupport.sprayJsonMarshaller
import spray.httpx.marshalling

//import spray.httpx.marshalling.ToResponseMarshallable.isMarshallable
import spray.routing.AuthenticationFailedRejection
import spray.routing.Directive.pimpApply
import spray.routing.Directives
//import spray.routing.HttpService
import spray.routing.RejectionHandler
import spray.routing.Route
import spray.routing.directives.DebuggingDirectives
import spray.routing.directives.LogEntry
import spray.routing.directives.LoggingMagnet.forMessageFromFullShow*/

import whisk.common.LogMarker
import whisk.common.LogMarkerToken
import whisk.common.Logging
import whisk.common.LoggingMarkers
import whisk.common.TransactionCounter
import whisk.common.TransactionId

/**
 * This trait extends the spray HttpService trait with logging and transaction counting
 * facilities common to all OpenWhisk REST services.
 */
trait BasicHttpService extends Directives with Actor with TransactionCounter {

    /**
     * Gets the actor context.
     */
    //implicit def actorRefFactory: ActorContext

    /**
     * Gets the logging
     */
    implicit def logging: Logging

    implicit def myRejectionHandler =
        RejectionHandler.default
                .mapRejectionResponse {
                    case res @ HttpResponse(_, _, ent: HttpEntity.Strict, _) =>
                        // since all Akka default rejection responses are Strict this will handle all rejections
                        val message = ent.data.utf8String.replaceAll("\"", """\"""")

                        // we copy the response in order to keep all headers and status code, wrapping the message as hand rolled JSON
                        // you could the entity using your favourite marshalling library (e.g. spray json or anything else)
                        res.copy(entity = HttpEntity(ContentTypes.`application/json`, s"""{"rejection": "$message"}"""))

                    //case CustomRejection(status, cause) :: _ => complete(status, ErrorResponse(cause, transid))

                    case x => x // pass through all other types of responses
                }

    /**
     * Gets the routes implemented by the HTTP service.
     *
     * @param transid the id for the transaction (every request is assigned an id)
     */
    def routes: Route

    /**
     * Gets the log level for a given route. The default is
     * InfoLevel so override as needed.
     *
     * @param the route
     * @return a log level for the route
     */
    def loglevelForRoute(route: String): Logging.LogLevel = Logging.InfoLevel

    /**
     * Receives a message and runs the router.
     */
    def receive = {
        case _ =>

            assignId { implicit transid =>
                //DebuggingDirectives.logRequest(logRequestInfo _) {
                //    DebuggingDirectives.logRequestResponse(logResponseInfo _) {
                Route.seal(routes)
                //    }
                //}
            }
    }

    /** Assigns transaction id to every request. */
    protected val assignId = extract(_ => transid())

    /** Rejection handler to terminate connection on a bad request. Delegates to Spray handler. */

    /*protected def customRejectionHandler(implicit transid: TransactionId) = RejectionHandler {
        case rejections => {
            logging.info(this, s"[REJECT] $rejections")
            rejections match {
                case AuthenticationFailedRejection(cause, challengeHeaders) :: _ =>
                    BasicHttpService.customRejectionHandler.apply(rejections.takeRight(1))
                case _ => BasicHttpService.customRejectionHandler.apply(rejections)
            }
        }
    }*/

    /** Generates log entry for every request. */
    protected def logRequestInfo(req: HttpRequest)(implicit tid: TransactionId): LogEntry = {
        val m = req.method.toString
        val p = req.uri.path.toString
        val q = req.uri.query().toString
        val l = loglevelForRoute(p)
        LogEntry(s"[$tid] $m $p $q", l)
    }

    protected def logResponseInfo(req: HttpRequest)(implicit tid: TransactionId): Any => Option[LogEntry] = {
        case res: HttpResponse =>
            val m = req.method.toString
            val p = req.uri.path.toString
            val l = loglevelForRoute(p)

            val name = "BasicHttpService"

            val token = LogMarkerToken("http", s"${m.toLowerCase}.${res.status.intValue}", LoggingMarkers.count)
            val marker = LogMarker(token, tid.deltaToStart, Some(tid.deltaToStart))

            Some(LogEntry(s"[$tid] [$name] $marker", l))
        case _ => None // other kind of responses
    }
}

object BasicHttpService extends Directives {
    def startService[T <: Actor](system: ActorSystem, name: String, interface: String, port: Integer, service: Creator[T]) = {
        val actor = system.actorOf(Props.create(service), s"$name-service")

        implicit val timeout = Timeout(5 seconds)
        //IO(Http)(system) ? Http.Bind(actor, interface, port)
    }

    /** Rejection handler to terminate connection on a bad request. Delegates to Spray handler. */
    /*def customRejectionHandler(implicit transid: TransactionId) = RejectionHandler {
        // get default rejection message, package it as an ErrorResponse instance
        // which gets serialized into a Json object
        case r if RejectionHandler.Default.isDefinedAt(r) => {
            ctx =>
                RejectionHandler.Default(r) {
                    ctx.withHttpResponseMapped {
                        case resp @ HttpResponse(_, HttpEntity.NonEmpty(ContentType(`text/plain`, _), msg), _, _) =>
                            resp.withEntity(marshalling.marshalUnsafe(ErrorResponse(msg.asString, transid)))
                    }
                }
        }
        case CustomRejection(status, cause) :: _ => complete(status, ErrorResponse(cause, transid))
    }*/
}
