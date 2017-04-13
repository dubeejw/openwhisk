/*
 * Copyright 2015-2016 IBM Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package whisk.core.controller.v2

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Directives._
import akka.stream.ActorMaterializer
import akka.http.scaladsl.model.StatusCodes._

import scala.concurrent.Future

import whisk.common.TransactionId
import whisk.core.WhiskConfig
import whisk.core.WhiskConfig.whiskVersionBuildno
import whisk.core.WhiskConfig.whiskVersionDate
import whisk.core.controller.RestAPIVersion
import whisk.core.entity.WhiskAuthStore
import whisk.common.Logging
import whisk.common.TransactionId

class API(config: WhiskConfig, host: String, port: Int)
        (implicit val actorSystem: ActorSystem, implicit val logger: Logging)
        extends AnyRef
        with Authenticate {
    implicit val materializer = ActorMaterializer()
    implicit val executionContext = actorSystem.dispatcher
    implicit val authStore = WhiskAuthStore.datastore(config)

    implicit val transactionId = TransactionId.unknown

    val apiPath = "api"
    val apiVersion = "v2"

    def prefix = pathPrefix(apiPath / apiVersion)

    // This will go away and be replaced with SwaggerDocs
    val restAPIVersion = new RestAPIVersion("v2", config(whiskVersionDate), config(whiskVersionBuildno)) {
        override def routes(implicit transid: TransactionId) = ???
    }

    val info = (pathEndOrSingleSlash & get) {
        complete(OK, restAPIVersion.info.toString)
    }

    val routes = {
        prefix {
            customBasicAuth("OpenWhisk secure realm")(validateCredentials2) { user =>
                info
            }
        }
    }

    val bindingFuture = {
        Http().bindAndHandle(routes, host, port)
    }

    def shutdown(): Future[Unit] = {
        bindingFuture.flatMap(_.unbind()).map(_ => ())
    }

}
