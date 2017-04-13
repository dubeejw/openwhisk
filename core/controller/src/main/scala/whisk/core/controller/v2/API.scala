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
