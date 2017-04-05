package whisk.core.controller.v2

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
//import akka.http.scaladsl.model.{ContentTypes, HttpEntity}
import akka.http.scaladsl.server.Directives._
import akka.stream.ActorMaterializer
import akka.http.scaladsl.model.StatusCodes._

import scala.concurrent.Future

import whisk.common.TransactionId
import whisk.core.WhiskConfig
import whisk.core.WhiskConfig.whiskVersionBuildno
import whisk.core.WhiskConfig.whiskVersionDate
import whisk.core.controller.RestAPIVersion
import whisk.common.AkkaLogging

//import whisk.core.entity.WhiskActivationStore
//import whisk.core.entity.WhiskAuthStore

class API(config: WhiskConfig, host: String, port: Int, logger: AkkaLogging)
     (implicit val actorSystem: ActorSystem) extends AnyRef {
    implicit val materializer = ActorMaterializer()
    implicit val executionContext = actorSystem.dispatcher

    val restAPIVersion = new RestAPIVersion("v2", config(whiskVersionDate), config(whiskVersionBuildno)) {
        override def routes(implicit transid: TransactionId) = ???
    }

    val infoRoute = path("api" / "v2") {
        get {
            complete(OK, restAPIVersion.info.toString)
        }
    }

    val allRoutes = {
        extractRequest { request =>
            logger.info(this, request.uri.toString)
            infoRoute
        }
    }

    val bindingFuture = {
        Http().bindAndHandle(allRoutes, host, port)
    }

    def shutdown(): Future[Unit] = {
        bindingFuture.flatMap(_.unbind()).map(_ => ())
    }
}