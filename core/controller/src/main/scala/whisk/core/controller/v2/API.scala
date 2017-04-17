package whisk.core.controller.v2

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Directives._
import akka.stream.ActorMaterializer
import akka.http.scaladsl.model.StatusCodes._

import scala.concurrent.Future
import scala.concurrent.ExecutionContext

import whisk.common.TransactionId
import whisk.core.WhiskConfig
import whisk.core.WhiskConfig.whiskVersionBuildno
import whisk.core.WhiskConfig.whiskVersionDate
import whisk.core.controller.RestAPIVersion
import whisk.core.entity.WhiskAuthStore
import whisk.common.Logging
import whisk.common.TransactionId
import whisk.core.controller._
import whisk.core.entity._
import whisk.core.entity.types._
import whisk.core.entitlement.v2._


class API(config: WhiskConfig, host: String, port: Int)(
        implicit val actorSystem: ActorSystem,
        implicit val logger: Logging,
        implicit val entityStore: EntityStore,
        implicit val entitlementProvider: EntitlementProvider)
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
            info ~ customBasicAuth("OpenWhisk secure realm", validateCredentials) { user =>
                namespaces.routes(user)
            }
        }
    }

    val bindingFuture = {
        Http().bindAndHandle(routes, host, port)
    }

    def shutdown(): Future[Unit] = {
        bindingFuture.flatMap(_.unbind()).map(_ => ())
    }

    private val namespaces = new NamespacesApi(apiPath, apiVersion)

    class NamespacesApi(
       val apiPath: String,
       val apiVersion: String)(
       implicit override val entityStore: EntityStore,
       override val entitlementProvider: EntitlementProvider,
       override val executionContext: ExecutionContext,
       override val logging: Logging)
    extends WhiskNamespacesApi
}
