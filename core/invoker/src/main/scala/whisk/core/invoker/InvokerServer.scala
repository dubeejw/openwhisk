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

package whisk.core.invoker

import scala.concurrent.Future
import scala.concurrent.ExecutionContext

import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Route
//import akka.http.scaladsl.server.RouteResult
import akka.stream.ActorMaterializer

import whisk.http.BasicRasService
import whisk.core.WhiskConfig
//import akka.actor.{/*Actor,*/ ActorSystem/*, Props*/}



/**
 * Implements web server to handle certain REST API calls.
 * Currently provides a health ping route, only.
 */
trait InvokerServer
    extends BasicRasService
    /*with Actor*/ {

    //override def actorRefFactory = context

    implicit val whiskConfig: WhiskConfig
    //implicit val actorSystem1: ActorSystem
    implicit val executionContext1: ExecutionContext
    implicit val materializer = ActorMaterializer()
    private implicit val actorSystem2 = context.system


    override def routes: Route = {
        super.routes
    }

    val bindingFuture = {
        //Http().bindAndHandle(RouteResult.route2HandlerFlow(routes), "0.0.0.0", whiskConfig.servicePort.toInt)
        //logging.error(this, "asdf.")

        Http().bindAndHandle(routes, "0.0.0.0", 8080)//whiskConfig.servicePort.toInt)
    }

    def shutdown(): Future[Unit] = {
        bindingFuture.flatMap(_.unbind()).map(_ => ())
    }

    //logging.error(this, "awewersdf.")

}
