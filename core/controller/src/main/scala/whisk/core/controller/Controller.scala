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

package whisk.core.controller

import akka.actor._
import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Directives._
import akka.stream.ActorMaterializer

trait ControllerService {
    implicit val system: ActorSystem
    implicit val materializer: ActorMaterializer

    val ping = path("ping") {
        get { complete("pong") }
    }

    val route = ping
}

class Controller(implicit val system: ActorSystem, implicit val materializer: ActorMaterializer) extends ControllerService {
    def startServer(host: String, port: Int) = {
        Http().bindAndHandle(route, host, port)
    }
}

object Controller {
    def main(args: Array[String]): Unit = {
        implicit val actorSystem = ActorSystem("controller-actor-system")
        implicit val materializer = ActorMaterializer()

        val controller = new Controller()
        controller.startServer("0.0.0.0", 8080)
    }
}
