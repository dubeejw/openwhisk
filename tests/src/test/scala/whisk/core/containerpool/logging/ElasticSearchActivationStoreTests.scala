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

//import java.time.ZonedDateTime

//import akka.NotUsed
import akka.actor.ActorSystem
/*import akka.http.scaladsl.model.HttpMethods.{GET, POST}
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers.{Accept, RawHeader}*/
import akka.stream.ActorMaterializer
//import akka.stream.scaladsl.Flow
import akka.testkit.TestKit
import common.StreamLogging
import org.junit.runner.RunWith
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FlatSpecLike, Matchers}
import pureconfig.error.ConfigReaderException
//import spray.json._
import whisk.core.entity._
//import whisk.core.entity.size._

/*import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.util.{Success, Try}*/

@RunWith(classOf[JUnitRunner])
class ElasticSearchActivationStoreTests
    extends TestKit(ActorSystem("ElasticSearchActivationStore"))
    with FlatSpecLike
    with Matchers
    with ScalaFutures
    with StreamLogging {

  val materializer = ActorMaterializer()

  behavior of "ElasticSearch Activation Store"

  it should "fail when loading out of box configs since whisk.activationstore.elasticsearch does not exist" in {
    a[ConfigReaderException[_]] should be thrownBy new ArtifactElasticSearchActivationStore(
      system,
      materializer,
      logging)
  }

}
