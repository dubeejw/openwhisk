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

package whisk.core.loadBalancer

import akka.actor.Actor
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.util.Failure
import scala.util.Success
import scala.util.matching.Regex.Match

import spray.http.StatusCodes.InternalServerError
import spray.http.StatusCodes.OK
import spray.httpx.SprayJsonSupport._
import spray.json.DefaultJsonProtocol._
import spray.json.JsBoolean
import spray.json.JsObject
import whisk.common.ConsulKV
import whisk.common.ConsulKV.LoadBalancerKeys
import whisk.common.ConsulKVReporter
import whisk.common.Logging
import whisk.common.TransactionId
import whisk.common.Verbosity
import whisk.connector.kafka.KafkaProducerConnector
import whisk.connector.kafka.KafkaConsumerConnector
import whisk.core.WhiskConfig
import whisk.core.connector.{Message, ActivationMessage, CompletionMessage }
import whisk.core.connector.LoadBalancerResponse
import whisk.core.entity.DocInfo
import whisk.http.BasicRasService
import whisk.utils.ExecutionContextFactory

class LoadBalancerService(config: WhiskConfig, verbosity: Verbosity.Level)
    extends LoadBalancerToKafka
    with Logging {

    /** The execution context for futures */
    implicit val executionContext = ExecutionContextFactory.makeExecutionContext()

    /**
     * The two public methods are getInvokerHealth and the inherited doPublish methods.
     */
    def getInvokerHealth(): JsObject = invokerHealth.getInvokerHealth()

    override def getInvoker(message: ActivationMessage): Option[Int] = invokerHealth.getInvoker(message)
    override def activationThrottle = _activationThrottle

    override val producer = new KafkaProducerConnector(config.kafkaHost, executionContext)

    private val kvStore = new ConsulKV(config.consulServer)
    private val invokerHealth = new InvokerHealth(config, { () => producer.sentCount() })
    private val _activationThrottle = new ActivationThrottle(LoadBalancer.config.consulServer, invokerHealth)

    // This must happen after the overrides
    setVerbosity(verbosity)

    private var count = 0
    private val overloadThreshold = 5000 // this is the total across all invokers.  Disable by setting to -1.
    private val reporter = new ConsulKVReporter(kvStore, 3000, 2000,
        LoadBalancerKeys.hostnameKey,
        LoadBalancerKeys.startKey,
        LoadBalancerKeys.statusKey,
        { () =>
            val producedCount = getActivationCount()
            val invokerInfo = invokerHealth.getInvokerActivationCounts()
            val consumedCounts = invokerInfo map {indexCount => indexCount._2}
            val consumedCount = consumedCounts.foldLeft(0)(_ + _)
            val inFlight = producedCount - consumedCount
            val overload = JsBoolean(overloadThreshold > 0 && inFlight >= overloadThreshold)
            count = count + 1
            if (count % 10 == 0) {
                warn(this, s"In flight: $producedCount - $consumedCount = $inFlight $overload")
                info(this, s"Completion counts: [${invokerInfo.mkString(", ")}]")
            }
            Map(LoadBalancerKeys.overloadKey -> overload,
                LoadBalancerKeys.invokerHealth -> getInvokerHealth()) ++
                getUserActivationCounts()
        })

    /**
     * WIP
     *
     * @param msg is the kafka message payload as Json
     */
    def processCompletion(msg: CompletionMessage) = {
        implicit val tid = msg.transid
        val aid = msg.activationId
        info(this, s"LoadBalancerService.processCompletion: activation id $aid")
    }

    val consumer = new KafkaConsumerConnector(config.kafkaHost, "completions", "completed")
    consumer.onMessage((topic, bytes) => {
        val raw = new String(bytes, "utf-8")
        CompletionMessage(raw) match {
            case Success(m) =>
                processCompletion(m)
            case Failure(t) =>
                error(this, s"failed processing message: $raw with $t")
        }
        true
    })

}
