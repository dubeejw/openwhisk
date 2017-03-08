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

package whisk.core.controller

import akka.actor.Actor
import akka.actor.ActorContext
import akka.actor.ActorSystem
import akka.japi.Creator
import spray.routing.Directive.pimpApply
import spray.routing.Route
import whisk.common.AkkaLogging
import whisk.common.Logging
import whisk.common.TransactionId
import whisk.core.WhiskConfig
import whisk.core.entitlement.EntitlementProvider
import whisk.core.loadBalancer.LoadBalancerService
import whisk.http.BasicHttpService
import whisk.http.BasicRasService
import whisk.core.entity._
import whisk.core.entitlement._
import whisk.core.entity.ActivationId.ActivationIdGenerator

/**
 * The Controller is the service that provides the REST API for OpenWhisk.
 *
 * It extends the BasicRasService so it includes a ping endpoint for monitoring.
 *
 * Spray sends messages to akka Actors -- the Controller is an Actor, ready to receive messages.
 *
 * @Idioglossia uses the spray-routing DSL
 * http://spray.io/documentation/1.1.3/spray-routing/advanced-topics/understanding-dsl-structure/
 *
 * @param config A set of properties needed to run an instance of the controller service
 * @param instance if running in scale-out, a unique identifier for this instance in the group
 * @param verbosity logging verbosity
 * @param executionContext Scala runtime support for concurrent operations
 */
class Controller(
    config: WhiskConfig,
    instance: Int,
    implicit val logging: Logging)
    extends BasicRasService
    with Actor {

    // each akka Actor has an implicit context
    override def actorRefFactory: ActorContext = context

    /**
     * A Route in spray is technically a function taking a RequestContext as a parameter.
     *
     * @Idioglossia The ~ spray DSL operator composes two independent Routes, building a routing
     * tree structure.
     * @see http://spray.io/documentation/1.2.3/spray-routing/key-concepts/routes/#composing-routes
     */
    override def routes(implicit transid: TransactionId): Route = {
        // handleRejections wraps the inner Route with a logical error-handler for
        // unmatched paths
        handleRejections(customRejectionHandler) {
            super.routes ~ apiv1.routes ~ apiv2.routes
        }
    }

    logging.info(this, s"starting controller instance ${instance}")

    // initialize datastores
    implicit val actorSystem = context.system
    implicit val executionContext = actorSystem.dispatcher
    implicit val whiskConfig = config
    implicit val authStore = WhiskAuthStore.datastore(whiskConfig)
    implicit val entityStore = WhiskEntityStore.datastore(whiskConfig)
    implicit val activationStore = WhiskActivationStore.datastore(whiskConfig)

    // initialize backend services
    implicit val loadBalancer = new LoadBalancerService(whiskConfig)
    implicit val consulServer = whiskConfig.consulServer
    implicit val entitlementProvider = new LocalEntitlementProvider(whiskConfig, loadBalancer)
    implicit val activationIdFactory = new ActivationIdGenerator {}

    // register collections and set verbosities on datastores and backend services
    Collection.initialize(entityStore)

    /** The REST APIs. */
    private val apiv1 = new RestAPIVersion_v1(config)
    private val apiv2 = new RestAPIVersion_v2(config)
}

/**
 * Singleton object provides a factory to create and start an instance of the Controller service.
 */
object Controller {

    // requiredProperties is a Map whose keys define properties that must be bound to
    // a value, and whose values are default values.   A null value in the Map means there is
    // no default value specified, so it must appear in the properties file
    def requiredProperties = Map(WhiskConfig.servicePort -> 8080.toString) ++
        RestAPIVersion_v1.requiredProperties ++
        RestAPIVersion_v2.requiredProperties ++
        LoadBalancerService.requiredProperties ++
        EntitlementProvider.requiredProperties

    def optionalProperties = EntitlementProvider.optionalProperties

    // akka-style factory to create a Controller object
    private class ServiceBuilder(config: WhiskConfig, instance: Int, logging: Logging) extends Creator[Controller] {
        def create = new Controller(config, instance, logging)
    }

    def main(args: Array[String]): Unit = {
        implicit val system = ActorSystem("controller-actor-system")
        implicit val logging: Logging = new AkkaLogging(akka.event.Logging.getLogger(system, this))

        // extract configuration data from the environment
        val config = new WhiskConfig(requiredProperties, optionalProperties)

        // if deploying multiple instances (scale out), must pass the instance number as the
        // second argument.  (TODO .. seems fragile)
        val instance = if (args.length > 0) args(1).toInt else 0

        if (config.isValid) {
            val port = config.servicePort.toInt
            BasicHttpService.startService(system, "controller", "0.0.0.0", port, new ServiceBuilder(config, instance, logging))
        }
    }
}
