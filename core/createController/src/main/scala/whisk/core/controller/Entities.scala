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

package whisk.core.createController

import scala.concurrent.Future
import scala.language.postfixOps
import scala.util.Try

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport._
import akka.http.scaladsl.model.StatusCodes.RequestEntityTooLarge
import akka.http.scaladsl.server.Directive0
import akka.http.scaladsl.server.Directives
import akka.http.scaladsl.server.RequestContext
import akka.http.scaladsl.server.Route
import akka.http.scaladsl.server.RouteResult
import spray.json.JsonPrinter
import whisk.common.TransactionId
import whisk.core.entitlement.Privilege._
import whisk.core.entitlement.Privilege
import whisk.core.entitlement.Resource
import whisk.core.entity._
import whisk.core.entity.ActivationEntityLimit
import whisk.core.entity.size._
import whisk.http.ErrorResponse.terminate
import whisk.http.Messages

protected[createController] trait ValidateRequestSize extends Directives {

  protected def validateSize(check: => Option[SizeError])(implicit tid: TransactionId, jsonPrinter: JsonPrinter) =
    new Directive0 {
      override def tapply(f: Unit => Route) = {
        check map {
          case e: SizeError => terminate(RequestEntityTooLarge, Messages.entityTooBig(e))
        } getOrElse f(None)
      }
    }

  /** Checks if request entity is within allowed length range. */
  protected def isWhithinRange(length: Long) = {
    if (length <= allowedActivationEntitySize) {
      None
    } else
      Some {
        SizeError(fieldDescriptionForSizeError, length.B, allowedActivationEntitySize.B)
      }
  }

  protected val allowedActivationEntitySize: Long = ActivationEntityLimit.MAX_ACTIVATION_ENTITY_LIMIT.toBytes
  protected val fieldDescriptionForSizeError = "Request"
}

/** A trait implementing the basic operations on WhiskEntities in support of the various APIs. */
trait WhiskCollectionAPI
    extends Directives
    with AuthenticatedRouteProvider
    with AuthorizedRouteProvider
    with ValidateRequestSize
    with ReadOps
    with WriteOps {

  /** The core collections require backend services to be injected in this trait. */
  services: WhiskServices =>

  /** Creates an entity, or updates an existing one, in namespace. Terminates HTTP request. */
  protected def create(user: Identity, entityName: FullyQualifiedEntityName)(
    implicit transid: TransactionId): RequestContext => Future[RouteResult]


  /** Indicates if listing entities in collection requires filtering out private entities. */
  protected val listRequiresPrivateEntityFilter = false // currently supported on PACKAGES only

  /** Dispatches resource to the proper handler depending on context. */
  protected override def dispatchOp(user: Identity, op: Privilege, resource: Resource)(
    implicit transid: TransactionId) = {
    resource.entity match {
      case Some(EntityName(name)) =>
        op match {
          case PUT =>
            entity(as[LimitedWhiskEntityPut]) { e =>
              validateSize(e.isWithinSizeLimits)(transid, RestApiCommons.jsonDefaultResponsePrinter) {
                create(user, FullyQualifiedEntityName(resource.namespace, name))
              }
            }

          case _      => reject
        }
      case None =>
        op match {
          case _ => reject
        }
    }
  }

  /** Validates entity name from the matched path segment. */
  protected val segmentDescriptionForSizeError = "Name segement"

  protected override final def entityname(s: String) = {
    validate(
      isEntity(s), {
        if (s.length > EntityName.ENTITY_NAME_MAX_LENGTH) {
          Messages.entityNameTooLong(
            SizeError(segmentDescriptionForSizeError, s.length.B, EntityName.ENTITY_NAME_MAX_LENGTH.B))
        } else {
          Messages.entityNameIllegal
        }
      }) & extract(_ => s)
  }

  /** Confirms that a path segment is a valid entity name. Used to reject invalid entity names. */
  protected final def isEntity(n: String) = Try { EntityName(n) } isSuccess
}
