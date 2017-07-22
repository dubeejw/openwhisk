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

package common

import scala.collection.mutable.Buffer

class BmxWsk() extends Wsk with BmxWskPath {
    override implicit val action = new BmxWskAction
    override implicit val trigger = new WskTrigger
    override implicit val rule = new WskRule
    override implicit val activation = new WskActivation
    override implicit val pkg = new WskPackage
    override implicit val namespace = new WskNamespace
    override implicit val api = new WskApi
    override implicit val apiexperimental = new WskApiExperimental
}

class BmxWskAction() extends WskAction with BmxWskPath{
}

class BmxWskTrigger() extends WskTrigger with BmxWskPath {
}

class BmxWskRule() extends WskRule with BmxWskPath{
}

class BmxWskActivation() extends WskActivation with BmxWskPath{
}

class BmxWskPackage() extends WskPackage with BmxWskPath{
}

class BmxWskNamespace() extends WskNamespace with BmxWskPath{
}

class BmxWskApi() extends WskApi with BmxWskPath{
}

class BmxWskApiExperimental() extends WskApiExperimental with BmxWskPath {
}

trait BmxWskPath extends WskPath {
    override def baseCommand = Buffer("/usr/local/bin/bx", "wsk")
}

