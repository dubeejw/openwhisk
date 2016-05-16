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

package system.basic

import java.io.File
import scala.collection.mutable.ListBuffer
import org.apache.commons.io.FileUtils
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterAll
import org.scalatest.BeforeAndAfterEachTestData
import org.scalatest.FlatSpec
import org.scalatest.Matchers
import org.scalatest.ParallelTestExecution
import org.scalatest.TestData
import org.scalatest.junit.JUnitRunner
import common.DeleteFromCollection
import common.RunWskAdminCmd
import common.RunWskCmd
import common.TestUtils
import common.TestUtils._
import common.Wsk
import common.WskAction
import common.WskProps
import spray.json._
import spray.json.DefaultJsonProtocol.StringJsonFormat
import spray.json.PimpedAny
import common.TestHelpers
import common.WskTestHelpers
import common.TestHelpers
import common.WskProps
import whisk.core.entity.WhiskPackage

@RunWith(classOf[JUnitRunner])
class WskBasicTests
    extends TestHelpers
    with WskTestHelpers {

    implicit val wskprops = WskProps()
    val wsk = new Wsk()
    val defaultAction = Some(TestUtils.getCatalogFilename("samples/hello.js"))

    behavior of "Wsk CLI"

    it should "confirm wsk exists" in {
        Wsk.exists
    }

    it should "show help and usage info" in {
        val stdout = wsk.cli(Seq("-h")).stdout
        println(s"show help and usage:\n$stdout")
        stdout should include regex ("""(?i)Usage:""")
        stdout should include regex ("""(?i)Flags""")
        stdout should include regex ("""(?i)Available commands""")
        stdout should include regex ("""(?i)--help""")
    }

    it should "show cli build version" in {
        val stdout = wsk.cli(Seq("property", "get", "--cliversion")).stdout
        stdout should include regex ("""(?i)whisk CLI version\s+201.*""")
    }

    it should "show api version" in {
        val stdout = wsk.cli(Seq("property", "get", "--apiversion")).stdout
        stdout should include regex ("""(?i)whisk API version\s+v1""")
    }

    it should "show api build version" in {
        val stdout = wsk.cli(wskprops.overrides ++ Seq("property", "get", "--apibuild")).stdout
        stdout should include regex ("""(?i)whisk API build\s+201.*""")
    }

    it should "show api build number" in {
        val stdout = wsk.cli(wskprops.overrides ++ Seq("property", "get", "--apibuildno")).stdout
        stdout should include regex ("""(?i)whisk API build.*\s+.*""")
    }

    it should "set auth in property file" in {
        val wskprops = File.createTempFile("wskprops", ".tmp")
        println(s"Using property file ", wskprops.getAbsolutePath())
        val env = Map("WSK_CONFIG_FILE" -> wskprops.getAbsolutePath())
        wsk.cli(Seq("property", "set", "--auth", "testKey"), env = env)
        val fileContent = FileUtils.readFileToString(wskprops)
        println(s"Property file contents:\n$fileContent")
        fileContent should include("AUTH=testKey")
        wskprops.delete()
    }

    it should "reject creating duplicate entity" in withAssetCleaner(wskprops) {
        (wp, assetHelper) =>
            val name = "testDuplicateCreate"
            assetHelper.withCleaner(wsk.trigger, name) {
                (trigger, _) => trigger.create(name)
            }
            assetHelper.withCleaner(wsk.action, name, confirmDelete = false) {
                (action, _) => action.create(name, defaultAction, expectedExitCode = CONFLICT)
            }
    }

    it should "reject deleting entity in wrong collection" in withAssetCleaner(wskprops) {
        (wp, assetHelper) =>
            val name = "testCrossDelete"
            assetHelper.withCleaner(wsk.trigger, name) {
                (trigger, _) => trigger.create(name)
            }
            wsk.action.delete(name, expectedExitCode = CONFLICT)
    }

    it should "reject creating entities with invalid names" in withAssetCleaner(wskprops) {
        (wp, assetHelper) =>
            val names = Seq(
                ("", NOTALLOWED),
                (" ", BAD_REQUEST),
                ("hi+there", BAD_REQUEST),
                ("$hola", BAD_REQUEST),
                ("dora?", BAD_REQUEST),
                ("|dora|dora?", BAD_REQUEST))

            names foreach {
                case (name, ec) =>
                    assetHelper.withCleaner(wsk.action, name, confirmDelete = false) {
                        (action, _) => action.create(name, defaultAction, expectedExitCode = ec)
                    }
            }
    }

    it should "reject unauthenticated access" in {
        implicit val wskprops = WskProps("xxx") // shadow properties
        val errormsg = "The supplied authentication is invalid"
        wsk.namespace.list(expectedExitCode = UNAUTHORIZED).
            stdout should include(errormsg)
        wsk.namespace.get(expectedExitCode = UNAUTHORIZED).
            stdout should include(errormsg)
    }

    it should "reject deleting action in shared package not owned by authkey" in {
        wsk.action.get("/whisk.system/util/cat") // make sure it exists
        wsk.action.delete("/whisk.system/util/cat", expectedExitCode = FORBIDDEN)
    }

    it should "reject create action in shared package not owned by authkey" in {
        wsk.action.get("/whisk.system/util/notallowed", expectedExitCode = NOT_FOUND) // make sure it does not exist
        val file = Some(TestUtils.getCatalogFilename("samples/hello.js"))
        try {
            wsk.action.create("/whisk.system/util/notallowed", file, expectedExitCode = FORBIDDEN)
        } finally {
            wsk.action.sanitize("/whisk.system/util/notallowed")
        }
    }

    it should "reject update action in shared package not owned by authkey" in {
        wsk.action.create("/whisk.system/util/cat", None,
            update = true, shared = Some(true), expectedExitCode = FORBIDDEN)
    }

    it should "reject bad command" in {
        val result = wsk.cli(Seq("bogus"), expectedExitCode = ERROR_EXIT)
        val stderr = result.stderr
        val stdout = result.stdout
        println(s"bad command stderr:\n$stderr")
        println(s"bad command stdout:\n$stdout")
        result.stderr should include regex ("""(?i)Run 'wsk --help' for usage""")
    }

    it should "reject authenticated command when no auth key is given" in {
        // override wsk props file in case it exists
        val wskprops = File.createTempFile("wskprops", ".tmp")
        val env = Map("WSK_CONFIG_FILE" -> wskprops.getAbsolutePath())
        val result = wsk.cli(Seq("list"), env = env, expectedExitCode = MISUSE_EXIT)
        println(s"reject auth when no auth key stderr:\n$result.stderr")
        println(s"reject auth when no auth key stdout:\n$result.stdout")
        result.stderr should include("usage:")
        result.stderr should include("--auth is required")
    }

    behavior of "Wsk Package CLI"

    it should "list shared packages" in {
        val result = wsk.pkg.list(Some("/whisk.system")).stdout
        result should include regex ("""/whisk.system/samples\s+shared""")
        result should include regex ("""/whisk.system/util\s+shared""")
    }

    it should "list shared package actions" in {
        val result = wsk.action.list(Some("/whisk.system/util")).stdout
        result should include regex ("""/whisk.system/util/head\s+shared""")
        result should include regex ("""/whisk.system/util/date\s+shared""")
    }

    it should "create, update, get and list a package" in withAssetCleaner(wskprops) {
        (wp, assetHelper) =>
            val name = "samplePackage"
            val params = Map("a" -> "A")
            assetHelper.withCleaner(wsk.pkg, name) {
                (pkg, _) =>
                    pkg.create(name, parameters = params, shared = Some(true))
                    pkg.create(name, update = true)
            }
            val stdout = wsk.pkg.get(name).stdout
            stdout should include regex (""""key": "a"""")
            stdout should include regex (""""value": "A"""")
            stdout should include regex (""""publish": true""")
            stdout should include regex (""""version": "0.0.2"""")
            wsk.pkg.list().stdout should include(name)
    }

    it should "create a package binding" in withAssetCleaner(wskprops) {
        (wp, assetHelper) =>
            val name = "bindPackage"
            val provider = "/whisk.system/samples"
            val annotations = Map("a" -> "A", WhiskPackage.bindingFieldName -> "xxx")
            assetHelper.withCleaner(wsk.pkg, name) {
                (pkg, _) =>
                    pkg.bind(provider, name, annotations = annotations)
            }
            val stdout = wsk.pkg.get(name).stdout
            stdout should include regex (""""key": "a"""")
            stdout should include regex (""""value": "A"""")
            stdout should include regex (s""""key": "${WhiskPackage.bindingFieldName}"""")
            stdout should not include regex(""""key": "xxx"""")
    }

    behavior of "Wsk Action CLI"

    it should "create the same action twice with different cases" in withAssetCleaner(wskprops) {
        (wp, assetHelper) =>
            assetHelper.withCleaner(wsk.action, "TWICE") { (action, name) => action.create(name, defaultAction) }
            assetHelper.withCleaner(wsk.action, "twice") { (action, name) => action.create(name, defaultAction) }
    }

    it should "create, update, get and list an action" in withAssetCleaner(wskprops) {
        (wp, assetHelper) =>
            val name = "createAndUpdate"
            val file = Some(TestUtils.getCatalogFilename("samples/hello.js"))
            val params = Map("a" -> "A")
            assetHelper.withCleaner(wsk.action, name) {
                (action, _) =>
                    action.create(name, file, parameters = params, shared = Some(true))
                    action.create(name, None, update = true)
            }
            val stdout = wsk.action.get(name).stdout
            stdout should include regex (""""key": "a"""")
            stdout should include regex (""""value": "A"""")
            stdout should include regex (""""publish": true""")
            stdout should include regex (""""version": "0.0.2"""")
            wsk.action.list().stdout should include(name)
    }

    it should "get an action" in {
        wsk.action.get("/whisk.system/samples/wordCount").
            stdout should include("words")
    }

    it should "reject delete of action that does not exist" in {
        wsk.action.sanitize("deleteFantasy").
            stdout should include regex ("""error: The requested resource does not exist. \(code \d+\)""")
    }

    it should "reject create with missing file" in {
        wsk.action.create("missingFile", Some("notfound"),
            expectedExitCode = ERROR_EXIT).
            stdout should include("Unable to parse action")
    }

    /**
     * Tests creating an action from a malformed js file. This should fail in
     * some way - preferably when trying to create the action. If not, then
     * surely when it runs there should be some indication in the logs. Don't
     * think this is true currently.
     */
    it should "create and invoke action with malformed js resulting in activation error" in withAssetCleaner(wskprops) {
        (wp, assetHelper) =>
            val name = "MALFORMED"
            assetHelper.withCleaner(wsk.action, name) {
                (action, _) => action.create(name, Some(TestUtils.getTestActionFilename("malformed.js")))
            }

            val activation = wsk.action.invoke(name, Map("payload" -> "whatever"))
            val activationId = wsk.action.extractActivationId(activation)
            activationId shouldBe a[Some[_]]

            val expected = "ReferenceError" // representing nodejs giving an error when given malformed.js
            val (found, logs) = wsk.activation.contains(activationId.get, expected)
            assert(found, s"Did not find '$expected' in activation($activationId) ${logs getOrElse "empty"}")
    }

    it should "invoke a blocking action and get only the result" in withAssetCleaner(wskprops) {
        (wp, assetHelper) =>
            val name = "basicInvoke"
            assetHelper.withCleaner(wsk.action, name) {
                (action, _) => action.create(name, Some(TestUtils.getCatalogFilename("samples/wc.js")))
            }
            wsk.action.invoke(name, Map("payload" -> "'one two three'"), blocking = true, result = true)
                .stdout should include regex (""""count": 3""")
    }

    behavior of "Wsk Trigger CLI"

    it should "create trigger, get trigger, update trigger and list trigger" in withAssetCleaner(wskprops) {
        (wp, assetHelper) =>
            val name = "listTriggers"
            val params = Map("a" -> "A")
            assetHelper.withCleaner(wsk.trigger, name) {
                (trigger, _) =>
                    trigger.create(name, parameters = params, shared = Some(true))
                    trigger.create(name, update = true)
            }
            val stdout = wsk.trigger.get(name).stdout
            stdout should include regex (""""key": "a"""")
            stdout should include regex (""""value": "A"""")
            stdout should include regex (""""publish": true""")
            stdout should include regex (""""version": "0.0.2"""")
            wsk.trigger.list().stdout should include(name)
    }

    it should "not create a trigger when feed fails to initialize" in {
        val name = "badfeed"
        wsk.trigger.create(name, feed = Some(s"bogus"), expectedExitCode = ANY_ERROR_EXIT).
            exitCode should { equal(NOT_FOUND) or equal(FORBIDDEN) }
        wsk.trigger.get(name, expectedExitCode = NOT_FOUND)

        wsk.trigger.create(name, feed = Some(s"bogus/feed"), expectedExitCode = ANY_ERROR_EXIT).
            exitCode should { equal(NOT_FOUND) or equal(FORBIDDEN) }
        wsk.trigger.get(name, expectedExitCode = NOT_FOUND)

        // verify that the feed runs and returns an application error (502 or Gateway Timeout)
        wsk.trigger.create(name, feed = Some(s"/whisk.system/github/webhook"), expectedExitCode = TIMEOUT)
        wsk.trigger.get(name, expectedExitCode = NOT_FOUND)
    }

    behavior of "Wsk Rule CLI"

    it should "create rule, get rule, update rule and list rule" in withAssetCleaner(wskprops) {
        (wp, assetHelper) =>
            val ruleName = "listRules"
            val triggerName = "listRulesTrigger"
            val actionName = "listRulesAction";
            assetHelper.withCleaner(wsk.trigger, triggerName) {
                (trigger, name) => trigger.create(name)
            }
            assetHelper.withCleaner(wsk.action, actionName) {
                (action, name) => action.create(name, defaultAction)
            }
            assetHelper.withCleaner(wsk.rule, ruleName) {
                (rule, name) =>
                    rule.create(name, trigger = triggerName, action = actionName)
                    rule.create(name, trigger = triggerName, action = actionName, update = true)
            }

            val stdout = wsk.rule.get(ruleName).stdout
            stdout should include(ruleName)
            stdout should include(triggerName)
            stdout should include(actionName)
            stdout should include regex (""""version": "0.0.2"""")
            wsk.rule.list().stdout should include(ruleName)
    }

    behavior of "Wsk Namespace CLI"

    it should "list namespaces" in {
        wsk.namespace.list().
            stdout should include regex ("@|guest")
    }

    it should "list entities in default namespace" in {
        // use a fresh wsk props instance that is guaranteed to use
        // the default namespace
        wsk.namespace.get(expectedExitCode = SUCCESS_EXIT)(WskProps()).
            stdout should include("default")
    }
}
