/*
Copyright 2015-2016 IBM Corporation

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package commands

import (
        "archive/tar"
        "compress/gzip"
        "encoding/base64"
        "encoding/json"
        "errors"
        "fmt"
        "io"
        "io/ioutil"
        "os"
        "path/filepath"
        "regexp"
        "strings"

        "../../go-whisk/whisk"

        "github.com/fatih/color"
        "github.com/spf13/cobra"
)

//////////////
// Commands //
//////////////

var actionCmd = &cobra.Command{
        Use:   "action",
        Short: "work with actions",
}

var actionCreateCmd = &cobra.Command{
        Use:   "create <name string> <artifact string>",
        Short: "create a new action",

        Run: func(cmd *cobra.Command, args []string) {
                action, err := parseAction(cmd, args)
                if err != nil {
                        fmt.Println(err)
                        return
                }
                action, _, err = client.Actions.Insert(action, false)
                if err != nil {
                        fmt.Println(err)
                        return
                }

                fmt.Printf("%s created action %s", color.GreenString("ok:"), boldString(action.Name))
        },
}

/*
usage: wsk action update [-h] [-u AUTH] [--docker] [--copy] [--sequence]
[--lib LIB] [--shared [{yes,no}]]
[-a ANNOTATION ANNOTATION] [-p PARAM PARAM]
[-t TIMEOUT] [-m MEMORY]
name [artifact]

positional arguments:
name                  the name of the action
artifact              artifact (e.g., file name) containing action
definition

optional arguments:
-h, --help            show this help message and exit
-u AUTH, --auth AUTH  authorization key
--docker              treat artifact as docker image path on dockerhub
--copy                treat artifact as the name of an existing action
--sequence            treat artifact as comma separated sequence of actions
to invoke
--lib LIB             add library to artifact (must be a gzipped tar file)
--shared [{yes,no}]   shared action (default: private)
-a ANNOTATION ANNOTATION, --annotation ANNOTATION ANNOTATION
annotations
-p PARAM PARAM, --param PARAM PARAM
default parameters
-t TIMEOUT, --timeout TIMEOUT
the timeout limit in milliseconds when the action will
be terminated
-m MEMORY, --memory MEMORY
the memory limit in MB of the container that runs the
action
*/
var actionUpdateCmd = &cobra.Command{
        Use:   "update <name string> <artifact string>",
        Short: "update an existing action",

        Run: func(cmd *cobra.Command, args []string) {
                action, err := parseAction(cmd, args)
                if err != nil {
                        fmt.Println(err)
                        return
                }
                action, _, err = client.Actions.Insert(action, true)
                if err != nil {
                        fmt.Println(err)
                        return
                }

                fmt.Printf("%s updated action %s", color.GreenString("ok:"), boldString(action.Name))

        },
}

var actionInvokeCmd = &cobra.Command{
        Use:   "invoke <name string> <payload string>",
        Short: "invoke action",
        Run: func(cmd *cobra.Command, args []string) {

                var err error
                var actionName, payloadArg string
                if len(args) < 1 || len(args) > 2 {
                        err = errors.New("Invalid argument list")
                        fmt.Println(err)
                        return
                }

                actionName = args[0]

                payload := map[string]interface{}{}

                if len(flags.common.param) > 0 {
                        parameters, err := parseParameters(flags.common.param)
                        if err != nil {
                                fmt.Printf("error: %s", err)
                                return
                        }

                        for _, param := range parameters {
                                payload[param.Key] = param.Value
                        }
                }

                if len(args) == 2 {
                        payloadArg = args[1]
                        reader := strings.NewReader(payloadArg)
                        err = json.NewDecoder(reader).Decode(&payload)
                        if err != nil {
                                payload["payload"] = payloadArg
                        }
                }

                activation, _, err := client.Actions.Invoke(actionName, payload, flags.common.blocking)
                if err != nil {
                        fmt.Printf("error: %s", err)
                        return
                }

                if flags.common.blocking && flags.action.result {
                        printJSON(activation.Response.Result)
                } else if flags.common.blocking {
                        fmt.Printf("%s invoked %s with id %s\n", color.GreenString("ok:"), boldString(actionName), boldString(activation.ActivationID))
                        boldPrintf("response:\n")
                        printJSON(activation.Response)
                } else {
                        fmt.Printf("%s invoked %s with id %s\n", color.GreenString("ok:"), boldString(actionName), boldString(activation.ActivationID))
                }

        },
}

var actionGetCmd = &cobra.Command{
        Use:   "get <name string>",
        Short: "get action",

        Run: func(cmd *cobra.Command, args []string) {

                var err error
                if len(args) != 1 {
                        err = errors.New("Invalid argument")
                        fmt.Println(err)
                        return
                }

                actionName := args[0]
                action, _, err := client.Actions.Get(actionName)
                if err != nil {
                        fmt.Printf("error: %s", err)
                        return
                }
                // print out response

                if flags.common.summary {
                        fmt.Printf("%s /%s/%s\n", boldString("action"), action.Namespace, action.Name)
                } else {
                        fmt.Printf("%s got action %s\n", color.GreenString("ok:"), boldString(actionName))
                        printJSON(action)
                }

        },
}

var actionDeleteCmd = &cobra.Command{
        Use:   "delete <name string>",
        Short: "delete action",

        Run: func(cmd *cobra.Command, args []string) {
                actionName := args[0]
                _, err := client.Actions.Delete(actionName)
                if err != nil {
                        fmt.Printf("error: %s", err)
                        return
                }
                // print out response
                fmt.Printf("%s deleted action %s\n", color.GreenString("ok:"), boldString(actionName))
        },
}

var actionListCmd = &cobra.Command{
        Use:   "list <namespace string>",
        Short: "list all actions",

        Run: func(cmd *cobra.Command, args []string) {
                var err error
                qName := qualifiedName{}
                if len(args) == 1 {
                        qName, err = parseQualifiedName(args[0])
                        if err != nil {
                                fmt.Printf("error: %s", err)
                                return
                        }
                        ns := qName.namespace
                        if len(ns) == 0 {
                                err = errors.New("No valid namespace detected.  Make sure that namespace argument is preceded by a \"/\"")
                                fmt.Printf("error: %s\n", err)
                                return
                        }

                        client.Namespace = ns

                        if pkg := qName.packageName; len(pkg) > 0 {
                                // todo :: scope call to package
                        }
                }

                options := &whisk.ActionListOptions{
                        Skip:  flags.common.skip,
                        Limit: flags.common.limit,
                }

                actions, _, err := client.Actions.List(options)
                if err != nil {
                        fmt.Printf("error: %s", err)
                        return
                }
                printList(actions)
        },
}

/*
usage: wsk action update [-h] [-u AUTH] [--docker] [--copy] [--sequence]
[--lib LIB] [--shared [{yes,no}]]
[-a ANNOTATION ANNOTATION] [-p PARAM PARAM]
[-t TIMEOUT] [-m MEMORY]
name [artifact]
 */
func parseAction(cmd *cobra.Command, args []string) (*whisk.Action, error) {
        var err error
        var shared bool
        var actionName, artifact string

        if len(args) < 1 /*|| len(args) > 2*/ {
                err = errors.New("Invalid argument list")
                return nil, err
        }

        actionName = args[0]

        if len(args) == 2 {
                artifact = args[1]
        }

        if (flags.common.shared == "yes") {
                shared = true
        } else {
                shared = false
        }


        exec := whisk.Exec{}


        parameters, err := parseParameters(flags.common.param)
        if err != nil {
                return nil, err
        }

        annotations, err := parseAnnotations(flags.common.annotation)
        if err != nil {
                return nil, err
        }

        limits := whisk.Limits{
                Timeout: flags.action.timeout,
                Memory:  flags.action.memory,
        }

        if flags.action.docker {
                exec.Image = artifact
        } else if flags.action.copy {
                existingAction, _, err := client.Actions.Get(actionName)
                if err != nil {
                        return nil, err
                }
                exec = existingAction.Exec
        } else if flags.action.sequence {
                currentNamespace := client.Config.Namespace
                client.Config.Namespace = "whisk.system"
                pipeAction, _, err := client.Actions.Get("system/pipe")
                if err != nil {
                        return nil, err
                }
                exec = pipeAction.Exec
                client.Config.Namespace = currentNamespace
        } else if artifact != "" {
                stat, err := os.Stat(artifact)
                if err != nil {
                        // file does not exist
                        return nil, err
                }

                file, err := ioutil.ReadFile(artifact)
                if err != nil {
                        return nil, err
                }

                exec.Code = string(file)

                if matched, _ := regexp.MatchString(".swift$", stat.Name()); matched {
                        exec.Kind = "swift"
                } else {
                        exec.Kind = "nodejs"
                }
        }

        if flags.action.lib != "" {
                file, err := os.Open(flags.action.lib)
                if err != nil {
                        return nil, err
                }

                var r io.Reader
                switch ext := filepath.Ext(file.Name()); ext {
                case "tar":
                        r = tar.NewReader(file)
                case "gzip":
                        r, err = gzip.NewReader(file)
                default:
                        err = fmt.Errorf("Unrecognized file compression %s", ext)
                }
                if err != nil {
                        return nil, err
                }
                lib, err := ioutil.ReadAll(r)
                if err != nil {
                        return nil, err
                }

                exec.Init = base64.StdEncoding.EncodeToString(lib)
        }

        action := &whisk.Action{
                Name:        actionName,
                Publish:     shared,
                Exec:        exec,
                Annotations: annotations,
                Parameters:  parameters,
                Limits:      limits,
        }

        return action, nil
}

///////////
// Flags //
///////////

func init() {

        actionCreateCmd.Flags().BoolVar(&flags.action.docker, "docker", false, "treat artifact as docker image path on dockerhub")
        actionCreateCmd.Flags().BoolVar(&flags.action.copy, "copy", false, "treat artifact as the name of an existing action")
        actionCreateCmd.Flags().BoolVar(&flags.action.sequence, "sequence", false, "treat artifact as comma separated sequence of actions to invoke")
        actionCreateCmd.Flags().StringVar(&flags.action.shared, "shared", "", "shared action (default: private)")
        actionCreateCmd.Flags().StringVar(&flags.action.lib, "lib", "", "add library to artifact (must be a gzipped tar file)")
        actionCreateCmd.Flags().StringVar(&flags.action.xPackage, "package", "", "package")
        actionCreateCmd.Flags().IntVarP(&flags.action.timeout, "timeout", "t", 0, "the timeout limit in miliseconds when the action will be terminated")
        actionCreateCmd.Flags().IntVarP(&flags.action.memory, "memory", "m", 0, "the memory limit in MB of the container that runs the action")
        actionCreateCmd.Flags().StringSliceVarP(&flags.common.annotation, "annotation", "a", []string{}, "annotations")
        actionCreateCmd.Flags().StringSliceVarP(&flags.common.param, "param", "p", []string{}, "default parameters")

        actionUpdateCmd.Flags().BoolVar(&flags.action.docker, "docker", false, "treat artifact as docker image path on dockerhub")
        actionUpdateCmd.Flags().BoolVar(&flags.action.copy, "copy", false, "treat artifact as the name of an existing action")
        actionUpdateCmd.Flags().BoolVar(&flags.action.sequence, "sequence", false, "treat artifact as comma separated sequence of actions to invoke")
        actionUpdateCmd.Flags().StringVar(&flags.action.shared, "shared", "", "shared action (default: private)")
        actionUpdateCmd.Flags().StringVar(&flags.action.lib, "lib", "", "add library to artifact (must be a gzipped tar file)")
        actionUpdateCmd.Flags().StringVar(&flags.action.xPackage, "package", "", "package")
        actionUpdateCmd.Flags().IntVarP(&flags.action.timeout, "timeout", "t", 0, "the timeout limit in miliseconds when the action will be terminated")
        actionUpdateCmd.Flags().IntVarP(&flags.action.memory, "memory", "m", 0, "the memory limit in MB of the container that runs the action")
        actionUpdateCmd.Flags().StringSliceVarP(&flags.common.annotation, "annotation", "a", []string{}, "annotations")
        actionUpdateCmd.Flags().StringSliceVarP(&flags.common.param, "param", "p", []string{}, "default parameters")

        actionInvokeCmd.Flags().StringSliceVarP(&flags.common.param, "param", "p", []string{}, "parameters")
        actionInvokeCmd.Flags().BoolVarP(&flags.common.blocking, "blocking", "b", false, "blocking invoke")
        actionInvokeCmd.Flags().BoolVarP(&flags.action.result, "result", "r", false, "show only activation result if a blocking activation (unless there is a failure)")

        actionGetCmd.Flags().BoolVarP(&flags.common.summary, "summary", "s", false, "summarize entity details")

        actionListCmd.Flags().IntVarP(&flags.common.skip, "skip", "s", 0, "skip this many entitites from the head of the collection")
        actionListCmd.Flags().IntVarP(&flags.common.limit, "limit", "l", 30, "only return this many entities from the collection")
        actionListCmd.Flags().BoolVar(&flags.common.full, "full", false, "include full entity description")

        actionCmd.AddCommand(
                actionCreateCmd,
                actionUpdateCmd,
                actionInvokeCmd,
                actionGetCmd,
                actionDeleteCmd,
                actionListCmd,
        )
}
