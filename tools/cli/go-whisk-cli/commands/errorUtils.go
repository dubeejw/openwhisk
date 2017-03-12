package commands

import (
    "errors"
    "fmt"
    "io"

    "../../go-whisk/whisk"
    "../wski18n"

    "github.com/fatih/color"
    "github.com/spf13/cobra"
    "github.com/mattn/go-colorable"

)

func parseActionError(cmd *cobra.Command, args []string, err error) (error) {
    whisk.Debug(whisk.DbgError, "parseAction(%s, %s) error: %s\n", cmd, args, err)
    errMsg := wski18n.T("Unable to parse action command arguments: {{.err}}", map[string]interface{}{"err": err})
    whiskErr := whisk.MakeWskErrorFromWskError(
        errors.New(errMsg),
        err,
        whisk.EXITCODE_ERR_GENERAL,
        whisk.DISPLAY_MSG,
        whisk.DISPLAY_USAGE)
    return whiskErr
}

func actionInsertError(action *whisk.Action, err error) (error) {
    whisk.Debug(whisk.DbgError, "client.Actions.Insert(%#v, false) error: %s\n", action, err)
    errMsg := wski18n.T("Unable to create action: {{.err}}", map[string]interface{}{"err": err})
    whiskErr := whisk.MakeWskErrorFromWskError(
        errors.New(errMsg),
        err,
        whisk.EXITCODE_ERR_NETWORK,
        whisk.DISPLAY_MSG, whisk.NO_DISPLAY_USAGE)
    return whiskErr
}

func actionUpdateError(action *whisk.Action, err error) (error) {
    whisk.Debug(whisk.DbgError, "client.Actions.Insert(%#v, %t, false) error: %s\n", action, err)
    errMsg := wski18n.T("Unable to update action: {{.err}}", map[string]interface{}{"err": err})
    whiskErr := whisk.MakeWskErrorFromWskError(
        errors.New(errMsg),
        err,
        whisk.EXITCODE_ERR_NETWORK,
        whisk.DISPLAY_MSG,
        whisk.NO_DISPLAY_USAGE)
    return whiskErr
}

func parseQualifiedNameError(actionName string, err error) (error) {
    whisk.Debug(whisk.DbgError, "parseQualifiedName(%s) failed: %s\n", actionName, err)
    errMsg := wski18n.T("'{{.name}}' is not a valid qualified name: {{.err}}",
        map[string]interface{}{"name": actionName, "err": err})
    whiskErr := whisk.MakeWskErrorFromWskError(
        errors.New(errMsg),
        err,
        whisk.EXITCODE_ERR_GENERAL,
        whisk.DISPLAY_MSG,
        whisk.NO_DISPLAY_USAGE)
    return whiskErr
}

func getJSONFromStringsError(params []string, err error) (error) {
    whisk.Debug(whisk.DbgError, "getJSONFromStrings(%#v, false) failed: %s\n", params, err)
    errMsg := wski18n.T("Invalid parameter argument '{{.param}}': {{.err}}",
        map[string]interface{}{"param": fmt.Sprintf("%#v", params), "err": err})
    whiskErr := whisk.MakeWskErrorFromWskError(
        errors.New(errMsg),
        err,
        whisk.EXITCODE_ERR_GENERAL,
        whisk.DISPLAY_MSG,
        whisk.DISPLAY_USAGE)
    return whiskErr
}

func handleInvocationError(err error, entityName string, parameters interface{}) (error) {
    whisk.Debug(whisk.DbgError, "client.Actions.Invoke(%s, %s, %t) error: %s\n", entityName, parameters,
        flags.common.blocking, err)
    errMsg := wski18n.T("Unable to invoke action '{{.name}}': {{.err}}",
        map[string]interface{}{"name": entityName, "err": err})
    whiskErr := whisk.MakeWskErrorFromWskError(
        errors.New(errMsg),
        err,
        whisk.EXITCODE_ERR_GENERAL,
        whisk.DISPLAY_MSG,
        whisk.NO_DISPLAY_USAGE)
    return whiskErr
}


func printBlockingTimeoutMsg(namespace string, entityName string, activationID interface{}) {
    fmt.Fprintf(colorable.NewColorableStderr(),
        wski18n.T("{{.ok}} invoked /{{.namespace}}/{{.name}}, but the request has not yet finished, with id {{.id}}\n",
            map[string]interface{} {
                "ok": color.GreenString("ok:"),
                "namespace": boldString(namespace),
                "name": boldString(entityName),
                "id": boldString(activationID),
            }))
}

func printInvocationMsg(namespace string, entityName string, activationID interface{}, response map[string]interface {},
outputStream io.Writer) {
    if !flags.action.result {
        fmt.Fprintf(outputStream,
            wski18n.T("{{.ok}} invoked /{{.namespace}}/{{.name}} with id {{.id}}\n",
                map[string]interface{} {
                    "ok": color.GreenString("ok:"),
                    "namespace": boldString(namespace),
                    "name": boldString(entityName),
                    "id": boldString(activationID),
                }))
    }

    if flags.common.blocking {
        printJSON(response, outputStream)
    }
}

func fieldExistsError(field string) (error) {
    errMsg := wski18n.T("Invalid field filter '{{.arg}}'.", map[string]interface{}{"arg": field})
    whiskErr := whisk.MakeWskError(
        errors.New(errMsg),
        whisk.EXITCODE_ERR_GENERAL,
        whisk.DISPLAY_MSG,
        whisk.NO_DISPLAY_USAGE)
    return whiskErr
}

func actionGetError(qualifiedName QualifiedName, err error) (error) {
    whisk.Debug(whisk.DbgError, "client.Actions.Get(%s) error: %s\n", qualifiedName.entityName, err)
    errMsg := wski18n.T("Unable to get action: {{.err}}", map[string]interface{}{"err": err})
    whiskErr := whisk.MakeWskErrorFromWskError(
        errors.New(errMsg),
        err,
        whisk.EXITCODE_ERR_GENERAL,
        whisk.DISPLAY_MSG, whisk.NO_DISPLAY_USAGE)
    return whiskErr
}

func actionDeleteError(qualifiedName QualifiedName, err error) (error) {
    whisk.Debug(whisk.DbgError, "client.Actions.Delete(%s) error: %s\n", qualifiedName QualifiedName.entityName, err)
    errMsg := wski18n.T("Unable to delete action: {{.err}}", map[string]interface{}{"err": err})
    whiskErr := whisk.MakeWskErrorFromWskError(errors.New(errMsg), err, whisk.EXITCODE_ERR_GENERAL,
        whisk.DISPLAY_MSG, whisk.NO_DISPLAY_USAGE)
    return whiskErr
}

func actionListError(qualifiedName QualifiedName, err error) (error) {
    whisk.Debug(whisk.DbgError, "client.Actions.List(%s, %#v) error: %s\n", qualifiedName.entityName, options, err)
    errMsg := wski18n.T("Unable to obtain the list of actions for namespace '{{.name}}': {{.err}}",
        map[string]interface{}{"name": getClientNamespace(), "err": err})
    whiskErr := whisk.MakeWskErrorFromWskError(
        errors.New(errMsg),
        err,
        whisk.EXITCODE_ERR_NETWORK,
        whisk.DISPLAY_MSG,
        whisk.NO_DISPLAY_USAGE)
    return whiskErr
}