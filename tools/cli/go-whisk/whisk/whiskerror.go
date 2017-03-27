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

package whisk

import (
    "fmt"
    "errors"
)

/*const EXITCODE_ERR_GENERAL      int = 1
const EXITCODE_ERR_USAGE        int = 2
const EXITCODE_ERR_NETWORK      int = 3
const EXITCODE_ERR_HTTP_RESP    int = 4
const NOT_ALLOWED               int = 149
const EXITCODE_TIMED_OUT        int = 202

const DISPLAY_MSG               bool = true
const NO_DISPLAY_MSG            bool = false
const DISPLAY_USAGE             bool = true
const NO_DISPLAY_USAGE          bool = false
const NO_MSG_DISPLAYED          bool = false
const DISPLAY_PREFIX            bool = true
const NO_DISPLAY_PREFIX         bool = false
const APPLICATION_ERR           bool = true
const NO_APPLICATION_ERR        bool = false
const TIMED_OUT                 bool = true*/

type Error struct {
    Head                interface{}
    Tail                interface{}
    ExitCode            int     // Error code to be returned to the OS
    DisplayMsg          bool    // When true, the error message should be displayed to console
    MsgDisplayed        bool    // When true, the error message has already been displayed, don't display it again
    DisplayUsage        bool    // When true, the CLI usage should be displayed before exiting
    DisplayPrefix       bool    // When true, the CLI will prefix an error message with "error: "
    ApplicationError    bool    // When true, the error is a result of an application failure
    TimedOut            bool    // When True, the error is a result of a timeout
}

/*
Prints the error message contained inside an Error. An error prefix may, or may not be displayed depending on the
Error's setting for DisplayPrefix.

Parameters:
    err     - Error object used to display an error message from
 */
func (whiskError Error) Error() string {
    return fmt.Sprint("%s: %s", whiskError.Head, whiskError.Tail)
}

/*
Instantiate a Error structure
Parameters:
    error   - RootErr. object implementing the error interface
    int     - ExitCode.  Used if error object does not have an exit code OR if ExitCodeOverride is true
    bool    - DisplayMsg.  If true, the error message should be displayed on the console
    bool    - DisplayUsage.  If true, the command usage syntax/help should be displayed on the console
    bool    - MsgDisplayed.  If true, the error message has been displayed on the console
    bool    - DisplayPreview.  If true, the error message will be prefixed with "error: "
    bool    - TimedOut. If true, the error is a result of a timeout
*/
func MakeWhiskError (err error, flags ...interface{} ) (resWhiskError *Error) {
    resWhiskError = &Error{
        Head: err,
        Tail: nil,
        ExitCode: 0,
        DisplayMsg: false,
        DisplayUsage: false,
        MsgDisplayed: false,
        DisplayPrefix: true,
        ApplicationError: false,
        TimedOut: false,
    }

    if len(flags) > 0 { resWhiskError.ExitCode = flags[0].(int) }
    if len(flags) > 1 { resWhiskError.DisplayMsg = flags[1].(bool) }
    if len(flags) > 2 { resWhiskError.DisplayUsage = flags[2].(bool) }
    if len(flags) > 3 { resWhiskError.MsgDisplayed = flags[3].(bool) }
    if len(flags) > 4 { resWhiskError.DisplayPrefix = flags[4].(bool) }
    if len(flags) > 5 { resWhiskError.ApplicationError = flags[5].(bool) }
    if len(flags) > 6 { resWhiskError.TimedOut = flags[6].(bool) }

    return resWhiskError
}

/*
Instantiate a WhiskError structure
Parameters:
    error       - RootErr. object implementing the error interface
    WhiskError    - WhiskError being wrappered.  It's exitcode will be used as this WhiskError's exitcode.  Ignored if nil
    int         - ExitCode. Used if error object is nil or if the error object is not a WhiskError
    bool        - DisplayMsg. If true, the error message should be displayed on the console
    bool        - DisplayUsage. If true, the command usage syntax/help should be displayed on the console
    bool        - MsgDisplayed. If true, the error message has been displayed on the console
    bool        - ApplicationError. If true, the error is a result of an application error
    bool        - TimedOut. If true, the error resulted from a timeout
*/
func NestedWhiskError(message string, whiskError error, flags ...interface{}) (resWhiskError *Error) {
    var err *Error

    // Get the exit code, and flags from the existing Whisk error
    if whiskError != nil {

        // Ensure the Whisk error is a pointer
        switch errorType := whiskError.(type) {
            case *Error:
                resWhiskError = errorType
            case Error:
                resWhiskError = &errorType
        }

        if resWhiskError != nil {
            flags = getWhiskErrorProperties2(resWhiskError, flags...)
        }
    }

    err = MakeWhiskError(errors.New(message), flags...)
    err.Tail = whiskError

    return err
}

/*
Returns the settings from a Error. Values returned will include ExitCode, DisplayMsg, DisplayUsage, MsgDisplayed,
DisplayPrefix, TimedOut.

Parameters:
    whiskError  - Error to examine.
    flags       - Boolean values that may override the Error object's values for DisplayMsg, DisplayUsage,
                    MsgDisplayed, ApplicationError, TimedOut.
 */
func getWhiskErrorProperties2(whiskError *Error, flags ...interface{}) ([]interface{}) {
    if len(flags) > 0 {
        flags[0] = flags[0].(int)
    } else {
        flags = append(flags, whiskError.ExitCode)
    }

    if len(flags) > 1 {
        flags[1] = whiskError.DisplayMsg || true // TODO
    } else {
        flags = append(flags, DISPLAY_MSG)
    }

    if len(flags) > 2 {
        flags[2] = flags[2].(bool) || true // TODO
    } else {
        flags = append(flags, whiskError.DisplayUsage)
    }

    if len(flags) < 3 {
        flags = append(flags, whiskError.MsgDisplayed)
    }

    if len(flags) < 4 {
        flags = append(flags, whiskError.DisplayPrefix)
    }

    if len(flags) < 5 {
        flags = append(flags, whiskError.ApplicationError)
    }

    if len(flags) < 6 {
        flags = append(flags, whiskError.TimedOut)
    }

    return flags
}
