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
)
import "fmt"

const EXITCODE_ERR_GENERAL      int = 1
const EXITCODE_ERR_USAGE        int = 2
const EXITCODE_ERR_NETWORK      int = 3
const EXITCODE_ERR_HTTP_RESP    int = 4

const DISPLAY_MSG       bool = true
const NO_DISPLAY_MSG    bool = false
const DISPLAY_USAGE     bool = true
const NO_DISPLAY_USAGE  bool = false
const DISPLAY_PREFIX    bool = true
const NO_DISPLAY_PREFIX bool = false

type WskError struct {
    RootErr         error
    ExitCode        int
    DisplayMsg      bool    // When true, the error message should be displayed to console
    MsgDisplayed    bool    // When true, the error message has already been displayed, don't display it again
    DisplayUsage    bool    // When true, the CLI usage should be displayed before exiting
    DisplayPrefix   bool    // When true, the error message will be prefixed with "error: "
}

func (err WskError) Error() string {
    if err.DisplayPrefix {
        return fmt.Sprintf("error: %s", err.RootErr.Error())
    } else {
        return fmt.Sprintf(err.RootErr.Error())
    }
}

/*
Instantiate a WskError structure
Parameters:
    error   - RootErr. object implementing the error interface
    int     - ExitCode.  Used if error object does not have an exit code OR if ExitCodeOverride is true
    bool    - DisplayMsg.  If true, the error message should be displayed on the console
    bool    - DisplayUsage.  If true, the command usage syntax/help should be displayed on the console
    bool    - MsgDisplayed.  If true, the error message has been displayed on the console
    bool    - DisplayPreview.  If true, the error message will be prefixed with "error: "
*/
func MakeWskError (err error, errorCode int, flags ...bool ) (whiskError *WskError) {
    whiskError = &WskError{
        RootErr: err,
        ExitCode: errorCode,
        DisplayMsg: false,
        DisplayUsage: false,
        MsgDisplayed: false,
        DisplayPrefix: true,
    }

    if len(flags) > 0 { whiskError.DisplayMsg = flags[0] }
    if len(flags) > 1 { whiskError.DisplayUsage = flags[1] }
    if len(flags) > 2 { whiskError.MsgDisplayed = flags[2] }
    if len(flags) > 3 { whiskError.DisplayPrefix = flags[3] }

    return whiskError
}

/*
Instantiate a WskError structure
Parameters:
    error   - RootErr. object implementing the error interface
    WskError -WskError being wrappered.  It's exitcode will be used as this WskError's exitcode.  Ignored if nil
    int     - ExitCode.  Used if error object is nil or if the error object is not a WskError
    bool    - DisplayMsg.  If true, the error message should be displayed on the console
    bool    - DisplayUsage.  If true, the command usage syntax/help should be displayed on the console
    bool    - MsgDisplayed.  If true, the error message has been displayed on the console
    bool    - DisplayPrefix.  If true, the error message will be prefixed with "error: "
*/
func MakeWskErrorFromWskError (e error, werr error, ec int, flags ...bool) (we *WskError) {
    exitCode := ec

    // Get the exit code, and flags from the existing Whisk error
    if werr != nil {

        // Get the Whisk error from the Error object
        switch errorType := werr.(type) {
            case *WskError:
                we = errorType
            case WskError:
                we = &errorType
        }

        exitCode, flags = getWhiskErrorProperties(we, flags...)
    }

    return MakeWskError(e, exitCode, flags...)
}

func getWhiskErrorProperties(whiskError *WskError, flags ...bool) (int, []bool) {

    if len(flags) > 0 {
        flags[0] = whiskError.DisplayMsg
    } else {
        flags = append(flags, whiskError.DisplayMsg)
    }

    if len(flags) > 1 {
        flags[1] = whiskError.DisplayUsage || flags[1]
    } else {
        flags = append(flags, whiskError.DisplayUsage)
    }

    if len(flags) > 2 {
        flags[2] = whiskError.MsgDisplayed || flags[2]
    } else {
        flags = append(flags, whiskError.MsgDisplayed)
    }

    if len(flags) > 3 {
        flags[3] = whiskError.DisplayPrefix || flags[3]
    } else {
        flags = append(flags, whiskError.DisplayPrefix)
    }

    return whiskError.ExitCode, flags
}

