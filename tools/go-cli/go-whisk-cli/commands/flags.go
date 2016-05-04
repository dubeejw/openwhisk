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

///////////
// Flags //
///////////

var flags struct {
	global struct {
		verbose    bool
		auth       string
		apihost    string
		apiversion string
	}

	common struct {
		blocking   bool
		annotation []string
		param      []string
		shared     bool // AKA "public" or "publish"
		skip       int  // skip first N records
		limit      int  // return max N records
		full       bool // return full records (docs=true for client request)
		summary    bool
	}

	property struct {
		auth          string
		apihost       string
		apiversion    string
		namespace     string
		cliversion    string
		apibuild      string
		all           bool
		apihostSet    string
		apiversionSet string
		namespaceSet  string
	}

	action struct {
		docker   bool
		copy     bool
		pipe     bool
		shared   bool
		sequence bool
		lib      string
		timeout  int
		memory   int
		result   bool
		xPackage string
	}

	activation struct {
		action       string // retrieve results for this action
		upto         int64  // retrieve results up to certain time
		since        int64  // retrieve results after certain time
		seconds      int    // stop polling for activation upda
		sinceSeconds int
		sinceMinutes int
		sinceHours   int
		sinceDays    int
		exit         int
	}

	xPackage struct {
		serviceGUID string
	}

	// rule
	rule struct {
		enable  bool
		disable bool
	}
}
