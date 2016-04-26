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
	"fmt"
	"net/http"
	"net/url"
	"os"

	"../../go-whisk/whisk"
)

var client *whisk.Client

func init() {
	var err error

	err = loadProperties()
	if err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}

	baseURL, err := url.Parse(Properties.APIHost)
	if err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}

	clientConfig := &whisk.Config{
		AuthToken: Properties.Auth,
		Namespace: Properties.Namespace,
		BaseURL:   baseURL,
		Version:   Properties.APIVersion,
	}

	// Setup client
	client, err = whisk.NewClient(http.DefaultClient, clientConfig)
	if err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}

}

func Execute() error {
	return WskCmd.Execute()
}
