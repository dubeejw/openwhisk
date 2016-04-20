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

package whisk

import (
	"fmt"
	"net/url"
	"reflect"

	"github.com/google/go-querystring/query"
	"github.com/hokaccha/go-prettyjson"
)

// addOptions adds the parameters in opt as URL query parameters to s.  opt
// must be a struct whose fields may contain "url" tags.
func addRouteOptions(route string, options interface{}) (string, error) {
	v := reflect.ValueOf(options)
	if v.Kind() == reflect.Ptr && v.IsNil() {
		return route, nil
	}

	u, err := url.Parse(route)
	if err != nil {
		return route, err
	}

	qs, err := query.Values(options)
	if err != nil {
		return route, err
	}

	u.RawQuery = qs.Encode()
	return u.String(), nil
}

func printJSON(v interface{}) {
	output, _ := prettyjson.Marshal(v)
	fmt.Println(string(output))
}
