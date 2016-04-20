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

// NOTE :: deprecated
type KeyValue struct {
	Key   string `json:"key,omitempty"`
	Value string `json:"value,omitempty"`
}

type Annotations []KeyValue

// type Annotations map[string]interface{}

type Parameters []KeyValue

// type Parameters map[string]interface{}

type Limits struct {
	Timeout int `json:"timeout,omitempty"`
	Memory  int `json:"memory,omitempty"`
}
