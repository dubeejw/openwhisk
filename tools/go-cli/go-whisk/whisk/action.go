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
	"net/http"
)

type ActionService struct {
	client *Client
}

type Action struct {
	Namespace string `json:"namespace,omitempty"`
	Name      string `json:"name,omitempty"`
	Version   string `json:"version,omitempty"`
	Publish   bool   `json:"publish,omitempty"`

	Exec        `json:"exec,omitempty"`
	Annotations `json:"annotations,omitempty"`
	Parameters  `json:"parameters,omitempty"`
	Limits      `json:"limits,omitempty"`
}

type Exec struct {
	Code  string `json:"code,omitempty"`
	Kind  string `json:"kind,omitempty"`
	Image string `json:"image,omitempty"`
	Init  string `json:"init,omitempty"`
}

type ActionListOptions struct {
	Limit int  `url:"limit,omitempty"`
	Skip  int  `url:"skip,omitempty"`
	Docs  bool `url:"docs,omitempty"`
}

////////////////////
// Action Methods //
////////////////////

func (s *ActionService) List(options *ActionListOptions) ([]Action, *http.Response, error) {
	route := "actions"
	route, err := addRouteOptions(route, options)
	if err != nil {
		return nil, nil, err
	}

	req, err := s.client.NewRequest("GET", route, nil)
	if err != nil {
		return nil, nil, err
	}

	var actions []Action
	resp, err := s.client.Do(req, &actions)
	if err != nil {
		return nil, resp, err
	}

	return actions, resp, err

}

func (s *ActionService) Insert(action *Action, overwrite bool) (*Action, *http.Response, error) {
	route := fmt.Sprintf("actions/%s?overwrite=%t", action.Name, overwrite)

	req, err := s.client.NewRequest("PUT", route, action)
	if err != nil {
		return nil, nil, err
	}

	a := new(Action)
	resp, err := s.client.Do(req, &a)
	if err != nil {
		return nil, resp, err
	}

	return a, resp, nil

}

func (s *ActionService) Get(actionName string) (*Action, *http.Response, error) {
	route := fmt.Sprintf("actions/%s", actionName)

	req, err := s.client.NewRequest("GET", route, nil)
	if err != nil {
		return nil, nil, err
	}

	a := new(Action)
	resp, err := s.client.Do(req, &a)
	if err != nil {
		return nil, resp, err
	}

	return a, resp, nil

}

func (s *ActionService) Delete(actionName string) (*http.Response, error) {
	route := fmt.Sprintf("actions/%s", actionName)

	req, err := s.client.NewRequest("DELETE", route, nil)
	if err != nil {
		return nil, err
	}

	resp, err := s.client.Do(req, nil)
	if err != nil {
		return resp, err
	}

	return resp, nil
}

func (s *ActionService) Invoke(actionName string, payload map[string]interface{}, blocking bool) (*Activation, *http.Response, error) {
	route := fmt.Sprintf("actions/%s?blocking=%t", actionName, blocking)

	req, err := s.client.NewRequest("POST", route, payload)
	if err != nil {
		return nil, nil, err
	}

	a := new(Activation)
	resp, err := s.client.Do(req, &a)
	if err != nil {
		return nil, resp, err
	}

	return a, resp, nil

}
