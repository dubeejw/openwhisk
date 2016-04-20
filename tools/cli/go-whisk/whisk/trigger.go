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

type TriggerService struct {
	client *Client
}

type Trigger struct {
	Namespace string `json:"namespace,omitempty"`
	Name      string `json:"name,omitempty"`
	Version   string `json:"version,omitempty"`
	Publish   bool   `json:"publish,omitempty"`

	ID          string `json:"id"`
	Annotations `json:"annotations"`
	Parameters  `json:"parameters"`
	Limits      `json:"limits"`
}

type TriggerListOptions struct {
	Limit int  `url:"limit,omitempty"`
	Skip  int  `url:"skip,omitempty"`
	Docs  bool `url:"docs,omitempty"`
}

func (s *TriggerService) List(options *TriggerListOptions) ([]Trigger, *http.Response, error) {
	route := "triggers"
	route, err := addRouteOptions(route, options)
	if err != nil {
		return nil, nil, err
	}

	req, err := s.client.NewRequest("GET", route, nil)
	if err != nil {
		return nil, nil, err
	}

	var triggers []Trigger
	resp, err := s.client.Do(req, &triggers)
	if err != nil {
		return nil, resp, err
	}

	return triggers, resp, err

}

func (s *TriggerService) Insert(trigger *Trigger, overwrite bool) (*Trigger, *http.Response, error) {
	route := fmt.Sprintf("triggers/%s?overwrite=%s", trigger.Name, overwrite)

	req, err := s.client.NewRequest("POST", route, trigger)
	if err != nil {
		return nil, nil, err
	}

	t := new(Trigger)
	resp, err := s.client.Do(req, &t)
	if err != nil {
		return nil, resp, err
	}

	return t, resp, nil

}

func (s *TriggerService) Get(triggerName string) (*Trigger, *http.Response, error) {
	route := fmt.Sprintf("triggers/%s", triggerName)

	req, err := s.client.NewRequest("GET", route, nil)
	if err != nil {
		return nil, nil, err
	}

	t := new(Trigger)
	resp, err := s.client.Do(req, &t)
	if err != nil {
		return nil, resp, err
	}

	return t, resp, nil

}

func (s *TriggerService) Delete(triggerName string) (*http.Response, error) {
	route := fmt.Sprintf("triggers/%s", triggerName)

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

func (s *TriggerService) Fire(triggerName string, payload map[string]interface{}) (*Trigger, *http.Response, error) {
	route := fmt.Sprintf("triggers/", triggerName)

	req, err := s.client.NewRequest("POST", route, payload)
	if err != nil {
		return nil, nil, err
	}

	t := new(Trigger)
	resp, err := s.client.Do(req, &t)
	if err != nil {
		return nil, resp, err
	}

	return t, resp, nil

}
