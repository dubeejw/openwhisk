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
	"net/http"
	"net/url"
)

type Info struct {
	Whisk   string `json:"whisk,omitempty"`
	Version string `json:"version,omitempty"`
	Build   string `json:"build,omitempty"`
}

type InfoService struct {
	client *Client
}

func (s *InfoService) Get() (*Info, *http.Response, error) {
	// make a request to c.BaseURL / v1

	ref, err := url.Parse(s.client.Config.Version)
	if err != nil {
		return nil, nil, err
	}

	u := s.client.BaseURL.ResolveReference(ref)

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, nil, err
	}

	info := new(Info)
	resp, err := s.client.Do(req, &info)
	if err != nil {
		return nil, resp, err
	}

	return info, resp, nil
}
