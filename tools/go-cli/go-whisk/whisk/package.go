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
    "net/url"
    "errors"
)

type PackageService struct {
    client *Client
}

type PackageInterface interface {
    GetName() string
}

// Use this struct to create/update a package/binding with the Publish setting
type SentPackagePublish struct {
    Namespace string `json:"-"`
    Name      string `json:"-"`
    Version   string `json:"version,omitempty"`
    Publish   bool   `json:"publish"`
    Annotations 	 `json:"annotations,omitempty"`
    Parameters  	 `json:"parameters,omitempty"`
}
func (p *SentPackagePublish) GetName() string {
    return p.Name
}

// Use this struct to update a package/binding with no change to the Publish setting
type SentPackageNoPublish struct {
    Namespace string `json:"-"`
    Name      string `json:"-"`
    Version   string `json:"version,omitempty"`
    Publish   bool   `json:"publish,omitempty"`
    Annotations 	 `json:"annotations,omitempty"`
    Parameters  	 `json:"parameters,omitempty"`
}
func (p *SentPackageNoPublish) GetName() string {
    return p.Name
}

// Use this struct to represent the package/binding sent from the Whisk server
type Package struct {
    Namespace string `json:"namespace,omitempty"`
    Name      string `json:"name,omitempty"`
    Version   string `json:"version,omitempty"`
    Publish   bool   `json:"publish"`
    Annotations 	 `json:"annotations,omitempty"`
    Parameters  	 `json:"parameters,omitempty"`
    Binding          `json:"binding,omitempty"`
}
func (p *Package) GetName() string {
    return p.Name
}

// Use this struct when creating a binding.  Publish is NOT optional
type BindingPackage struct {
    Namespace string `json:"-"`
    Name      string `json:"-"`
    Version   string `json:"version,omitempty"`
    Publish   bool   `json:"publish"`
    Annotations 	 `json:"annotations,omitempty"`
    Parameters  	 `json:"parameters,omitempty"`
    Binding          `json:"binding"`
}
func (p *BindingPackage) GetName() string {
    return p.Name
}

type Binding struct {
    Namespace string `json:"namespace,omitempty"`
    Name      string `json:"name,omitempty"`
}

type BindingUpdates struct {
    Added   []Binding `json:"added,omitempty"`
    Updated []Binding `json:"added,omitempty"`
    Deleted []Binding `json:"added,omitempty"`
}

type PackageListOptions struct {
    Public bool `url:"public,omitempty"`
    Limit  int  `url:"limit,omitempty"`
    Skip   int  `url:"skip,omitempty"`
    Since  int  `url:"since,omitempty"`
    Docs   bool `url:"docs,omitempty"`
}

func (s *PackageService) List(options *PackageListOptions) ([]Package, *http.Response, error) {
    route := fmt.Sprintf("packages")
    route, err := addRouteOptions(route, options)
    if err != nil {
        return nil, nil, err
    }

    req, err := s.client.NewRequest("GET", route, nil)
    if err != nil {
        return nil, nil, err
    }

    var packages []Package
    resp, err := s.client.Do(req, &packages)
    if err != nil {
        return nil, resp, err
    }

    return packages, resp, err

}

func (s *PackageService) Get(packageName string) (*Package, *http.Response, error) {
    route := fmt.Sprintf("packages/%s", url.QueryEscape(packageName))

    req, err := s.client.NewRequest("GET", route, nil)
    if err != nil {
        return nil, nil, err
    }

    p := new(Package)
    resp, err := s.client.Do(req, &p)
    if err != nil {
        return nil, resp, err
    }

    return p, resp, nil

}

func (s *PackageService) Insert(x_package PackageInterface, overwrite bool) (*Package, *http.Response, error) {
    route := fmt.Sprintf("packages/%s?overwrite=%t", url.QueryEscape(x_package.GetName()), overwrite)

    req, err := s.client.NewRequest("PUT", route, x_package)
    if err != nil {
        if IsDebug() {
            fmt.Printf("PackageService.Insert: http.NewRequest(PUT, %s); error '%s'\n", route, err)
        }
        errStr := fmt.Sprintf("Unable to create PUT HTTP request for '%s'; error: %s", route, err)
        werr := MakeWskErrorFromWskError(errors.New(errStr), err, EXITCODE_ERR_GENERAL, DISPLAY_MSG, NO_DISPLAY_USAGE)
        return nil, nil, werr
    }

    p := new(Package)
    resp, err := s.client.Do(req, &p)
    if err != nil {
        if IsDebug() {
            fmt.Printf("PackageService.Insert: s.client.Do() error - HTTP req %s; error '%s'\n", req.URL.String(), err)
        }
        errStr := fmt.Sprintf("Request failure: %s", err)
        werr := MakeWskErrorFromWskError(errors.New(errStr), err, EXITCODE_ERR_NETWORK, DISPLAY_MSG, NO_DISPLAY_USAGE)
        return nil, resp, werr
    }

    return p, resp, nil
}

func (s *PackageService) Delete(packageName string) (*http.Response, error) {
    route := fmt.Sprintf("packages/%s", url.QueryEscape(packageName))

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

func (s *PackageService) Refresh() (*BindingUpdates, *http.Response, error) {
    route := "packages/refresh"

    req, err := s.client.NewRequest("POST", route, nil)
    if err != nil {
        return nil, nil, err
    }

    updates := &BindingUpdates{}
    resp, err := s.client.Do(req, updates)
    if err != nil {
        return nil, resp, err
    }

    return updates, resp, nil
}
