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
