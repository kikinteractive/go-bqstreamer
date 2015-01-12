package main

import (
	"io/ioutil"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"golang.org/x/oauth2/jwt"
	bigquery "google.golang.org/api/bigquery/v2"
)

const (
	// Error channel buffer size.
	errorBufferSize = 20
)

// A row associates a single table's row to its project, dataset and table.
type row struct {
	projectID string
	datasetID string
	tableID   string
	data      map[string]bigquery.JsonValue
}

// NewJWTConfig returns a new JWT configuration from a JSON key,
// acquired via https://console.developers.google.com
// A config is used to authenticate with Google OAuth2.
func NewJWTConfig(keyPath string) (c *jwt.Config, err error) {
	keyBytes, err := ioutil.ReadFile(keyPath)
	if err != nil {
		return
	}

	c, err = google.JWTConfigFromJSON(
		keyBytes,
		"https://www.googleapis.com/auth/bigquery")

	// No need to check if err != nil since we return anyways.
	return
}

// NewBigQueryService returns a new BigQuery service (client), authenticated via OAuth2/JWT.
// NOTE: This function contacts (authenticates with) Google OAuth2 service,
// thus susceptible to network delays and blocks.
func NewBigQueryService(c *jwt.Config) (service *bigquery.Service, err error) {
	// Create *http.Client.
	client := c.Client(oauth2.NoContext)

	// Create authenticated BigQuery service.
	service, err = bigquery.New(client)

	// No need to check if err != nil since we return anyways.
	return
}
