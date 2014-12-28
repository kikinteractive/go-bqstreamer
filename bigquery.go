package main

import (
	"io/ioutil"
	"net/http"

	"code.google.com/p/goauth2/oauth"     // TODO Deprecated, use google.golang.org
	"code.google.com/p/goauth2/oauth/jwt" // TODO Deprecated, use google.golang.org
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

// NewJWTToken returns a new JWT (OAuth2) token from an email and PEM key.
func NewJWTToken(email, pemPath string) (t *jwt.Token, err error) {
	pemBytes, err := ioutil.ReadFile(pemPath)
	if err != nil {
		return
	}

	t = jwt.NewToken(email, bigquery.BigqueryScope, pemBytes)

	return
}

// NewBigQueryService returns a new BigQuery service (client), authenticated via JWT (OAuth2).
// NOTE: This function contacts (authenticates with) Google OAuth2 service,
// thus susceptible to network delays and blocks.
func NewBigQueryService(t *jwt.Token) (service *bigquery.Service, err error) {
	// Generate OAuth2 tokens.
	httpClient := new(http.Client)
	token, err := t.Assert(httpClient)
	if err != nil {
		return
	}

	// Create OAuth2 client.
	config := &oauth.Config{
		Scope:    bigquery.BigqueryScope,
		AuthURL:  "https://accounts.google.com/o/oauth2/auth",
		TokenURL: "https://accounts.google.com/o/oauth2/token",
	}
	transport := &oauth.Transport{Config: config, Token: token}
	client := transport.Client()

	// Create BigQuery service, authenticated.
	service, err = bigquery.New(client)
	if err != nil {
		return
	}

	return
}
