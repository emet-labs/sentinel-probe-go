package client

import (
	"net/http"

	"connectrpc.com/connect"

	"github.com/emet-labs/sentinel-probe-go/gen/sentinel/probe/v1/probev1connect"
)

// TransportOptions configures the Connect client for Sentinel's decision endpoint.
// Analog of transport.ts's TransportOptions.
type TransportOptions struct {
	// BaseURL of the Sentinel decision endpoint, for example "http://sentinel.local:7070".
	BaseURL string
	// HTTPClient to issue requests with. Defaults to http.DefaultClient when nil. Hosts
	// override it to install timeouts, connection pools, or auth round-trippers.
	HTTPClient connect.HTTPClient
	// ClientOptions are passed through to connect, for example interceptors that attach
	// source authentication headers.
	ClientOptions []connect.ClientOption
}

// NewSentinelClient builds a Connect client for SentinelDecisionService.
//
// Connect is the right transport here for the same reason the TypeScript reference chose it,
// and more so in Go: connectrpc.com/connect is built directly on stdlib net/http — the client
// is an *http.Client and the server is an http.Handler — which is exactly the "stdlib
// net/http host language" issue #33 names. grpc-go plus protoc-gen-go-grpc would drag in
// HTTP/2 gRPC framing and is not stdlib net/http.
//
// HTTP/2 is deliberately NOT forced. The TypeScript reference passes httpVersion: "2" to
// createConnectTransport, which was flagged on its review as an untested assumption about a
// server that does not exist yet. http.DefaultClient speaks HTTP/1.1 and Connect works over
// both; whoever lands the decision endpoint (#22) decides, and hosts can override
// HTTPClient. Note that Connect's gRPC and gRPC-Web protocols do require HTTP/2 with prior
// knowledge for cleartext, so a host selecting connect.WithGRPC must supply a matching
// HTTPClient.
func NewSentinelClient(opts TransportOptions) probev1connect.SentinelDecisionServiceClient {
	httpClient := opts.HTTPClient
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	return probev1connect.NewSentinelDecisionServiceClient(httpClient, opts.BaseURL, opts.ClientOptions...)
}
