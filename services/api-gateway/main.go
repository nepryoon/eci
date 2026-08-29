// Command api-gateway runs the internal ext-auth/SSE helper used by the
// public Envoy listener. Envoy, not this process, is the external boundary.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/eci-project/eci/libs/go/eci/observability"
	retrievalv1 "github.com/eci-project/eci/libs/go/eci/retrieval/v1"
	"github.com/eci-project/eci/services/api-gateway/internal/authn"
	"github.com/eci-project/eci/services/api-gateway/internal/edge"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	defaultMaxJSONBodyBytes = int64(1 << 20)
	maximumRequestTimeout   = 30 * time.Second
)

type processConfig struct {
	issuer                  string
	audience                string
	allowHTTPForDevelopment bool
	retrievalAddress        string
	listenAddress           string
	metricsAddress          string
	maxJSONBodyBytes        int64
	requestTimeout          time.Duration
}

func loadConfig() (processConfig, error) {
	config := processConfig{
		issuer:           os.Getenv("ECI_OIDC_ISSUER"),
		audience:         os.Getenv("ECI_OIDC_AUDIENCE"),
		retrievalAddress: os.Getenv("RETRIEVAL_ENGINE_ADDR"),
		listenAddress:    envOrDefault("API_GATEWAY_ADDR", ":8081"),
		metricsAddress:   envOrDefault("API_GATEWAY_METRICS_ADDR", ":9107"),
	}
	if !nonEmptyTrimmed(config.issuer) || !nonEmptyTrimmed(config.audience) || !nonEmptyTrimmed(config.retrievalAddress) {
		return processConfig{}, fmt.Errorf("ECI_OIDC_ISSUER, ECI_OIDC_AUDIENCE and RETRIEVAL_ENGINE_ADDR are required without surrounding whitespace")
	}
	if err := validateListenAddress(config.listenAddress); err != nil {
		return processConfig{}, fmt.Errorf("API_GATEWAY_ADDR: %w", err)
	}
	if err := validateListenAddress(config.metricsAddress); err != nil {
		return processConfig{}, fmt.Errorf("API_GATEWAY_METRICS_ADDR: %w", err)
	}
	host, _, err := net.SplitHostPort(config.retrievalAddress)
	if err != nil || host == "" {
		return processConfig{}, fmt.Errorf("RETRIEVAL_ENGINE_ADDR must be host:port")
	}

	allowHTTP := envOrDefault("ECI_OIDC_ALLOW_HTTP_DEV", "false")
	config.allowHTTPForDevelopment, err = strconv.ParseBool(allowHTTP)
	if err != nil {
		return processConfig{}, fmt.Errorf("ECI_OIDC_ALLOW_HTTP_DEV must be a boolean")
	}
	maxBody := envOrDefault("API_GATEWAY_MAX_JSON_BODY_BYTES", strconv.FormatInt(defaultMaxJSONBodyBytes, 10))
	config.maxJSONBodyBytes, err = strconv.ParseInt(maxBody, 10, 64)
	if err != nil || config.maxJSONBodyBytes <= 0 || config.maxJSONBodyBytes > defaultMaxJSONBodyBytes {
		return processConfig{}, fmt.Errorf("API_GATEWAY_MAX_JSON_BODY_BYTES must be between 1 and %d", defaultMaxJSONBodyBytes)
	}
	timeout := envOrDefault("API_GATEWAY_REQUEST_TIMEOUT", maximumRequestTimeout.String())
	config.requestTimeout, err = time.ParseDuration(timeout)
	if err != nil || config.requestTimeout <= 0 || config.requestTimeout > maximumRequestTimeout {
		return processConfig{}, fmt.Errorf("API_GATEWAY_REQUEST_TIMEOUT must be between 1ns and %s", maximumRequestTimeout)
	}
	return config, nil
}

func envOrDefault(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func nonEmptyTrimmed(value string) bool {
	return value != "" && value == strings.TrimSpace(value)
}

func validateListenAddress(address string) error {
	if !nonEmptyTrimmed(address) {
		return fmt.Errorf("must be a non-empty host:port")
	}
	if _, _, err := net.SplitHostPort(address); err != nil {
		return fmt.Errorf("must be host:port: %w", err)
	}
	return nil
}

func main() {
	config, err := loadConfig()
	if err != nil {
		log.Fatalf("api-gateway: configuration: %v", err)
	}

	rootContext, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	shutdownTracing, err := observability.InitTracing("api-gateway")
	if err != nil {
		log.Fatalf("api-gateway: init tracing: %v", err)
	}
	defer func() {
		shutdownContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := shutdownTracing(shutdownContext); err != nil {
			log.Printf("api-gateway: shutdown tracing: %v", err)
		}
	}()

	authenticator, err := authn.New(rootContext, authn.Config{
		Issuer:                  config.issuer,
		Audience:                config.audience,
		AllowHTTPForDevelopment: config.allowHTTPForDevelopment,
	}, prometheus.DefaultRegisterer)
	if err != nil {
		log.Fatalf("api-gateway: initialize OIDC authenticator: %v", err)
	}
	connection, err := grpc.NewClient(
		config.retrievalAddress,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithStatsHandler(otelgrpc.NewClientHandler()),
	)
	if err != nil {
		log.Fatalf("api-gateway: initialize retrieval client: %v", err)
	}
	defer connection.Close()

	handler, err := edge.NewEdgeHandler(
		authenticator,
		retrievalv1.NewRetrievalEngineClient(connection),
		edge.EdgeConfig{
			MaxJSONBodyBytes: config.maxJSONBodyBytes,
			RequestTimeout:   config.requestTimeout,
			Registerer:       prometheus.DefaultRegisterer,
		},
	)
	if err != nil {
		log.Fatalf("api-gateway: initialize edge helper: %v", err)
	}

	edgeServer := &http.Server{
		Addr:              config.listenAddress,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    16 * 1024,
	}
	metricsServer := &http.Server{
		Addr:              config.metricsAddress,
		Handler:           promhttp.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       30 * time.Second,
		MaxHeaderBytes:    8 * 1024,
	}
	errorsChannel := make(chan error, 2)
	go serveHTTP(edgeServer, "edge helper", errorsChannel)
	go serveHTTP(metricsServer, "metrics", errorsChannel)

	log.Printf("api-gateway: internal helper listening on %s (retrieval=%s metrics=%s)", config.listenAddress, config.retrievalAddress, config.metricsAddress)
	select {
	case <-rootContext.Done():
	case err := <-errorsChannel:
		log.Printf("api-gateway: server stopped unexpectedly: %v", err)
		stop()
	}

	shutdownContext, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := edgeServer.Shutdown(shutdownContext); err != nil {
		log.Printf("api-gateway: edge shutdown: %v", err)
	}
	if err := metricsServer.Shutdown(shutdownContext); err != nil {
		log.Printf("api-gateway: metrics shutdown: %v", err)
	}
}

func serveHTTP(server *http.Server, name string, errorsChannel chan<- error) {
	if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		errorsChannel <- fmt.Errorf("%s: %w", name, err)
	}
}
