// Package kafkaconfig builds fail-closed Kafka transports from trusted process
// configuration. User input and message metadata never participate.
package kafkaconfig

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

type Config struct {
	Dialer    *kafka.Dialer
	Transport *kafka.Transport
}

func FromEnvironment() (Config, error) {
	enabled, err := strconv.ParseBool(envOrDefault("KAFKA_TLS_ENABLED", "false"))
	if err != nil {
		return Config{}, fmt.Errorf("KAFKA_TLS_ENABLED must be a boolean: %w", err)
	}
	if !enabled {
		if mtlsEnabled, mtlsErr := strconv.ParseBool(envOrDefault("KAFKA_MTLS_ENABLED", "false")); mtlsErr != nil {
			return Config{}, fmt.Errorf("KAFKA_MTLS_ENABLED must be a boolean: %w", mtlsErr)
		} else if mtlsEnabled {
			return Config{}, fmt.Errorf("KAFKA_TLS_ENABLED must be true when Kafka mTLS is enabled")
		}
		return Config{}, nil
	}
	caFile := strings.TrimSpace(os.Getenv("KAFKA_TLS_CA_FILE"))
	if caFile == "" {
		return Config{}, fmt.Errorf("KAFKA_TLS_CA_FILE is required when Kafka TLS is enabled")
	}
	data, err := os.ReadFile(caFile)
	if err != nil {
		return Config{}, fmt.Errorf("Kafka CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(data) {
		return Config{}, fmt.Errorf("Kafka CA file contains no PEM certificates")
	}
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: pool}
	mtlsEnabled, err := strconv.ParseBool(envOrDefault("KAFKA_MTLS_ENABLED", "false"))
	if err != nil {
		return Config{}, fmt.Errorf("KAFKA_MTLS_ENABLED must be a boolean: %w", err)
	}
	if mtlsEnabled {
		certFile := strings.TrimSpace(os.Getenv("KAFKA_TLS_CERT_FILE"))
		keyFile := strings.TrimSpace(os.Getenv("KAFKA_TLS_KEY_FILE"))
		if certFile == "" || keyFile == "" {
			return Config{}, fmt.Errorf("KAFKA_TLS_CERT_FILE and KAFKA_TLS_KEY_FILE are required when Kafka mTLS is enabled")
		}
		certificate, loadErr := tls.LoadX509KeyPair(certFile, keyFile)
		if loadErr != nil {
			return Config{}, fmt.Errorf("Kafka client identity: %w", loadErr)
		}
		tlsConfig.Certificates = []tls.Certificate{certificate}
	}
	return Config{
		Dialer:    &kafka.Dialer{Timeout: 10 * time.Second, DualStack: true, TLS: tlsConfig},
		Transport: &kafka.Transport{TLS: tlsConfig.Clone()},
	}, nil
}

func envOrDefault(name, fallback string) string {
	if value, ok := os.LookupEnv(name); ok {
		return value
	}
	return fallback
}
