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
