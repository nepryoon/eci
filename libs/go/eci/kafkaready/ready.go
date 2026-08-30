// Package kafkaready exposes a fail-closed HTTP readiness check for Kafka
// consumers. It verifies authenticated topic metadata and group-coordinator
// access without waiting for an event to exist on an otherwise idle topic.
package kafkaready

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

const checkTimeout = 2 * time.Second

type kafkaClient interface {
	Metadata(context.Context, *kafka.MetadataRequest) (*kafka.MetadataResponse, error)
	FindCoordinator(context.Context, *kafka.FindCoordinatorRequest) (*kafka.FindCoordinatorResponse, error)
	OffsetFetch(context.Context, *kafka.OffsetFetchRequest) (*kafka.OffsetFetchResponse, error)
}

// Checker verifies the exact topic and consumer-group capabilities used by a
// worker. Configuration is trusted process configuration, never request data.
type Checker struct {
	client kafkaClient
	group  string
	topics []string
}

// New constructs a checker over the same authenticated transport as the
// worker's Reader and Writer.
func New(brokers []string, transport kafka.RoundTripper, group string, topics []string) (*Checker, error) {
	if len(brokers) == 0 || slices.Contains(brokers, "") {
		return nil, fmt.Errorf("kafkaready: brokers must be non-empty")
	}
	if strings.TrimSpace(group) == "" || group != strings.TrimSpace(group) {
		return nil, fmt.Errorf("kafkaready: group must be non-blank without surrounding whitespace")
	}
	if len(topics) == 0 {
		return nil, fmt.Errorf("kafkaready: at least one topic is required")
	}
	for _, topic := range topics {
		if strings.TrimSpace(topic) == "" || topic != strings.TrimSpace(topic) {
			return nil, fmt.Errorf("kafkaready: topics must be non-blank without surrounding whitespace")
		}
	}
	client := &kafka.Client{Addr: kafka.TCP(brokers...), Transport: transport, Timeout: checkTimeout}
	return newChecker(client, group, topics), nil
}

func newChecker(client kafkaClient, group string, topics []string) *Checker {
	return &Checker{client: client, group: group, topics: slices.Clone(topics)}
}

// Check proves the broker accepted the worker identity for every input topic
// and its consumer group. It performs no write and never consumes an event.
func (c *Checker) Check(ctx context.Context) error {
	metadata, err := c.client.Metadata(ctx, &kafka.MetadataRequest{Topics: c.topics})
	if err != nil {
		return fmt.Errorf("kafkaready: metadata request failed: %w", err)
	}
	if metadata == nil {
		return fmt.Errorf("kafkaready: metadata response missing")
	}
	found := make(map[string]bool, len(metadata.Topics))
	for _, topic := range metadata.Topics {
		if topic.Error != nil {
			return fmt.Errorf("kafkaready: topic %q unavailable: %w", topic.Name, topic.Error)
		}
		if len(topic.Partitions) == 0 {
			return fmt.Errorf("kafkaready: topic %q has no partitions", topic.Name)
		}
		found[topic.Name] = true
	}
	for _, topic := range c.topics {
		if !found[topic] {
			return fmt.Errorf("kafkaready: topic %q absent from metadata", topic)
		}
	}

	coordinator, err := c.client.FindCoordinator(ctx, &kafka.FindCoordinatorRequest{
		Key: c.group, KeyType: kafka.CoordinatorKeyTypeConsumer,
	})
	if err != nil {
		return fmt.Errorf("kafkaready: coordinator request failed: %w", err)
	}
	if coordinator == nil || coordinator.Error != nil {
		if coordinator != nil && coordinator.Error != nil {
			return fmt.Errorf("kafkaready: group unavailable: %w", coordinator.Error)
		}
		return fmt.Errorf("kafkaready: coordinator response missing")
	}
	if coordinator.Coordinator == nil || coordinator.Coordinator.Host == "" || coordinator.Coordinator.Port <= 0 {
		return fmt.Errorf("kafkaready: coordinator endpoint missing")
	}
	partitions := make(map[string][]int, len(metadata.Topics))
	for _, topic := range metadata.Topics {
		for _, partition := range topic.Partitions {
			partitions[topic.Name] = append(partitions[topic.Name], partition.ID)
		}
	}
	offsets, err := c.client.OffsetFetch(ctx, &kafka.OffsetFetchRequest{
		Addr: kafka.TCP(net.JoinHostPort(
			coordinator.Coordinator.Host,
			strconv.Itoa(coordinator.Coordinator.Port),
		)),
		GroupID: c.group,
		Topics:  partitions,
	})
	if err != nil {
		return fmt.Errorf("kafkaready: group offset request failed: %w", err)
	}
	if offsets == nil || offsets.Error != nil {
		if offsets != nil && offsets.Error != nil {
			return fmt.Errorf("kafkaready: group offset access denied: %w", offsets.Error)
		}
		return fmt.Errorf("kafkaready: group offset response missing")
	}
	for topic, topicPartitions := range offsets.Topics {
		for _, partition := range topicPartitions {
			if partition.Error != nil {
				return fmt.Errorf("kafkaready: group offset for %q/%d unavailable: %w", topic, partition.Partition, partition.Error)
			}
		}
	}
	for topic, requested := range partitions {
		if len(offsets.Topics[topic]) != len(requested) {
			return fmt.Errorf("kafkaready: group offsets for topic %q incomplete", topic)
		}
	}
	return nil
}

// Handler returns only a status code. Broker, TLS and ACL details stay inside
// the process and cannot leak through the readiness endpoint.
func Handler(check func(context.Context) error) http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		ctx, cancel := context.WithTimeout(request.Context(), checkTimeout)
		defer cancel()
		response.Header().Set("Cache-Control", "no-store")
		if check == nil || check(ctx) != nil {
			response.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		response.WriteHeader(http.StatusNoContent)
	})
}
