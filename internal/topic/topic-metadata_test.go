package topic

import (
	"errors"
	"testing"
	"time"

	"github.com/IBM/sarama"
)

type refreshingTopicClient struct {
	topicReadClient
	refreshes  int
	refreshErr error
	timeout    time.Duration
}

func (c *refreshingTopicClient) Config() *sarama.Config {
	config := sarama.NewConfig()
	config.Admin.Timeout = c.timeout
	return config
}

func (c *refreshingTopicClient) RefreshMetadata(...string) error {
	c.refreshes++
	if c.refreshes > 1 {
		c.partitionCount = 6
	}
	return c.refreshErr
}

func TestReadTopicAfterPartitionChangeRefreshesStaleMetadata(t *testing.T) {
	readClient := &refreshingTopicClient{topicReadClient: topicReadClient{partitionCount: 3}, timeout: time.Second}
	var client sarama.Client = readClient
	var admin sarama.ClusterAdmin = &topicConfigAdmin{}
	topic, err := readTopicAfterPartitionChange(&client, &admin, "example-events", 6)
	if err != nil || len(topic.Partitions) != 6 || readClient.refreshes != 2 {
		t.Fatalf("stale partition metadata survived: topic=%v, refreshes=%d, err=%v", topic, readClient.refreshes, err)
	}
}

func TestReadTopicAfterPartitionChangeReportsRefreshFailure(t *testing.T) {
	for _, refreshErr := range []error{nil, sarama.ErrOutOfBrokers} {
		readClient := &refreshingTopicClient{topicReadClient: topicReadClient{partitionCount: 3}, refreshErr: refreshErr}
		var client sarama.Client = readClient
		var admin sarama.ClusterAdmin = &topicConfigAdmin{}
		_, err := readTopicAfterPartitionChange(&client, &admin, "example-events", 6)
		if err == nil || (refreshErr != nil && !errors.Is(err, refreshErr)) {
			t.Fatalf("refreshErr=%v: expected metadata error, got %v", refreshErr, err)
		}
	}
}
