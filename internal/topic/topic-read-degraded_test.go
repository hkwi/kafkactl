package topic

import (
	"errors"
	"strings"
	"testing"

	"github.com/IBM/sarama"
)

type unavailableReplicaClient struct {
	topicReadClient
	replicas []int32
}

func (c *unavailableReplicaClient) Replicas(string, int32) ([]int32, error) {
	return append([]int32(nil), c.replicas...), sarama.ErrReplicaNotAvailable
}

func (c *unavailableReplicaClient) InSyncReplicas(string, int32) ([]int32, error) {
	return []int32{1, 2}, sarama.ErrReplicaNotAvailable
}

func TestReadTopicPreservesUnavailableReplicaMetadata(t *testing.T) {
	warnings := captureTopicWarnings(t)
	var client sarama.Client = &unavailableReplicaClient{topicReadClient{partitionCount: 1}, []int32{3, 1, 2}}
	var admin sarama.ClusterAdmin = &topicConfigAdmin{}
	topic, err := readTopic(&client, &admin, "example-events", allFields)
	if err != nil || topic.ReplicationFactor != 3 || len(topic.Partitions) != 1 {
		t.Fatalf("usable replica metadata was discarded: topic=%v, err=%v", topic, err)
	}
	partition := topic.Partitions[0]
	if len(partition.Replicas) != 3 || partition.Replicas[0] != 1 || len(partition.ISRs) != 2 {
		t.Fatalf("replica/ISR data was lost: %+v", partition)
	}
	if text := warnings(); !strings.Contains(text, sarama.ErrReplicaNotAvailable.Error()) {
		t.Fatalf("replica availability warning omitted its cause: %s", text)
	}
}

func TestReadTopicRejectsUnavailableReplicasWithoutMetadata(t *testing.T) {
	var client sarama.Client = &unavailableReplicaClient{topicReadClient: topicReadClient{partitionCount: 1}}
	var admin sarama.ClusterAdmin = &topicConfigAdmin{}
	_, err := readTopic(&client, &admin, "example-events", allFields)
	if !errors.Is(err, sarama.ErrReplicaNotAvailable) {
		t.Fatalf("missing replica IDs must not become an RF=0 success: %v", err)
	}
}

type failedOptionalReadsClient struct {
	topicReadClient
}

func (c *failedOptionalReadsClient) GetOffset(string, int32, int64) (int64, error) {
	return 0, errors.New("synthetic offset failure")
}

func (c *failedOptionalReadsClient) Leader(string, int32) (*sarama.Broker, error) {
	return nil, errors.New("synthetic leader failure")
}

func (c *failedOptionalReadsClient) InSyncReplicas(string, int32) ([]int32, error) {
	return nil, errors.New("synthetic ISR failure")
}

func TestReadTopicWarnsWithUnderlyingOptionalReadErrors(t *testing.T) {
	warnings := captureTopicWarnings(t)
	var client sarama.Client = &failedOptionalReadsClient{topicReadClient{partitionCount: 1}}
	var admin sarama.ClusterAdmin = &topicConfigAdmin{}
	topic, err := readTopic(&client, &admin, "example-events", allFields)
	if err != nil || topic.ReplicationFactor != 3 {
		t.Fatalf("optional failures should not hide replica metadata: topic=%v, err=%v", topic, err)
	}
	for _, cause := range []string{"synthetic offset failure", "synthetic leader failure", "synthetic ISR failure"} {
		if text := warnings(); !strings.Contains(text, cause) {
			t.Errorf("warning omitted %q: %s", cause, text)
		}
	}
}
