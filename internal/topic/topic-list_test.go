package topic

import (
	"errors"
	"os"
	"strings"
	"testing"

	"github.com/IBM/sarama"
	"github.com/deviceinsight/kafkactl/v5/internal/output"
)

type failingTopicClient struct {
	topicReadClient
}

func (c *failingTopicClient) Partitions(name string) ([]int32, error) {
	if name == "example-removed" {
		return nil, sarama.ErrUnknownTopicOrPartition
	}
	return c.topicReadClient.Partitions(name)
}

type restrictedTopicAdmin struct {
	topicConfigAdmin
}

func (a *restrictedTopicAdmin) DescribeConfigs(resources []*sarama.ConfigResource, options sarama.DescribeConfigsOptions) ([]*sarama.ConfigResourceResult, error) {
	if resources[0].Name == "example-restricted" {
		return nil, sarama.ErrTopicAuthorizationFailed
	}
	return a.topicConfigAdmin.DescribeConfigs(resources, options)
}

func captureTopicWarnings(t *testing.T) func() string {
	t.Helper()
	file, err := os.CreateTemp(t.TempDir(), "warnings")
	if err != nil {
		t.Fatal(err)
	}
	previous := output.IoStreams.ErrOut
	output.IoStreams.ErrOut = file
	t.Cleanup(func() {
		output.IoStreams.ErrOut = previous
		_ = file.Close()
	})
	return func() string {
		t.Helper()
		data, err := os.ReadFile(file.Name())
		if err != nil {
			t.Fatal(err)
		}
		return string(data)
	}
}

func TestReadTopicsKeepsHealthyTopicsOnMetadataError(t *testing.T) {
	warnings := captureTopicWarnings(t)
	var client sarama.Client = &failingTopicClient{topicReadClient{partitionCount: 1}}
	var admin sarama.ClusterAdmin = &topicConfigAdmin{}
	fields := requestedTopicFields{partitionID: true, partitionReplicas: true, config: NoConfigs}
	topics := readTopics(&client, &admin, []string{"example-removed", "example-events"}, fields)
	if len(topics) != 1 || topics[0].Name != "example-events" || topics[0].ReplicationFactor != 3 {
		t.Fatalf("a removed topic hid healthy metadata: topics=%v", topics)
	}
	if text := warnings(); !strings.Contains(text, "example-removed") || !strings.Contains(text, sarama.ErrUnknownTopicOrPartition.Error()) {
		t.Fatalf("missing contextual warning: %s", text)
	}
}

func TestReadTopicsKeepsRFOnConfigError(t *testing.T) {
	for _, fields := range []requestedTopicFields{
		{partitionID: true, partitionReplicas: true, config: NonDefaultConfigs},
		allFields,
	} {
		t.Run(string(fields.config), func(t *testing.T) {
			warnings := captureTopicWarnings(t)
			var client sarama.Client = &topicReadClient{partitionCount: 1}
			var admin sarama.ClusterAdmin = &restrictedTopicAdmin{}
			topics := readTopics(&client, &admin, []string{"example-restricted", "example-events"}, fields)
			if len(topics) != 2 {
				t.Fatalf("a config ACL error hid topic metadata: topics=%v", topics)
			}
			for _, topic := range topics {
				if topic.ReplicationFactor != 3 || len(topic.Partitions) != 1 {
					t.Fatalf("config failure produced incomplete replica metadata: %+v", topic)
				}
			}
			if text := warnings(); !strings.Contains(text, "example-restricted") || !strings.Contains(text, sarama.ErrTopicAuthorizationFailed.Error()) {
				t.Fatalf("missing contextual config warning: %s", text)
			}
		})
	}
}

func TestReadTopicConfigErrorKeepsRFAndReturnsError(t *testing.T) {
	var client sarama.Client = &topicReadClient{partitionCount: 1}
	configError := errors.New("synthetic config failure")
	var admin sarama.ClusterAdmin = &topicConfigAdmin{err: configError}
	topic, err := readTopic(&client, &admin, "example-events", allFields)
	if !errors.Is(err, configError) || topic.ReplicationFactor != 3 {
		t.Fatalf("single-topic reads must preserve metadata and report config failure: topic=%v, err=%v", topic, err)
	}
}
