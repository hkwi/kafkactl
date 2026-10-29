package topic

import (
	"errors"
	"sync/atomic"
	"testing"

	"github.com/IBM/sarama"
)

type topicReadClient struct {
	sarama.Client
	partitionCount int
	partitionsErr  error
	replicasErr    error
}

func (c *topicReadClient) Partitions(string) ([]int32, error) {
	partitions := make([]int32, c.partitionCount)
	for i := range partitions {
		partitions[i] = int32(i)
	}
	return partitions, c.partitionsErr
}

func (c *topicReadClient) Replicas(string, int32) ([]int32, error) {
	if c.replicasErr != nil {
		return nil, c.replicasErr
	}
	return []int32{3, 1, 2}, nil
}

func (c *topicReadClient) InSyncReplicas(string, int32) ([]int32, error) {
	return []int32{3, 1, 2}, nil
}

func (c *topicReadClient) GetOffset(string, int32, int64) (int64, error) {
	return 17, nil
}

func (c *topicReadClient) Leader(string, int32) (*sarama.Broker, error) {
	return sarama.NewBroker("broker.example:9092"), nil
}

type topicConfigAdmin struct {
	sarama.ClusterAdmin
	err   error
	calls atomic.Int32
}

func (a *topicConfigAdmin) DescribeConfigs(resources []*sarama.ConfigResource, _ sarama.DescribeConfigsOptions) ([]*sarama.ConfigResourceResult, error) {
	a.calls.Add(1)
	if a.err != nil {
		return nil, a.err
	}
	results := make([]*sarama.ConfigResourceResult, len(resources))
	for i, resource := range resources {
		results[i] = &sarama.ConfigResourceResult{Type: resource.Type, Name: resource.Name}
	}
	return results, nil
}

func TestReadTopicWithoutConfigs(t *testing.T) {
	for _, config := range []PrintConfigsParam{"", NoConfigs} {
		t.Run(string(config), func(t *testing.T) {
			var client sarama.Client = &topicReadClient{partitionCount: 1}
			configAdmin := &topicConfigAdmin{err: errors.New("synthetic config failure")}
			var admin sarama.ClusterAdmin = configAdmin
			fields := requestedTopicFields{partitionID: true, partitionReplicas: true, config: config}
			topic, err := readTopic(&client, &admin, "example-events", fields)
			if err != nil {
				t.Fatalf("metadata-only read failed: %v", err)
			}
			if topic.ReplicationFactor != 3 || configAdmin.calls.Load() != 0 {
				t.Fatalf("RF=%d, DescribeConfigs calls=%d; want RF=3 without config requests", topic.ReplicationFactor, configAdmin.calls.Load())
			}
		})
	}
}

func TestReadTopicPropagatesUnusableReadErrors(t *testing.T) {
	syntheticError := errors.New("synthetic read failure")
	for _, failure := range []string{"partitions", "replicas", "configs"} {
		t.Run(failure, func(t *testing.T) {
			readClient := &topicReadClient{partitionCount: 1}
			configAdmin := &topicConfigAdmin{}
			fields := requestedTopicFields{partitionID: true, partitionReplicas: true, config: AllConfigs}
			switch failure {
			case "partitions":
				readClient.partitionsErr = syntheticError
			case "replicas":
				readClient.replicasErr = syntheticError
			case "configs":
				configAdmin.err = syntheticError
			}
			var client sarama.Client = readClient
			var admin sarama.ClusterAdmin = configAdmin
			_, err := readTopic(&client, &admin, "example-events", fields)
			if !errors.Is(err, syntheticError) {
				t.Fatalf("expected read error, got %v", err)
			}
		})
	}
}

func TestReadTopicConcurrentPartitions(t *testing.T) {
	var client sarama.Client = &topicReadClient{partitionCount: 32}
	var admin sarama.ClusterAdmin = &topicConfigAdmin{}
	topic, err := readTopic(&client, &admin, "example-events", allFields)
	if err != nil {
		t.Fatal(err)
	}
	if topic.ReplicationFactor != 3 || len(topic.Partitions) != 32 {
		t.Fatalf("unexpected topic: %+v", topic)
	}
	for i, partition := range topic.Partitions {
		if partition.ID != int32(i) || partition.Leader != "broker.example:9092" || len(partition.Replicas) != 3 || len(partition.ISRs) != 3 {
			t.Fatalf("unexpected partition: %+v", partition)
		}
	}
}

func TestReadTopicsSuccessfulResultsAreSorted(t *testing.T) {
	var client sarama.Client = &topicReadClient{partitionCount: 1}
	var admin sarama.ClusterAdmin = &topicConfigAdmin{}
	fields := requestedTopicFields{partitionID: true, partitionReplicas: true, config: NoConfigs}
	topics := readTopics(&client, &admin, []string{"example-z", "example-a"}, fields)
	if len(topics) != 2 || topics[0].Name != "example-a" || topics[1].Name != "example-z" {
		t.Fatalf("unexpected topic order: %v", topics)
	}
}
