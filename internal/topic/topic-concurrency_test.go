package topic

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/IBM/sarama"
)

type concurrentTopicClient struct {
	topicReadClient
	active atomic.Int32
	peak   atomic.Int32
}

func (c *concurrentTopicClient) trackRead() {
	active := c.active.Add(1)
	for peak := c.peak.Load(); active > peak; peak = c.peak.Load() {
		if c.peak.CompareAndSwap(peak, active) {
			break
		}
	}
	time.Sleep(time.Millisecond)
	c.active.Add(-1)
}

func (c *concurrentTopicClient) Partitions(name string) ([]int32, error) {
	c.trackRead()
	return c.topicReadClient.Partitions(name)
}

func (c *concurrentTopicClient) GetOffset(string, int32, int64) (int64, error) {
	c.trackRead()
	return 17, nil
}

func TestReadTopicsBoundsConcurrentReads(t *testing.T) {
	readClient := &concurrentTopicClient{topicReadClient: topicReadClient{partitionCount: 1}}
	var client sarama.Client = readClient
	var admin sarama.ClusterAdmin = &topicConfigAdmin{}
	names := make([]string, 64)
	for i := range names {
		names[i] = fmt.Sprintf("example-events-%d", i)
	}
	fields := requestedTopicFields{partitionID: true, partitionReplicas: true, config: NoConfigs}
	topics := readTopics(&client, &admin, names, fields)
	if len(topics) != len(names) || readClient.peak.Load() > maxConcurrentTopicReads {
		t.Fatalf("topics=%d, peak concurrent reads=%d", len(topics), readClient.peak.Load())
	}
}

func TestReadTopicBoundsConcurrentPartitionReads(t *testing.T) {
	readClient := &concurrentTopicClient{topicReadClient: topicReadClient{partitionCount: 64}}
	var client sarama.Client = readClient
	var admin sarama.ClusterAdmin = &topicConfigAdmin{}
	topic, err := readTopic(&client, &admin, "example-events", allFields)
	if err != nil || len(topic.Partitions) != 64 || readClient.peak.Load() > maxConcurrentPartitionReads {
		t.Fatalf("partitions=%d, peak concurrent reads=%d, err=%v", len(topic.Partitions), readClient.peak.Load(), err)
	}
}
