package topic

import (
	"errors"
	"fmt"
	"testing"

	"github.com/IBM/sarama"
)

func TestGetTargetReplicasDecrease(t *testing.T) {

	// 3 brokers, current replicas on brokers 1,2,3, target RF=1
	// Should keep broker with lowest replica count
	brokerReplicaCount := map[int32]int{
		1: 2,
		2: 3,
		3: 1,
	}

	replicas, err := getTargetReplicas([]int32{1, 2, 3}, brokerReplicaCount, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(replicas) != 1 {
		t.Fatalf("expected 1 replica, got %d", len(replicas))
	}
	// Should keep broker 3 (lowest replica count)
	if replicas[0] != 3 {
		t.Fatalf("expected broker 3, got %d", replicas[0])
	}
}

func TestGetTargetReplicasIncrease(t *testing.T) {

	// 3 brokers, current replicas on broker 1 only, target RF=3
	brokerReplicaCount := map[int32]int{
		1: 2,
		2: 0,
		3: 1,
	}

	replicas, err := getTargetReplicas([]int32{1}, brokerReplicaCount, 3)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(replicas) != 3 {
		t.Fatalf("expected 3 replicas, got %d", len(replicas))
	}
	// broker 1 was already there, brokers 2 and 3 should be added
	// broker 2 has the lowest count so should be added first
	if replicas[0] != 1 {
		t.Fatalf("expected broker 1 first, got %d", replicas[0])
	}
	if replicas[1] != 2 {
		t.Fatalf("expected broker 2 second, got %d", replicas[1])
	}
	if replicas[2] != 3 {
		t.Fatalf("expected broker 3 third, got %d", replicas[2])
	}
}

func TestGetTargetReplicasDecreaseWithUnavailableBroker(t *testing.T) {

	// Scenario from issue #256: broker 3 is unavailable
	// brokerReplicaCount only contains available brokers (1 and 2)
	// Current replicas include broker 3 (which is unavailable)
	brokerReplicaCount := map[int32]int{
		1: 2,
		2: 2,
	}

	// Partition has replicas on brokers 3 and 1
	// Since broker 3 is not in brokerReplicaCount (unavailable),
	// it should be removed first when decreasing
	replicas, err := getTargetReplicas([]int32{3, 1}, brokerReplicaCount, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(replicas) != 1 {
		t.Fatalf("expected 1 replica, got %d", len(replicas))
	}
	// Should keep broker 1 (available) and remove broker 3 (unavailable)
	if replicas[0] != 1 {
		t.Fatalf("expected broker 1, got %d", replicas[0])
	}
}

func TestGetTargetReplicasDecreaseMultipleUnavailableBrokers(t *testing.T) {

	// Multiple unavailable brokers should all be removed first
	brokerReplicaCount := map[int32]int{
		1: 3,
		2: 3,
	}

	// Replicas on brokers 3, 4, 1, 2 - brokers 3 and 4 are unavailable
	// Target RF=2, should remove unavailable brokers first
	replicas, err := getTargetReplicas([]int32{3, 4, 1, 2}, brokerReplicaCount, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(replicas) != 2 {
		t.Fatalf("expected 2 replicas, got %d", len(replicas))
	}
	// Should keep brokers 1 and 2 (available) and remove 3 and 4 (unavailable)
	for _, r := range replicas {
		if r == 3 || r == 4 {
			t.Fatalf("unavailable broker %d should have been removed", r)
		}
	}
}

func TestGetTargetReplicasDecreaseAllUnavailable(t *testing.T) {

	// Edge case: all current replicas are on unavailable brokers
	brokerReplicaCount := map[int32]int{
		1: 0,
		2: 0,
	}

	// Current replicas are on brokers 3 and 4 (both unavailable)
	replicas, err := getTargetReplicas([]int32{3, 4}, brokerReplicaCount, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should still work, keeping one of them
	if len(replicas) != 1 {
		t.Fatalf("expected 1 replica, got %d", len(replicas))
	}
}

func TestGetTargetReplicasDecreaseUnavailableThenIncrease(t *testing.T) {

	// All current replicas on unavailable brokers; result must use only available brokers.
	brokerReplicaCount := map[int32]int{
		1: 0,
		2: 0,
	}

	replicas, err := getTargetReplicas([]int32{3, 4, 5}, brokerReplicaCount, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(replicas) != 2 {
		t.Fatalf("expected 2 replicas, got %d", len(replicas))
	}
	for _, r := range replicas {
		if r == 3 || r == 4 || r == 5 {
			t.Fatalf("unavailable broker %d should not be in result %v", r, replicas)
		}
	}
}

func TestGetTargetReplicasNotEnoughBrokers(t *testing.T) {

	brokerReplicaCount := map[int32]int{
		1: 1,
	}

	_, err := getTargetReplicas([]int32{1}, brokerReplicaCount, 3)
	if err == nil {
		t.Fatal("expected error but got nil")
	}
	if err.Error() != "not enough brokers" {
		t.Fatalf("expected 'not enough brokers' error, got: %v", err)
	}
}

type topicReadClient struct {
	sarama.Client
	partitionCount int
	replicasErr    error
}

func (c *topicReadClient) Partitions(string) ([]int32, error) {
	partitions := make([]int32, c.partitionCount)
	for i := range partitions {
		partitions[i] = int32(i)
	}
	return partitions, nil
}

func (c *topicReadClient) Replicas(string, int32) ([]int32, error) {
	return []int32{1, 2, 3}, c.replicasErr
}

func (c *topicReadClient) InSyncReplicas(string, int32) ([]int32, error) {
	return []int32{1, 2, 3}, nil
}

func (c *topicReadClient) GetOffset(_ string, partition int32, _ int64) (int64, error) {
	return int64(partition) + 42, nil
}

func (c *topicReadClient) Leader(_ string, partition int32) (*sarama.Broker, error) {
	return sarama.NewBroker(fmt.Sprintf("broker-%d.example:9092", partition)), nil
}

type topicConfigAdmin struct {
	sarama.ClusterAdmin
	err   error
	calls int
}

func (a *topicConfigAdmin) DescribeConfigs(resources []*sarama.ConfigResource, _ sarama.DescribeConfigsOptions) ([]*sarama.ConfigResourceResult, error) {
	a.calls++
	if a.err != nil {
		return nil, a.err
	}
	results := make([]*sarama.ConfigResourceResult, len(resources))
	for i, resource := range resources {
		results[i] = &sarama.ConfigResourceResult{Type: resource.Type, Name: resource.Name}
	}
	return results, nil
}

func TestReadTopicConfigsPreserveReplicationFactor(t *testing.T) {
	for _, tc := range []struct {
		name   string
		config PrintConfigsParam
		denied bool
		calls  int
	}{
		{name: "default", denied: true},
		{name: "none", config: NoConfigs, denied: true},
		{name: "non-default", config: NonDefaultConfigs, calls: 1},
		{name: "all", config: AllConfigs, calls: 1},
		{name: "non-default denied", config: NonDefaultConfigs, denied: true, calls: 1},
		{name: "all denied", config: AllConfigs, denied: true, calls: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var client sarama.Client = &topicReadClient{partitionCount: 1}
			configAdmin := &topicConfigAdmin{}
			if tc.denied {
				configAdmin.err = sarama.ErrTopicAuthorizationFailed
			}
			var admin sarama.ClusterAdmin = configAdmin
			fields := requestedTopicFields{partitionID: true, partitionReplicas: true, config: tc.config}
			topic, err := readTopic(&client, &admin, "example-events", fields)
			if tc.denied && tc.calls != 0 {
				if !errors.Is(err, sarama.ErrTopicAuthorizationFailed) {
					t.Fatalf("expected config authorization error, got %v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if topic.ReplicationFactor != 3 || configAdmin.calls != tc.calls {
				t.Fatalf("RF=%d, config requests=%d; want RF=3, requests=%d", topic.ReplicationFactor, configAdmin.calls, tc.calls)
			}
		})
	}
}

func TestReadTopicPreservesUnavailableReplicaIDs(t *testing.T) {
	var client sarama.Client = &topicReadClient{partitionCount: 1, replicasErr: sarama.ErrReplicaNotAvailable}
	var admin sarama.ClusterAdmin = &topicConfigAdmin{}
	fields := requestedTopicFields{partitionID: true, partitionReplicas: true, config: NoConfigs}
	topic, err := readTopic(&client, &admin, "example-events", fields)
	if err != nil || topic.ReplicationFactor != 3 || len(topic.Partitions[0].Replicas) != 3 {
		t.Fatalf("assigned replica IDs must survive an unavailable broker: topic=%+v, err=%v", topic, err)
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
		t.Fatalf("unexpected topic metadata: %+v", topic)
	}
	for i, partition := range topic.Partitions {
		if partition.ID != int32(i) || partition.Leader != fmt.Sprintf("broker-%d.example:9092", i) ||
			partition.OldestOffset != int64(i)+42 || partition.NewestOffset != int64(i)+42 {
			t.Fatalf("partition metadata mixed between concurrent reads: %+v", partition)
		}
	}
}
