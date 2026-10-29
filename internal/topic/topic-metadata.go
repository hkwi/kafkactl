package topic

import (
	"time"

	"github.com/IBM/sarama"
	"github.com/pkg/errors"
)

func readTopicAfterPartitionChange(client *sarama.Client, admin *sarama.ClusterAdmin, name string, count int32) (Topic, error) {
	deadline := time.Now().Add((*client).Config().Admin.Timeout)
	for {
		if err := (*client).RefreshMetadata(name); err != nil {
			return Topic{}, err
		}
		t, err := readTopic(client, admin, name, allFields)
		if err != nil {
			return t, err
		}
		if len(t.Partitions) >= int(count) {
			return t, nil
		}
		if !time.Now().Before(deadline) {
			return t, errors.Errorf("topic %q has %d partitions in metadata, expected at least %d", name, len(t.Partitions), count)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
