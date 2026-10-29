package topic

import (
	"sort"

	"github.com/IBM/sarama"
	"github.com/deviceinsight/kafkactl/v5/internal"
	"github.com/deviceinsight/kafkactl/v5/internal/output"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

func readTopic(client *sarama.Client, admin *sarama.ClusterAdmin, name string, requestedFields requestedTopicFields) (Topic, error) {
	var (
		err error
		ps  []int32
		top = Topic{Name: name}
	)

	if !requestedFields.partitionID {
		return top, nil
	}

	if ps, err = (*client).Partitions(name); err != nil {
		return top, err
	}

	var reads errgroup.Group
	top.Partitions = make([]Partition, len(ps))

	// read partitions in parallel
	for i, p := range ps {

		reads.Go(func() error {
			partitionId := p
			var err error

			np := Partition{ID: partitionId}

			if requestedFields.partitionOffset {
				if np.OldestOffset, err = (*client).GetOffset(name, partitionId, sarama.OffsetOldest); err != nil {
					output.Warnf("unable to read oldest offset for topic %s partition %d", name, partitionId)
				}

				if np.NewestOffset, err = (*client).GetOffset(name, partitionId, sarama.OffsetNewest); err != nil {
					output.Warnf("unable to read newest offset for topic %s partition %d", name, partitionId)
				}
			}

			if requestedFields.partitionLeader {
				var led *sarama.Broker
				if led, err = (*client).Leader(name, partitionId); err != nil {
					output.Warnf("unable to read leader for topic %s partition %d", name, partitionId)
					np.Leader = "none"
				} else {
					np.Leader = led.Addr()
				}
			}

			if requestedFields.partitionReplicas {
				if np.Replicas, err = (*client).Replicas(name, partitionId); err != nil {
					return errors.Wrapf(err, "unable to read replicas for topic %s partition %d", name, partitionId)
				} else {
					sort.Slice(np.Replicas, func(i, j int) bool { return np.Replicas[i] < np.Replicas[j] })
				}
			}

			if requestedFields.partitionISRs {
				if np.ISRs, err = (*client).InSyncReplicas(name, partitionId); err != nil {
					output.Warnf("unable to read inSyncReplicas for topic %s partition %d", name, partitionId)
				} else {
					sort.Slice(np.ISRs, func(i, j int) bool { return np.ISRs[i] < np.ISRs[j] })
				}
			}

			top.Partitions[i] = np
			return nil
		})
	}

	if err := reads.Wait(); err != nil {
		return top, err
	}

	sort.Slice(top.Partitions, func(i, j int) bool {
		return top.Partitions[i].ID < top.Partitions[j].ID
	})

	top.ReplicationFactor = replicationFactor(top)

	if requestedFields.config != "" && requestedFields.config != NoConfigs {

		topicConfig := sarama.ConfigResource{
			Type: sarama.TopicResource,
			Name: name,
		}

		if top.Configs, err = internal.ListConfigs(admin, topicConfig, requestedFields.config == AllConfigs); err != nil {
			return top, err
		}
	}

	return top, nil
}

// replicationFactor for topic calculated as minimal replication factor across partitions.
func replicationFactor(t Topic) int {
	var factor int
	for _, partition := range t.Partitions {
		if len(partition.Replicas) < factor || factor == 0 {
			factor = len(partition.Replicas)
		}
	}

	return factor
}

func readTopics(client *sarama.Client, admin *sarama.ClusterAdmin, topics []string, requestedFields requestedTopicFields) ([]Topic, error) {
	var reads errgroup.Group
	topicList := make([]Topic, len(topics))

	// read topics in parallel
	for i, topic := range topics {
		reads.Go(func() error {
			t, err := readTopic(client, admin, topic, requestedFields)
			if err != nil {
				return errors.Wrapf(err, "failed to read topic %q", topic)
			}
			topicList[i] = t
			return nil
		})
	}

	if err := reads.Wait(); err != nil {
		return nil, err
	}

	sort.Slice(topicList, func(i, j int) bool {
		return topicList[i].Name < topicList[j].Name
	})

	return topicList, nil
}
