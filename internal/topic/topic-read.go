package topic

import (
	"sort"

	"github.com/IBM/sarama"
	"github.com/deviceinsight/kafkactl/v5/internal"
	"github.com/deviceinsight/kafkactl/v5/internal/output"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

const maxConcurrentPartitionReads = 16

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
		return top, errors.Wrapf(err, "unable to read partitions for topic %s", name)
	}

	var reads errgroup.Group
	reads.SetLimit(maxConcurrentPartitionReads)
	top.Partitions = make([]Partition, len(ps))

	// read partitions in parallel
	for i, p := range ps {

		reads.Go(func() error {
			partitionID := p
			var err error

			np := Partition{ID: partitionID}

			if requestedFields.partitionOffset {
				if np.OldestOffset, err = (*client).GetOffset(name, partitionID, sarama.OffsetOldest); err != nil {
					output.Warnf("unable to read oldest offset for topic %s partition %d: %v", name, partitionID, err)
				}

				if np.NewestOffset, err = (*client).GetOffset(name, partitionID, sarama.OffsetNewest); err != nil {
					output.Warnf("unable to read newest offset for topic %s partition %d: %v", name, partitionID, err)
				}
			}

			if requestedFields.partitionLeader {
				var led *sarama.Broker
				if led, err = (*client).Leader(name, partitionID); err != nil {
					output.Warnf("unable to read leader for topic %s partition %d: %v", name, partitionID, err)
					np.Leader = "none"
				} else {
					np.Leader = led.Addr()
				}
			}

			if requestedFields.partitionReplicas {
				if np.Replicas, err = (*client).Replicas(name, partitionID); err != nil {
					if !errors.Is(err, sarama.ErrReplicaNotAvailable) || len(np.Replicas) == 0 {
						return errors.Wrapf(err, "unable to read replicas for topic %s partition %d", name, partitionID)
					}
					output.Warnf("replica unavailable for topic %s partition %d; using assigned replica IDs: %v", name, partitionID, err)
				}
				sort.Slice(np.Replicas, func(i, j int) bool { return np.Replicas[i] < np.Replicas[j] })
			}

			if requestedFields.partitionISRs {
				if np.ISRs, err = (*client).InSyncReplicas(name, partitionID); err != nil {
					output.Warnf("unable to read inSyncReplicas for topic %s partition %d: %v", name, partitionID, err)
				}
				sort.Slice(np.ISRs, func(i, j int) bool { return np.ISRs[i] < np.ISRs[j] })
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

	top.Configs, err = readTopicConfigs(admin, name, requestedFields.config)
	return top, err
}

func readTopicConfigs(admin *sarama.ClusterAdmin, name string, config PrintConfigsParam) ([]internal.Config, error) {
	if config != AllConfigs && config != NonDefaultConfigs {
		return nil, nil
	}
	resource := sarama.ConfigResource{Type: sarama.TopicResource, Name: name}
	return internal.ListConfigs(admin, resource, config == AllConfigs)
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
