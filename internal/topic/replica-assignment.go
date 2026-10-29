package topic

import (
	"sort"

	"github.com/IBM/sarama"
	"github.com/deviceinsight/kafkactl/v5/internal/output"
	"github.com/deviceinsight/kafkactl/v5/internal/util"
	"github.com/pkg/errors"
)

func availableReplicaBrokers(client sarama.Client) []*sarama.Broker {
	var allBrokers = client.Brokers()

	var availableBrokers []*sarama.Broker
	for _, broker := range allBrokers {
		// Brokers discovered in metadata are connected lazily. Connected alone
		// reports local connection state, not whether a broker is reachable.
		if err := broker.Open(client.Config()); err != nil && !errors.Is(err, sarama.ErrAlreadyConnected) {
			output.Warnf("unable to open broker %d (%s): %v", broker.ID(), broker.Addr(), err)
			continue
		}
		connected, err := broker.Connected()
		if err != nil {
			output.Warnf("unable to connect to broker %d (%s): %v", broker.ID(), broker.Addr(), err)
			continue
		}
		if connected {
			availableBrokers = append(availableBrokers, broker)
		} else {
			output.Warnf("broker %d (%s) is not available and will be excluded from replica assignment", broker.ID(), broker.Addr())
		}
	}

	return availableBrokers
}

func getTargetReplicas(currentReplicas []int32, brokerReplicaCount map[int32]int, targetReplicationFactor int16) ([]int32, error) {

	replicas := currentReplicas

	for len(replicas) > int(targetReplicationFactor) {

		sort.Slice(replicas, func(i, j int) bool {
			brokerI := replicas[i]
			brokerJ := replicas[j]
			_, availableI := brokerReplicaCount[brokerI]
			_, availableJ := brokerReplicaCount[brokerJ]

			// Unavailable brokers (not in brokerReplicaCount) should be sorted to the end
			// so they are removed first when decreasing replication factor
			if availableI != availableJ {
				return availableI
			}
			return brokerReplicaCount[brokerI] < brokerReplicaCount[brokerJ] || (brokerReplicaCount[brokerI] == brokerReplicaCount[brokerJ] && brokerI < brokerJ)
		})

		lastReplica := replicas[len(replicas)-1]
		replicas = replicas[:len(replicas)-1]
		if _, ok := brokerReplicaCount[lastReplica]; ok {
			brokerReplicaCount[lastReplica]--
		}
	}

	// Strip unavailable replicas so the increase phase replaces them with available brokers.
	var availableReplicas []int32
	for _, r := range replicas {
		if _, ok := brokerReplicaCount[r]; ok {
			availableReplicas = append(availableReplicas, r)
		}
	}
	replicas = availableReplicas

	var unusedBrokerIDs []int32

	if len(replicas) < int(targetReplicationFactor) {
		for brokerID := range brokerReplicaCount {
			if !util.ContainsInt32(replicas, brokerID) {
				unusedBrokerIDs = append(unusedBrokerIDs, brokerID)
			}
		}
		if len(unusedBrokerIDs) < (int(targetReplicationFactor) - len(replicas)) {
			return nil, errors.New("not enough brokers")
		}
	}

	for len(replicas) < int(targetReplicationFactor) {

		sort.Slice(unusedBrokerIDs, func(i, j int) bool {
			brokerI := unusedBrokerIDs[i]
			brokerJ := unusedBrokerIDs[j]
			return brokerReplicaCount[brokerI] < brokerReplicaCount[brokerJ] || (brokerReplicaCount[brokerI] == brokerReplicaCount[brokerJ] && brokerI > brokerJ)
		})

		replicas = append(replicas, unusedBrokerIDs[0])
		brokerReplicaCount[unusedBrokerIDs[0]]++
		unusedBrokerIDs = unusedBrokerIDs[1:]
	}

	return replicas, nil
}
