package alter_test

import (
	"testing"

	"github.com/deviceinsight/kafkactl/v5/internal/testutil"
	"github.com/deviceinsight/kafkactl/v5/internal/topic"
)

func TestAlterTopicPartitionsAndReplicationFactorIntegration(t *testing.T) {
	testutil.StartIntegrationTest(t)
	name := testutil.CreateTopic(t, "example-combined", "--partitions", "3", "--replication-factor", "1")
	cli := testutil.CreateKafkaCtlCommand()
	if _, err := cli.Execute("alter", "topic", name, "--partitions", "6", "--replication-factor", "3"); err != nil {
		t.Fatal(err)
	}
	if _, err := cli.Execute("describe", "topic", name, "-o", "yaml"); err != nil {
		t.Fatal(err)
	}
	described, err := topic.FromYaml(cli.GetStdOut())
	if err != nil {
		t.Fatal(err)
	}
	if len(described.Partitions) != 6 {
		t.Fatalf("got %d partitions, want 6", len(described.Partitions))
	}
	for _, partition := range described.Partitions {
		if len(partition.Replicas) != 3 {
			t.Errorf("partition %d has RF=%d, want 3", partition.ID, len(partition.Replicas))
		}
	}
}
