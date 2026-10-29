package topic

import (
	"sort"

	"github.com/IBM/sarama"
	"github.com/deviceinsight/kafkactl/v5/internal/output"
	"golang.org/x/sync/errgroup"
)

const maxConcurrentTopicReads = 8

func readTopics(client *sarama.Client, admin *sarama.ClusterAdmin, topics []string, requestedFields requestedTopicFields) []Topic {
	var reads errgroup.Group
	reads.SetLimit(maxConcurrentTopicReads)
	results := make([]*Topic, len(topics))
	metadataFields := requestedFields
	metadataFields.config = NoConfigs

	for i, name := range topics {
		reads.Go(func() error {
			t, err := readTopic(client, admin, name, metadataFields)
			if err != nil {
				output.Warnf("skipping topic %q: %v", name, err)
				return nil
			}
			// Config permissions must not hide usable partition/replica metadata.
			if t.Configs, err = readTopicConfigs(admin, name, requestedFields.config); err != nil {
				output.Warnf("unable to read configs for topic %q: %v", name, err)
			}
			results[i] = &t
			return nil
		})
	}
	_ = reads.Wait()

	topicList := make([]Topic, 0, len(topics))
	for _, result := range results {
		if result != nil {
			topicList = append(topicList, *result)
		}
	}
	sort.Slice(topicList, func(i, j int) bool { return topicList[i].Name < topicList[j].Name })
	return topicList
}
