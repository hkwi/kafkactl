package topic

import (
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/IBM/sarama"
)

type replicaProbeClient struct {
	sarama.Client
	brokers []*sarama.Broker
	config  *sarama.Config
}

func (c *replicaProbeClient) Brokers() []*sarama.Broker { return c.brokers }
func (c *replicaProbeClient) Config() *sarama.Config    { return c.config }

type blockedReplicaDialer struct {
	started chan struct{}
	release chan struct{}
}

func (d *blockedReplicaDialer) Dial(string, string) (net.Conn, error) {
	d.started <- struct{}{}
	<-d.release
	return nil, errors.New("synthetic dial failure")
}

func TestAvailableReplicaBrokersStartsAllDialsBeforeWaiting(t *testing.T) {
	captureTopicWarnings(t)
	dialer := &blockedReplicaDialer{started: make(chan struct{}, 3), release: make(chan struct{})}
	var once sync.Once
	release := func() { once.Do(func() { close(dialer.release) }) }
	config := sarama.NewConfig()
	config.ApiVersionsRequest = false
	config.Net.Proxy.Enable = true
	config.Net.Proxy.Dialer = dialer
	client := &replicaProbeClient{config: config, brokers: []*sarama.Broker{
		sarama.NewBroker("broker1.example:9092"),
		sarama.NewBroker("broker2.example:9092"),
		sarama.NewBroker("broker3.example:9092"),
	}}
	done := make(chan []*sarama.Broker, 1)
	go func() { done <- availableReplicaBrokers(client) }()
	t.Cleanup(func() {
		release()
		for _, broker := range client.brokers {
			_ = broker.Close()
		}
	})
	for range client.brokers {
		select {
		case <-dialer.started:
		case <-time.After(time.Second):
			release()
			<-done
			t.Fatal("a pending broker dial blocked other connection attempts")
		}
	}
	release()
	if brokers := <-done; len(brokers) != 0 {
		t.Fatalf("failed dials were treated as reachable: %v", brokers)
	}
}
