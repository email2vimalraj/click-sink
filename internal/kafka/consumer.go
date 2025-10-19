package kafka

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/yourname/click-sink/internal/config"
)

type Message struct {
	Value     []byte
	Timestamp time.Time
	Ack       func()
}

// ClaimObserver is notified when partitions are assigned/revoked for this consumer.
type ClaimObserver interface {
	OnPartitionAssigned(groupID, clientID, topic string, partition int32)
	OnPartitionReleased(groupID, clientID, topic string, partition int32)
}

type Consumer struct {
	group   sarama.ConsumerGroup
	topic   string
	name    string
	groupID string
	obs     ClaimObserver
}

func NewConsumer(cfg *config.KafkaConfig, clientID string) (*Consumer, error) {
	return NewConsumerWithObserver(cfg, clientID, sarama.OffsetNewest, nil)
}

func NewConsumerWithInitial(cfg *config.KafkaConfig, clientID string, initialOffset int64) (*Consumer, error) {
	return NewConsumerWithObserver(cfg, clientID, initialOffset, nil)
}

func NewConsumerWithObserver(cfg *config.KafkaConfig, clientID string, initialOffset int64, obs ClaimObserver) (*Consumer, error) {
	sc := sarama.NewConfig()
	sc.Version = sarama.V3_5_0_0
	sc.ClientID = clientID
	sc.Consumer.Return.Errors = true
	sc.Consumer.Offsets.Initial = initialOffset
	sc.Metadata.Full = true
	// Prefer round-robin assignment for more even distribution across members
	sc.Consumer.Group.Rebalance.Strategy = sarama.NewBalanceStrategyRoundRobin()

	// Security
	switch cfg.SecurityProtocol {
	case "SASL_SSL", "SASL_PLAINTEXT":
		sc.Net.SASL.Enable = true
		sc.Net.SASL.User = cfg.SASLUsername
		sc.Net.SASL.Password = cfg.SASLPassword
		switch cfg.SASLMechanism {
		case "PLAIN":
			sc.Net.SASL.Mechanism = sarama.SASLTypePlaintext
		case "SCRAM-SHA-256":
			sc.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
		case "SCRAM-SHA-512":
			sc.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
		default:
			// default PLAIN if provided
			sc.Net.SASL.Mechanism = sarama.SASLTypePlaintext
		}
		if cfg.SecurityProtocol == "SASL_SSL" {
			sc.Net.TLS.Enable = true
		}
	case "PLAINTEXT", "":
		// nothing
	default:
		return nil, fmt.Errorf("unsupported securityProtocol: %s", cfg.SecurityProtocol)
	}

	group, err := sarama.NewConsumerGroup(cfg.Brokers, cfg.GroupID, sc)
	if err != nil {
		return nil, err
	}
	return &Consumer{group: group, topic: cfg.Topic, name: clientID, groupID: cfg.GroupID, obs: obs}, nil
}

func (c *Consumer) Close() error { return c.group.Close() }

// Consume starts consuming and returns a channel of messages and a stop function.
func (c *Consumer) Consume(ctx context.Context) (<-chan Message, <-chan error) {
	out := make(chan Message, 1000)
	errCh := make(chan error, 1)

	h := &handler{topic: c.topic, out: out, name: c.name, groupID: c.groupID, obs: c.obs}
	go func() {
		defer close(out)
		for {
			if ctx.Err() != nil {
				return
			}
			if err := c.group.Consume(ctx, []string{c.topic}, h); err != nil {
				errCh <- err
				return
			}
		}
	}()
	return out, errCh
}

type handler struct {
	topic   string
	out     chan<- Message
	name    string
	groupID string
	obs     ClaimObserver
}

func (h *handler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *handler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h *handler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	if os.Getenv("KAFKA_DEBUG") != "" {
		log.Printf("kafka[%s]: consuming topic=%s partition=%d from offset=%d", h.name, claim.Topic(), claim.Partition(), claim.InitialOffset())
	}
	if h.obs != nil {
		h.obs.OnPartitionAssigned(h.groupID, h.name, claim.Topic(), claim.Partition())
		defer h.obs.OnPartitionReleased(h.groupID, h.name, claim.Topic(), claim.Partition())
	}
	for msg := range claim.Messages() {
		m := msg
		h.out <- Message{
			Value:     m.Value,
			Timestamp: m.Timestamp,
			Ack: func() {
				sess.MarkMessage(m, "")
			},
		}
	}
	return nil
}

// Sample reads up to n messages with a temporary consumer group and returns the payloads.
func Sample(ctx context.Context, cfg *config.KafkaConfig, n int) ([][]byte, error) {
	if n <= 0 {
		n = 100
	}
	tempCfg := *cfg
	tempCfg.GroupID = fmt.Sprintf("%s-detect-%d", cfg.GroupID, time.Now().UnixNano())
	c, err := NewConsumerWithInitial(&tempCfg, "click-sink-detect", sarama.OffsetOldest)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	msgsCh, errCh := c.Consume(ctx)
	payloads := make([][]byte, 0, n)
	for {
		select {
		case <-ctx.Done():
			return payloads, ctx.Err()
		case err := <-errCh:
			return payloads, err
		case m, ok := <-msgsCh:
			if !ok {
				return payloads, nil
			}
			payloads = append(payloads, m.Value)
			m.Ack()
			if len(payloads) >= n {
				return payloads, nil
			}
		}
	}
}

// SampleDirect consumes without using a consumer group, starting at the oldest offset
// on all partitions, and returns up to n messages (or until ctx timeout/cancel).
func SampleDirect(ctx context.Context, cfg *config.KafkaConfig, n int) ([][]byte, error) {
	if n <= 0 {
		n = 100
	}
	// Build a client config similar to ValidateConnectivity
	sc := sarama.NewConfig()
	sc.Version = sarama.V3_5_0_0
	// Security
	switch cfg.SecurityProtocol {
	case "SASL_SSL", "SASL_PLAINTEXT":
		sc.Net.SASL.Enable = true
		sc.Net.SASL.User = cfg.SASLUsername
		sc.Net.SASL.Password = cfg.SASLPassword
		switch cfg.SASLMechanism {
		case "PLAIN":
			sc.Net.SASL.Mechanism = sarama.SASLTypePlaintext
		case "SCRAM-SHA-256":
			sc.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
		case "SCRAM-SHA-512":
			sc.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
		default:
			sc.Net.SASL.Mechanism = sarama.SASLTypePlaintext
		}
		if cfg.SecurityProtocol == "SASL_SSL" {
			sc.Net.TLS.Enable = true
		}
	case "PLAINTEXT", "":
		// nothing
	default:
		return nil, fmt.Errorf("unsupported securityProtocol: %s", cfg.SecurityProtocol)
	}

	client, err := sarama.NewClient(cfg.Brokers, sc)
	if err != nil {
		return nil, err
	}
	defer client.Close()

	partitions, err := client.Partitions(cfg.Topic)
	if err != nil {
		return nil, err
	}
	if len(partitions) == 0 {
		return nil, fmt.Errorf("no partitions for topic %s", cfg.Topic)
	}

	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		return nil, err
	}
	defer consumer.Close()

	type msg struct{ val []byte }
	out := make(chan msg, n)
	var wg sync.WaitGroup

	// Start a partition consumer for each partition
	for _, p := range partitions {
		pc, err := consumer.ConsumePartition(cfg.Topic, p, sarama.OffsetOldest)
		if err != nil {
			// If one partition fails, continue others — we'll still get data if available
			if os.Getenv("KAFKA_DEBUG") != "" {
				log.Printf("kafka: direct sample failed to open partition %d: %v", p, err)
			}
			continue
		}
		wg.Add(1)
		go func(pc sarama.PartitionConsumer) {
			defer wg.Done()
			defer pc.Close()
			for {
				select {
				case <-ctx.Done():
					return
				case m, ok := <-pc.Messages():
					if !ok {
						return
					}
					select {
					case out <- msg{val: m.Value}:
					case <-ctx.Done():
						return
					}
				case err, ok := <-pc.Errors():
					if ok && os.Getenv("KAFKA_DEBUG") != "" {
						log.Printf("kafka: direct sample partition error: %v", err)
					}
					// Continue on errors to allow other partitions to proceed
				}
			}
		}(pc)
	}

	// Close out channel when all partitions finish
	go func() {
		wg.Wait()
		close(out)
	}()

	payloads := make([][]byte, 0, n)
	for {
		select {
		case <-ctx.Done():
			return payloads, ctx.Err()
		case m, ok := <-out:
			if !ok {
				return payloads, nil
			}
			payloads = append(payloads, m.val)
			if len(payloads) >= n {
				return payloads, nil
			}
		}
	}
}

// SamplePreferGroupThenDirect tries consumer-group sampling first. If it times out with
// zero messages, it falls back to direct partition consumption. This improves resilience
// in environments where group assignment may be delayed.
func SamplePreferGroupThenDirect(ctx context.Context, cfg *config.KafkaConfig, n int) ([][]byte, error) {
	// Try group-based sampling with a short sub-timeout
	gctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	payloads, err := Sample(gctx, cfg, n)
	// If we collected any messages, treat as success even if the short timeout fired
	if len(payloads) > 0 {
		return payloads, nil
	}
	// Propagate non-timeout errors
	if err != nil && err != context.DeadlineExceeded && err != context.Canceled {
		return nil, err
	}
	// Fallback: direct sample for the remaining time
	if os.Getenv("KAFKA_DEBUG") != "" {
		log.Printf("kafka: falling back to direct sampling (group err=%v, got=%d)", err, len(payloads))
	}
	dPayloads, dErr := SampleDirect(ctx, cfg, n)
	if len(dPayloads) > 0 {
		return dPayloads, nil
	}
	return dPayloads, dErr
}

// ValidateConnectivity attempts to connect to brokers and fetch metadata for the topic.
func ValidateConnectivity(cfg *config.KafkaConfig) error {
	sc := sarama.NewConfig()
	sc.Version = sarama.V3_5_0_0
	sc.ClientID = "click-sink-validate"
	// Security
	switch cfg.SecurityProtocol {
	case "SASL_SSL", "SASL_PLAINTEXT":
		sc.Net.SASL.Enable = true
		sc.Net.SASL.User = cfg.SASLUsername
		sc.Net.SASL.Password = cfg.SASLPassword
		switch cfg.SASLMechanism {
		case "PLAIN":
			sc.Net.SASL.Mechanism = sarama.SASLTypePlaintext
		case "SCRAM-SHA-256":
			sc.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
		case "SCRAM-SHA-512":
			sc.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
		default:
			sc.Net.SASL.Mechanism = sarama.SASLTypePlaintext
		}
		if cfg.SecurityProtocol == "SASL_SSL" {
			sc.Net.TLS.Enable = true
		}
	case "PLAINTEXT", "":
		// nothing
	default:
		return fmt.Errorf("unsupported securityProtocol: %s", cfg.SecurityProtocol)
	}
	client, err := sarama.NewClient(cfg.Brokers, sc)
	if err != nil {
		return err
	}
	defer client.Close()
	// Try getting partitions for the topic, which will confirm metadata
	_, err = client.Partitions(cfg.Topic)
	return err
}
