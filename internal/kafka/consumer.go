package kafka

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/IBM/sarama"
	"github.com/yourname/click-sink/internal/config"
)

type Message struct {
	Value     []byte
	Timestamp time.Time
	Ack       func()
}

type Consumer struct {
	group sarama.ConsumerGroup
	topic string
	name  string
}

func NewConsumer(cfg *config.KafkaConfig, clientID string) (*Consumer, error) {
	return NewConsumerWithInitial(cfg, clientID, sarama.OffsetNewest)
}

func NewConsumerWithInitial(cfg *config.KafkaConfig, clientID string, initialOffset int64) (*Consumer, error) {
	config := sarama.NewConfig()
	config.Version = sarama.V3_5_0_0
	config.ClientID = clientID
	config.Consumer.Return.Errors = true
	config.Consumer.Offsets.Initial = initialOffset
	config.Metadata.Full = true
	// Prefer round-robin assignment for more even distribution across members
	config.Consumer.Group.Rebalance.Strategy = sarama.NewBalanceStrategyRoundRobin()

	// Security
	switch cfg.SecurityProtocol {
	case "SASL_SSL", "SASL_PLAINTEXT":
		config.Net.SASL.Enable = true
		config.Net.SASL.User = cfg.SASLUsername
		config.Net.SASL.Password = cfg.SASLPassword
		switch cfg.SASLMechanism {
		case "PLAIN":
			config.Net.SASL.Mechanism = sarama.SASLTypePlaintext
		case "SCRAM-SHA-256":
			config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
		case "SCRAM-SHA-512":
			config.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
		default:
			// default PLAIN if provided
			config.Net.SASL.Mechanism = sarama.SASLTypePlaintext
		}
		if cfg.SecurityProtocol == "SASL_SSL" {
			config.Net.TLS.Enable = true
		}
	case "PLAINTEXT", "":
		// nothing
	default:
		return nil, fmt.Errorf("unsupported securityProtocol: %s", cfg.SecurityProtocol)
	}

	group, err := sarama.NewConsumerGroup(cfg.Brokers, cfg.GroupID, config)
	if err != nil {
		return nil, err
	}
	return &Consumer{group: group, topic: cfg.Topic, name: clientID}, nil
}

func (c *Consumer) Close() error { return c.group.Close() }

// Consume starts consuming and returns a channel of messages and a stop function.
func (c *Consumer) Consume(ctx context.Context) (<-chan Message, <-chan error) {
	out := make(chan Message, 1000)
	errCh := make(chan error, 1)

	h := &handler{topic: c.topic, out: out, name: c.name}
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
	topic string
	out   chan<- Message
	name  string
}

func (h *handler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *handler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h *handler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	if os.Getenv("KAFKA_DEBUG") != "" {
		log.Printf("kafka[%s]: consuming topic=%s partition=%d from offset=%d", h.name, claim.Topic(), claim.Partition(), claim.InitialOffset())
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
