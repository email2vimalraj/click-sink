package kafka

import (
    "fmt"

    "github.com/IBM/sarama"
)

// NewSyncProducer creates a synchronous Kafka producer.
func NewSyncProducer(brokers []string, clientID string) (sarama.SyncProducer, error) {
    cfg := sarama.NewConfig()
    cfg.Version = sarama.V3_5_0_0
    cfg.ClientID = clientID
    cfg.Producer.Return.Successes = true
    cfg.Producer.Idempotent = false
    p, err := sarama.NewSyncProducer(brokers, cfg)
    if err != nil {
        return nil, fmt.Errorf("new producer: %w", err)
    }
    return p, nil
}
