package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type KafkaConfig struct {
	Brokers          []string `yaml:"brokers"`
	Topic            string   `yaml:"topic"`
	GroupID          string   `yaml:"groupId"`
	SASLUsername     string   `yaml:"saslUsername"`
	SASLPassword     string   `yaml:"saslPassword"`
	SASLMechanism    string   `yaml:"saslMechanism"`
	SecurityProtocol string   `yaml:"securityProtocol"` // PLAINTEXT or SASL_SSL
}

type ClickHouseConfig struct {
	DSN                  string `yaml:"dsn"`
	Database             string `yaml:"database"`
	Table                string `yaml:"table"`
	BatchSize            int    `yaml:"batchSize"`
	BatchFlushInterval   string `yaml:"batchFlushInterval"`
	InsertRatePerSec     int    `yaml:"insertRatePerSecond"`
	CreateTableIfMissing bool   `yaml:"createTableIfMissing"`
}

type Config struct {
	Kafka      KafkaConfig      `yaml:"kafka"`
	ClickHouse ClickHouseConfig `yaml:"clickhouse"`
}

func Load(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var c Config
	if err := yaml.Unmarshal(b, &c); err != nil {
		return nil, err
	}
	// Defaults
	if c.ClickHouse.BatchSize <= 0 {
		c.ClickHouse.BatchSize = 500
	}
	if c.ClickHouse.InsertRatePerSec < 0 {
		c.ClickHouse.InsertRatePerSec = 0
	}
	if c.ClickHouse.BatchFlushInterval == "" {
		c.ClickHouse.BatchFlushInterval = "2s"
	}
	if c.Kafka.Topic == "" || len(c.Kafka.Brokers) == 0 {
		return nil, fmt.Errorf("kafka.brokers and kafka.topic are required")
	}
	if c.ClickHouse.DSN == "" || c.ClickHouse.Table == "" {
		return nil, fmt.Errorf("clickhouse.dsn and clickhouse.table are required")
	}
	return &c, nil
}
