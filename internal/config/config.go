package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

type KafkaConfig struct {
	Brokers       []string `yaml:"brokers"`
	Topic         string   `yaml:"topic"`
	GroupID       string   `yaml:"groupId"`
	SASLUsername  string   `yaml:"saslUsername"`
	SASLPassword  string   `yaml:"saslPassword"`
	SASLMechanism string   `yaml:"saslMechanism"`
	// SecurityProtocol: one of PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL
	SecurityProtocol string `yaml:"securityProtocol"`

	// TLS/mTLS options (used when SecurityProtocol is SSL or SASL_SSL). PEM-encoded strings.
	TLSCA                 string `yaml:"tlsCA"`
	TLSCert               string `yaml:"tlsCert"`
	TLSKey                string `yaml:"tlsKey"`
	TLSInsecureSkipVerify bool   `yaml:"tlsInsecureSkipVerify"`
	TLSServerName         string `yaml:"tlsServerName"`

	// GSSAPI (Kerberos) options (used when SASLMechanism == GSSAPI)
	GSSAPIEnabled            bool   `yaml:"gssapiEnabled"`
	GSSAPIRealm              string `yaml:"gssapiRealm"`
	GSSAPIServiceName        string `yaml:"gssapiServiceName"` // usually "kafka"
	GSSAPIAuthType           string `yaml:"gssapiAuthType"`    // USER or KEYTAB
	GSSAPIUsername           string `yaml:"gssapiUsername"`
	GSSAPIPassword           string `yaml:"gssapiPassword"`
	GSSAPIKeytabPath         string `yaml:"gssapiKeytabPath"`
	GSSAPIKerberosConfigPath string `yaml:"gssapiKerberosConfigPath"`
	GSSAPIDisablePAFXFAST    bool   `yaml:"gssapiDisablePAFXFAST"`
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
	Filters    FilterConfig     `yaml:"filters"`
}

// FilterConfig defines message filtering behavior before mapping/insertion.
// If Enabled is true, an expression is evaluated per message. When it yields true,
// the message is kept; when false, the message is dropped.
// Language currently supports "CEL" (Common Expression Language). The expression
// evaluates against a variable named "flat" which is a map<string, dyn> of flattened
// JSON fields (e.g., flat["user.id"], flat["event.type"]). Use string()/int()/double()
// casts and the built-in RE2 regex function matches() for regex filtering.
type FilterConfig struct {
	Enabled    bool   `yaml:"enabled" json:"enabled"`
	Language   string `yaml:"language" json:"language"`     // CEL (default)
	Expression string `yaml:"expression" json:"expression"` // e.g., flat["type"] == "purchase" && string(flat["user.email"]).matches("@example.com$")
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
