package store

import (
	"context"
	"time"

	"github.com/yourname/click-sink/internal/config"
)

// DesiredState is the intent for a pipeline.
type DesiredState string

const (
	DesiredStopped DesiredState = "stopped"
	DesiredStarted DesiredState = "started"
)

type Pipeline struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Description string    `json:"description"`
	CreatedAt   time.Time `json:"createdAt"`
	UpdatedAt   time.Time `json:"updatedAt"`
}

type PipelineSummary struct {
	Pipeline
	// Aggregated fields for UI
	Running   bool   `json:"running"`
	LastErr   string `json:"lastErr"`
	TotalRows int64  `json:"totalRows"`
}

// PipelineState represents desired state and optional replicas.
type PipelineState struct {
	PipelineID string       `json:"pipelineId"`
	Desired    DesiredState `json:"desired"`
	Replicas   int          `json:"replicas"`
	UpdatedAt  time.Time    `json:"updatedAt"`
}

// Assignment represents a running instance lease of a pipeline.
type Assignment struct {
	PipelineID string    `json:"pipelineId"`
	Slot       int       `json:"slot"`
	Replica    int       `json:"replica"`
	WorkerID   string    `json:"workerId"`
	LeaseUntil time.Time `json:"leaseUntil"`
	Heartbeat  time.Time `json:"heartbeat"`
}

// Worker describes a registered worker process.
type Worker struct {
	WorkerID string    `json:"workerId"`
	Mode     string    `json:"mode"` // "leases" or "no-leases"
	Version  string    `json:"version"`
	LastSeen time.Time `json:"lastSeen"`
}

// Claim represents a consumer group partition ownership observed at runtime.
type Claim struct {
	PipelineID string    `json:"pipelineId"`
	WorkerID   string    `json:"workerId"`
	Topic      string    `json:"topic"`
	Partition  int       `json:"partition"`
	LastSeen   time.Time `json:"lastSeen"`
}

// PipelineStore abstracts persistence and basic coordination.
type PipelineStore interface {
	// Pipeline CRUD
	ListPipelines(ctx context.Context) ([]Pipeline, error)
	CreatePipeline(ctx context.Context, name, description string) (*Pipeline, error)
	GetPipeline(ctx context.Context, id string) (*Pipeline, error)
	UpdatePipeline(ctx context.Context, id, name, description string) error
	DeletePipeline(ctx context.Context, id string) error

	// Configs
	GetKafkaConfig(ctx context.Context, id string) (*config.KafkaConfig, error)
	PutKafkaConfig(ctx context.Context, id string, cfg *config.KafkaConfig) error
	GetClickHouseConfig(ctx context.Context, id string) (*config.ClickHouseConfig, error)
	PutClickHouseConfig(ctx context.Context, id string, cfg *config.ClickHouseConfig) error
	GetMappingYAML(ctx context.Context, id string) ([]byte, error)
	PutMappingYAML(ctx context.Context, id string, y []byte) error

	// Filters
	GetFilterConfig(ctx context.Context, id string) (*config.FilterConfig, error)
	PutFilterConfig(ctx context.Context, id string, cfg *config.FilterConfig) error

	// Desired state
	GetState(ctx context.Context, id string) (*PipelineState, error)
	SetDesiredState(ctx context.Context, id string, desired DesiredState, replicas int) error

	// Coordination: slot-based lease helpers for multi-replica
	ListAssignments(ctx context.Context, id string) ([]Assignment, error)
	TryAcquireSlot(ctx context.Context, id, workerID string, ttl time.Duration) (slot int, ok bool, err error)
	RenewSlots(ctx context.Context, id, workerID string, slots []int, ttl time.Duration) error
	ReleaseSlot(ctx context.Context, id, workerID string, slot int) error

	// Worker registry (optional for FS store), used by UI to display cluster view
	UpsertWorkerHeartbeat(ctx context.Context, workerID, mode, version string) error
	ListWorkers(ctx context.Context) ([]Worker, error)

	// Runtime claims (optional for FS store)
	UpsertClaim(ctx context.Context, pipelineID, workerID, topic string, partition int) error
	RemoveClaim(ctx context.Context, pipelineID, workerID, topic string, partition int) error
	ListClaims(ctx context.Context, pipelineID string) ([]Claim, error)
}
