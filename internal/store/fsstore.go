package store

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/yourname/click-sink/internal/config"
	"gopkg.in/yaml.v3"
)

// FSStore is a filesystem-backed implementation using the existing .ui-data layout.
type FSStore struct {
	base string
	mu   sync.Mutex
}

func NewFSStore(base string) *FSStore {
	_ = os.MkdirAll(filepath.Join(base, "pipelines"), 0o755)
	return &FSStore{base: base}
}

func (s *FSStore) pipelinesDir() string { return filepath.Join(s.base, "pipelines") }

func (s *FSStore) ListPipelines(ctx context.Context) ([]Pipeline, error) {
	entries, err := os.ReadDir(s.pipelinesDir())
	if err != nil {
		return nil, err
	}
	out := make([]Pipeline, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		id := e.Name()
		p := Pipeline{ID: id, Name: id, CreatedAt: time.Now(), UpdatedAt: time.Now()}
		if b, err := os.ReadFile(filepath.Join(s.pipelinesDir(), id, "meta.json")); err == nil {
			var m struct {
				Name        string    `json:"name"`
				Description string    `json:"description"`
				CreatedAt   time.Time `json:"createdAt"`
				UpdatedAt   time.Time `json:"updatedAt"`
			}
			if json.Unmarshal(b, &m) == nil {
				if m.Name != "" {
					p.Name = m.Name
				}
				p.Description = m.Description
				if !m.CreatedAt.IsZero() {
					p.CreatedAt = m.CreatedAt
				}
				if !m.UpdatedAt.IsZero() {
					p.UpdatedAt = m.UpdatedAt
				}
			}
		}
		out = append(out, p)
	}
	return out, nil
}

func (s *FSStore) CreatePipeline(ctx context.Context, name, description string) (*Pipeline, error) {
	id := sanitizeID(name)
	dir := filepath.Join(s.pipelinesDir(), id)
	if _, err := os.Stat(dir); err == nil {
		id = id + "-" + time.Now().Format("20060102150405")
		dir = filepath.Join(s.pipelinesDir(), id)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	p := &Pipeline{ID: id, Name: name, Description: description, CreatedAt: time.Now(), UpdatedAt: time.Now()}
	_ = s.writeMeta(p)
	return p, nil
}

func (s *FSStore) GetPipeline(ctx context.Context, id string) (*Pipeline, error) {
	dir := filepath.Join(s.pipelinesDir(), id)
	if _, err := os.Stat(dir); err != nil {
		return nil, err
	}
	p := &Pipeline{ID: id, Name: id, CreatedAt: time.Now(), UpdatedAt: time.Now()}
	if b, err := os.ReadFile(filepath.Join(dir, "meta.json")); err == nil {
		var m Pipeline
		if json.Unmarshal(b, &m) == nil {
			p.Name = m.Name
			p.Description = m.Description
			p.CreatedAt = m.CreatedAt
			p.UpdatedAt = m.UpdatedAt
		}
	}
	return p, nil
}

func (s *FSStore) UpdatePipeline(ctx context.Context, id, name, description string) error {
	p, err := s.GetPipeline(ctx, id)
	if err != nil {
		return err
	}
	if name != "" {
		p.Name = name
	}
	p.Description = description
	p.UpdatedAt = time.Now()
	return s.writeMeta(p)
}

func (s *FSStore) DeletePipeline(ctx context.Context, id string) error {
	return os.RemoveAll(filepath.Join(s.pipelinesDir(), id))
}

func (s *FSStore) writeMeta(p *Pipeline) error {
	dir := filepath.Join(s.pipelinesDir(), p.ID)
	_ = os.MkdirAll(dir, 0o755)
	b, _ := json.MarshalIndent(struct {
		ID          string    `json:"id"`
		Name        string    `json:"name"`
		Description string    `json:"description"`
		CreatedAt   time.Time `json:"createdAt"`
		UpdatedAt   time.Time `json:"updatedAt"`
	}{p.ID, p.Name, p.Description, p.CreatedAt, p.UpdatedAt}, "", "  ")
	return os.WriteFile(filepath.Join(dir, "meta.json"), b, 0o644)
}

func (s *FSStore) GetKafkaConfig(ctx context.Context, id string) (*config.KafkaConfig, error) {
	b, err := os.ReadFile(filepath.Join(s.pipelinesDir(), id, "kafka.yaml"))
	if err != nil {
		return &config.KafkaConfig{}, nil
	}
	var k config.KafkaConfig
	_ = yaml.Unmarshal(b, &k)
	return &k, nil
}

func (s *FSStore) PutKafkaConfig(ctx context.Context, id string, cfg *config.KafkaConfig) error {
	by, _ := yaml.Marshal(cfg)
	return os.WriteFile(filepath.Join(s.pipelinesDir(), id, "kafka.yaml"), by, 0o644)
}

func (s *FSStore) GetClickHouseConfig(ctx context.Context, id string) (*config.ClickHouseConfig, error) {
	b, err := os.ReadFile(filepath.Join(s.pipelinesDir(), id, "clickhouse.yaml"))
	if err != nil {
		return &config.ClickHouseConfig{}, nil
	}
	var h config.ClickHouseConfig
	_ = yaml.Unmarshal(b, &h)
	return &h, nil
}

func (s *FSStore) PutClickHouseConfig(ctx context.Context, id string, cfg *config.ClickHouseConfig) error {
	by, _ := yaml.Marshal(cfg)
	return os.WriteFile(filepath.Join(s.pipelinesDir(), id, "clickhouse.yaml"), by, 0o644)
}

func (s *FSStore) GetMappingYAML(ctx context.Context, id string) ([]byte, error) {
	b, err := os.ReadFile(filepath.Join(s.pipelinesDir(), id, "mapping.yaml"))
	if err != nil {
		return []byte(""), nil
	}
	return b, nil
}

func (s *FSStore) PutMappingYAML(ctx context.Context, id string, y []byte) error {
	return os.WriteFile(filepath.Join(s.pipelinesDir(), id, "mapping.yaml"), y, 0o644)
}

func (s *FSStore) GetState(ctx context.Context, id string) (*PipelineState, error) {
	var st PipelineState
	b, err := os.ReadFile(filepath.Join(s.pipelinesDir(), id, "state.json"))
	if err != nil {
		// default
		st = PipelineState{PipelineID: id, Desired: DesiredStopped, Replicas: 1, UpdatedAt: time.Now()}
		return &st, nil
	}
	_ = json.Unmarshal(b, &st)
	if st.Replicas <= 0 {
		st.Replicas = 1
	}
	return &st, nil
}

func (s *FSStore) SetDesiredState(ctx context.Context, id string, desired DesiredState, replicas int) error {
	st := PipelineState{PipelineID: id, Desired: desired, Replicas: replicas, UpdatedAt: time.Now()}
	b, _ := json.MarshalIndent(st, "", "  ")
	dir := filepath.Join(s.pipelinesDir(), id)
	_ = os.MkdirAll(dir, 0o755)
	return os.WriteFile(filepath.Join(dir, "state.json"), b, 0o644)
}

// Single-replica lease emulation for FS store (best-effort, not distributed).
func (s *FSStore) TryAcquire(ctx context.Context, id, workerID string, ttl time.Duration) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	leaseFile := filepath.Join(s.pipelinesDir(), id, "lease.json")
	type lease struct {
		WorkerID   string    `json:"workerId"`
		LeaseUntil time.Time `json:"leaseUntil"`
	}
	if b, err := os.ReadFile(leaseFile); err == nil {
		var l lease
		_ = json.Unmarshal(b, &l)
		if time.Now().Before(l.LeaseUntil) {
			return false, nil
		}
	}
	l := lease{WorkerID: workerID, LeaseUntil: time.Now().Add(ttl)}
	by, _ := json.Marshal(l)
	return true, os.WriteFile(leaseFile, by, 0o644)
}

func (s *FSStore) Renew(ctx context.Context, id, workerID string, ttl time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	leaseFile := filepath.Join(s.pipelinesDir(), id, "lease.json")
	b, err := os.ReadFile(leaseFile)
	if err != nil {
		return err
	}
	var l struct {
		WorkerID   string    `json:"workerId"`
		LeaseUntil time.Time `json:"leaseUntil"`
	}
	_ = json.Unmarshal(b, &l)
	if l.WorkerID != workerID {
		return errors.New("not owner")
	}
	l.LeaseUntil = time.Now().Add(ttl)
	by, _ := json.Marshal(l)
	return os.WriteFile(leaseFile, by, 0o644)
}

func (s *FSStore) Release(ctx context.Context, id, workerID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	leaseFile := filepath.Join(s.pipelinesDir(), id, "lease.json")
	_ = os.Remove(leaseFile)
	return nil
}

// Slot-based lease helpers (best-effort, local only)
func (s *FSStore) ListAssignments(ctx context.Context, id string) ([]Assignment, error) {
	dir := filepath.Join(s.pipelinesDir(), id)
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return []Assignment{}, nil
		}
		return nil, err
	}
	out := []Assignment{}
	for _, e := range entries {
		name := e.Name()
		if !strings.HasPrefix(name, "lease-") || !strings.HasSuffix(name, ".json") {
			continue
		}
		mid := strings.TrimSuffix(strings.TrimPrefix(name, "lease-"), ".json")
		slot, err := strconv.Atoi(mid)
		if err != nil {
			continue
		}
		b, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			continue
		}
		var l struct {
			WorkerID   string    `json:"workerId"`
			LeaseUntil time.Time `json:"leaseUntil"`
		}
		if json.Unmarshal(b, &l) != nil {
			continue
		}
		if time.Now().After(l.LeaseUntil) {
			continue
		}
		out = append(out, Assignment{PipelineID: id, Slot: slot, WorkerID: l.WorkerID, LeaseUntil: l.LeaseUntil})
	}
	return out, nil
}

func (s *FSStore) TryAcquireSlot(ctx context.Context, id, workerID string, ttl time.Duration) (int, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// determine desired replicas to bound slot search
	st, _ := s.GetState(ctx, id)
	replicas := 1
	if st != nil && st.Replicas > 0 {
		replicas = st.Replicas
	}
	if replicas <= 0 {
		replicas = 1
	}
	for slot := 0; slot < replicas; slot++ {
		p := filepath.Join(s.pipelinesDir(), id, "lease-"+strconv.Itoa(slot)+".json")
		var cur struct {
			WorkerID   string    `json:"workerId"`
			LeaseUntil time.Time `json:"leaseUntil"`
		}
		if b, err := os.ReadFile(p); err == nil {
			_ = json.Unmarshal(b, &cur)
			if time.Now().Before(cur.LeaseUntil) {
				continue // occupied
			}
		}
		cur.WorkerID = workerID
		cur.LeaseUntil = time.Now().Add(ttl)
		by, _ := json.Marshal(cur)
		if err := os.WriteFile(p, by, 0o644); err != nil {
			return -1, false, err
		}
		return slot, true, nil
	}
	return -1, false, nil
}

func (s *FSStore) RenewSlots(ctx context.Context, id, workerID string, slots []int, ttl time.Duration) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, slot := range slots {
		p := filepath.Join(s.pipelinesDir(), id, "lease-"+strconv.Itoa(slot)+".json")
		b, err := os.ReadFile(p)
		if err != nil {
			continue
		}
		var cur struct {
			WorkerID   string    `json:"workerId"`
			LeaseUntil time.Time `json:"leaseUntil"`
		}
		if json.Unmarshal(b, &cur) != nil {
			continue
		}
		if cur.WorkerID != workerID {
			continue
		}
		cur.LeaseUntil = time.Now().Add(ttl)
		by, _ := json.Marshal(cur)
		_ = os.WriteFile(p, by, 0o644)
	}
	return nil
}

func (s *FSStore) ReleaseSlot(ctx context.Context, id, workerID string, slot int) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	p := filepath.Join(s.pipelinesDir(), id, "lease-"+strconv.Itoa(slot)+".json")
	// optional: check owner
	_ = os.Remove(p)
	return nil
}

// Worker registry (best-effort no-op for FS store)
func (s *FSStore) UpsertWorkerHeartbeat(ctx context.Context, workerID, mode, version string) error {
	return nil
}

func (s *FSStore) ListWorkers(ctx context.Context) ([]Worker, error) {
	return []Worker{}, nil
}

// Claims (no-op for FS store)
func (s *FSStore) UpsertClaim(ctx context.Context, pipelineID, workerID, topic string, partition int) error {
	return nil
}
func (s *FSStore) RemoveClaim(ctx context.Context, pipelineID, workerID, topic string, partition int) error {
	return nil
}
func (s *FSStore) ListClaims(ctx context.Context, pipelineID string) ([]Claim, error) {
	return []Claim{}, nil
}

// Helpers
func sanitizeID(s string) string {
	if s == "" {
		return "pipeline"
	}
	out := make([]rune, 0, len(s))
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' {
			out = append(out, r)
		} else if r == ' ' {
			out = append(out, '-')
		}
	}
	if len(out) == 0 {
		return "pipeline"
	}
	return string(out)
}

// no-op
