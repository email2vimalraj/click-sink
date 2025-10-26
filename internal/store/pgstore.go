package store

import (
	"context"
	"database/sql"
	"errors"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/yourname/click-sink/internal/config"
	"gopkg.in/yaml.v3"
)

// PGStore implements PipelineStore using PostgreSQL for persistence and coordination.
type PGStore struct {
	db *sql.DB
}

func NewPGStore(connString string) (*PGStore, error) {
	db, err := sql.Open("pgx", connString)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		return nil, err
	}
	s := &PGStore{db: db}
	if err := s.migrate(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *PGStore) migrate() error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS pipelines (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            description TEXT NOT NULL DEFAULT '',
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )`,
		`CREATE TABLE IF NOT EXISTS kafka_configs (
            pipeline_id TEXT PRIMARY KEY REFERENCES pipelines(id) ON DELETE CASCADE,
            yaml TEXT NOT NULL
        )`,
		`CREATE TABLE IF NOT EXISTS clickhouse_configs (
            pipeline_id TEXT PRIMARY KEY REFERENCES pipelines(id) ON DELETE CASCADE,
            yaml TEXT NOT NULL
        )`,
		`CREATE TABLE IF NOT EXISTS mappings (
            pipeline_id TEXT PRIMARY KEY REFERENCES pipelines(id) ON DELETE CASCADE,
            yaml TEXT NOT NULL
        )`,
		`CREATE TABLE IF NOT EXISTS filter_configs (
			pipeline_id TEXT PRIMARY KEY REFERENCES pipelines(id) ON DELETE CASCADE,
			yaml TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS pipeline_state (
            pipeline_id TEXT PRIMARY KEY REFERENCES pipelines(id) ON DELETE CASCADE,
            desired TEXT NOT NULL DEFAULT 'stopped',
            replicas INTEGER NOT NULL DEFAULT 1,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )`,
		// Leases now support multiple slots per pipeline for replicas. We attempt
		// to create the new shape and also alter if an older single-lease schema exists.
		`CREATE TABLE IF NOT EXISTS leases (
			pipeline_id TEXT NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
			slot INTEGER NOT NULL,
			worker_id TEXT NOT NULL,
			lease_until TIMESTAMPTZ NOT NULL,
			PRIMARY KEY(pipeline_id, slot)
		)`,
		`CREATE TABLE IF NOT EXISTS workers (
            worker_id TEXT PRIMARY KEY,
            mode TEXT NOT NULL,
            version TEXT NOT NULL,
            last_seen TIMESTAMPTZ NOT NULL DEFAULT now()
        )`,
		`CREATE TABLE IF NOT EXISTS claims (
			pipeline_id TEXT NOT NULL REFERENCES pipelines(id) ON DELETE CASCADE,
			worker_id TEXT NOT NULL,
			topic TEXT NOT NULL,
			partition INTEGER NOT NULL,
			last_seen TIMESTAMPTZ NOT NULL DEFAULT now(),
			PRIMARY KEY(pipeline_id, worker_id, topic, partition)
		)`,
	}
	for _, st := range stmts {
		if _, err := s.db.Exec(st); err != nil {
			return err
		}
	}
	// Best-effort migration from old single-lease schema (if present)
	// 1) Ensure columns exist
	_ = tryExec(s.db, `ALTER TABLE leases ADD COLUMN IF NOT EXISTS slot INTEGER NOT NULL DEFAULT 0`)
	_ = tryExec(s.db, `ALTER TABLE leases ADD COLUMN IF NOT EXISTS worker_id TEXT NOT NULL`)
	_ = tryExec(s.db, `ALTER TABLE leases ADD COLUMN IF NOT EXISTS lease_until TIMESTAMPTZ NOT NULL DEFAULT now()`)
	// 2) Ensure primary key on (pipeline_id, slot)
	_ = tryExec(s.db, `ALTER TABLE leases DROP CONSTRAINT IF EXISTS leases_pkey`)
	_ = tryExec(s.db, `ALTER TABLE leases ADD PRIMARY KEY(pipeline_id, slot)`)
	_ = tryExec(s.db, `ALTER TABLE leases ALTER COLUMN slot DROP DEFAULT`)
	return nil
}

func tryExec(db *sql.DB, q string) error { _, err := db.Exec(q); return err }

// Pipeline CRUD
func (s *PGStore) ListPipelines(ctx context.Context) ([]Pipeline, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, name, description, created_at, updated_at FROM pipelines ORDER BY created_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Pipeline{}
	for rows.Next() {
		var p Pipeline
		if err := rows.Scan(&p.ID, &p.Name, &p.Description, &p.CreatedAt, &p.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

func (s *PGStore) CreatePipeline(ctx context.Context, name, description string) (*Pipeline, error) {
	id := sanitizeID(name)
	if id == "pipeline" {
		id = id + "-" + time.Now().Format("20060102150405")
	}
	now := time.Now()
	_, err := s.db.ExecContext(ctx, `INSERT INTO pipelines(id,name,description,created_at,updated_at) VALUES($1,$2,$3,$4,$5)`, id, name, description, now, now)
	if err != nil {
		return nil, err
	}
	return &Pipeline{ID: id, Name: name, Description: description, CreatedAt: now, UpdatedAt: now}, nil
}

func (s *PGStore) GetPipeline(ctx context.Context, id string) (*Pipeline, error) {
	var p Pipeline
	err := s.db.QueryRowContext(ctx, `SELECT id,name,description,created_at,updated_at FROM pipelines WHERE id=$1`, id).Scan(&p.ID, &p.Name, &p.Description, &p.CreatedAt, &p.UpdatedAt)
	if err != nil {
		return nil, err
	}
	return &p, nil
}

func (s *PGStore) UpdatePipeline(ctx context.Context, id, name, description string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE pipelines SET name=COALESCE(NULLIF($2,''),name), description=$3, updated_at=now() WHERE id=$1`, id, name, description)
	return err
}

func (s *PGStore) DeletePipeline(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM pipelines WHERE id=$1`, id)
	return err
}

// Configs stored as YAML text
func (s *PGStore) GetKafkaConfig(ctx context.Context, id string) (*config.KafkaConfig, error) {
	var y string
	err := s.db.QueryRowContext(ctx, `SELECT yaml FROM kafka_configs WHERE pipeline_id=$1`, id).Scan(&y)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return &config.KafkaConfig{}, nil
		}
		return nil, err
	}
	var kc config.KafkaConfig
	if err := yaml.Unmarshal([]byte(y), &kc); err != nil {
		return nil, err
	}
	return &kc, nil
}

func (s *PGStore) PutKafkaConfig(ctx context.Context, id string, cfg *config.KafkaConfig) error {
	by, err := yaml.Marshal(cfg)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO kafka_configs(pipeline_id,yaml) VALUES($1,$2)
        ON CONFLICT(pipeline_id) DO UPDATE SET yaml=EXCLUDED.yaml`, id, string(by))
	return err
}

func (s *PGStore) GetClickHouseConfig(ctx context.Context, id string) (*config.ClickHouseConfig, error) {
	var y string
	err := s.db.QueryRowContext(ctx, `SELECT yaml FROM clickhouse_configs WHERE pipeline_id=$1`, id).Scan(&y)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return &config.ClickHouseConfig{}, nil
		}
		return nil, err
	}
	var hc config.ClickHouseConfig
	if err := yaml.Unmarshal([]byte(y), &hc); err != nil {
		return nil, err
	}
	return &hc, nil
}

func (s *PGStore) PutClickHouseConfig(ctx context.Context, id string, cfg *config.ClickHouseConfig) error {
	by, err := yaml.Marshal(cfg)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO clickhouse_configs(pipeline_id,yaml) VALUES($1,$2)
        ON CONFLICT(pipeline_id) DO UPDATE SET yaml=EXCLUDED.yaml`, id, string(by))
	return err
}

func (s *PGStore) GetMappingYAML(ctx context.Context, id string) ([]byte, error) {
	var y string
	err := s.db.QueryRowContext(ctx, `SELECT yaml FROM mappings WHERE pipeline_id=$1`, id).Scan(&y)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return []byte(""), nil
		}
		return nil, err
	}
	return []byte(y), nil
}

func (s *PGStore) PutMappingYAML(ctx context.Context, id string, y []byte) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO mappings(pipeline_id,yaml) VALUES($1,$2)
        ON CONFLICT(pipeline_id) DO UPDATE SET yaml=EXCLUDED.yaml`, id, string(y))
	return err
}

func (s *PGStore) GetFilterConfig(ctx context.Context, id string) (*config.FilterConfig, error) {
	var y string
	err := s.db.QueryRowContext(ctx, `SELECT yaml FROM filter_configs WHERE pipeline_id=$1`, id).Scan(&y)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return &config.FilterConfig{Enabled: false, Language: "CEL", Expression: ""}, nil
		}
		return nil, err
	}
	var fc config.FilterConfig
	if err := yaml.Unmarshal([]byte(y), &fc); err != nil {
		return nil, err
	}
	if fc.Language == "" {
		fc.Language = "CEL"
	}
	return &fc, nil
}

func (s *PGStore) PutFilterConfig(ctx context.Context, id string, cfg *config.FilterConfig) error {
	by, err := yaml.Marshal(cfg)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `INSERT INTO filter_configs(pipeline_id,yaml) VALUES($1,$2)
		ON CONFLICT(pipeline_id) DO UPDATE SET yaml=EXCLUDED.yaml`, id, string(by))
	return err
}

// Desired state
func (s *PGStore) GetState(ctx context.Context, id string) (*PipelineState, error) {
	var st PipelineState
	err := s.db.QueryRowContext(ctx, `SELECT pipeline_id, desired, replicas, updated_at FROM pipeline_state WHERE pipeline_id=$1`, id).Scan(&st.PipelineID, &st.Desired, &st.Replicas, &st.UpdatedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return &PipelineState{PipelineID: id, Desired: DesiredStopped, Replicas: 1, UpdatedAt: time.Now()}, nil
		}
		return nil, err
	}
	if st.Replicas <= 0 {
		st.Replicas = 1
	}
	return &st, nil
}

func (s *PGStore) SetDesiredState(ctx context.Context, id string, desired DesiredState, replicas int) error {
	if replicas <= 0 {
		replicas = 1
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO pipeline_state(pipeline_id,desired,replicas,updated_at) VALUES($1,$2,$3,now())
        ON CONFLICT(pipeline_id) DO UPDATE SET desired=EXCLUDED.desired, replicas=EXCLUDED.replicas, updated_at=now()`, id, string(desired), replicas)
	return err
}

// Leases (slot-based multi-replica)
func (s *PGStore) ListAssignments(ctx context.Context, id string) ([]Assignment, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT pipeline_id, slot, worker_id, lease_until FROM leases WHERE pipeline_id=$1 AND lease_until > now() ORDER BY slot`, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Assignment{}
	for rows.Next() {
		var a Assignment
		if err := rows.Scan(&a.PipelineID, &a.Slot, &a.WorkerID, &a.LeaseUntil); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

func (s *PGStore) TryAcquireSlot(ctx context.Context, id, workerID string, ttl time.Duration) (int, bool, error) {
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return -1, false, err
	}
	defer func() { _ = tx.Rollback() }()
	// desired replicas
	var replicas int
	if err := tx.QueryRowContext(ctx, `SELECT replicas FROM pipeline_state WHERE pipeline_id=$1`, id).Scan(&replicas); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			replicas = 1
		} else {
			return -1, false, err
		}
	}
	if replicas <= 0 {
		replicas = 1
	}
	// lock current active leases for this pipeline
	rows, err := tx.QueryContext(ctx, `SELECT slot FROM leases WHERE pipeline_id=$1 AND lease_until > now() FOR UPDATE`, id)
	if err != nil {
		return -1, false, err
	}
	defer rows.Close()
	occupied := map[int]bool{}
	for rows.Next() {
		var sslot int
		if err := rows.Scan(&sslot); err != nil {
			return -1, false, err
		}
		occupied[sslot] = true
	}
	// find first free slot [0..replicas-1]
	chosen := -1
	for i := 0; i < replicas; i++ {
		if !occupied[i] {
			chosen = i
			break
		}
	}
	if chosen == -1 {
		return -1, false, nil
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO leases(pipeline_id, slot, worker_id, lease_until) VALUES($1,$2,$3,$4)`, id, chosen, workerID, time.Now().Add(ttl))
	if err != nil {
		return -1, false, err
	}
	if err := tx.Commit(); err != nil {
		return -1, false, err
	}
	return chosen, true, nil
}

func (s *PGStore) RenewSlots(ctx context.Context, id, workerID string, slots []int, ttl time.Duration) error {
	if len(slots) == 0 {
		return nil
	}
	// Renew all provided slots in a transaction (per-slot updates to avoid array binding)
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	until := time.Now().Add(ttl)
	for _, sl := range slots {
		if _, err := tx.ExecContext(ctx, `UPDATE leases SET lease_until=$4 WHERE pipeline_id=$1 AND worker_id=$2 AND slot=$3`, id, workerID, sl, until); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *PGStore) ReleaseSlot(ctx context.Context, id, workerID string, slot int) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM leases WHERE pipeline_id=$1 AND worker_id=$2 AND slot=$3`, id, workerID, slot)
	return err
}

// Worker registry
func (s *PGStore) UpsertWorkerHeartbeat(ctx context.Context, workerID, mode, version string) error {
	if workerID == "" {
		return errors.New("workerID required")
	}
	if mode == "" {
		mode = "leases"
	}
	if version == "" {
		version = "dev"
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO workers(worker_id, mode, version, last_seen) VALUES($1,$2,$3,now())
		ON CONFLICT(worker_id) DO UPDATE SET mode=EXCLUDED.mode, version=EXCLUDED.version, last_seen=now()`, workerID, mode, version)
	return err
}

func (s *PGStore) ListWorkers(ctx context.Context) ([]Worker, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT worker_id, mode, version, last_seen FROM workers ORDER BY worker_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Worker
	for rows.Next() {
		var w Worker
		if err := rows.Scan(&w.WorkerID, &w.Mode, &w.Version, &w.LastSeen); err != nil {
			return nil, err
		}
		out = append(out, w)
	}
	return out, rows.Err()
}

// Claims
func (s *PGStore) UpsertClaim(ctx context.Context, pipelineID, workerID, topic string, partition int) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO claims(pipeline_id, worker_id, topic, partition, last_seen)
		VALUES($1,$2,$3,$4,now())
		ON CONFLICT(pipeline_id, worker_id, topic, partition) DO UPDATE SET last_seen=now()`,
		pipelineID, workerID, topic, partition)
	return err
}

func (s *PGStore) RemoveClaim(ctx context.Context, pipelineID, workerID, topic string, partition int) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM claims WHERE pipeline_id=$1 AND worker_id=$2 AND topic=$3 AND partition=$4`, pipelineID, workerID, topic, partition)
	return err
}

func (s *PGStore) ListClaims(ctx context.Context, pipelineID string) ([]Claim, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT pipeline_id, worker_id, topic, partition, last_seen FROM claims WHERE pipeline_id=$1 ORDER BY topic, partition`, pipelineID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Claim
	for rows.Next() {
		var c Claim
		if err := rows.Scan(&c.PipelineID, &c.WorkerID, &c.Topic, &c.Partition, &c.LastSeen); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}
