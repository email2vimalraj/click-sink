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
		`CREATE TABLE IF NOT EXISTS pipeline_state (
            pipeline_id TEXT PRIMARY KEY REFERENCES pipelines(id) ON DELETE CASCADE,
            desired TEXT NOT NULL DEFAULT 'stopped',
            replicas INTEGER NOT NULL DEFAULT 1,
            updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
        )`,
		`CREATE TABLE IF NOT EXISTS leases (
            pipeline_id TEXT PRIMARY KEY REFERENCES pipelines(id) ON DELETE CASCADE,
            worker_id TEXT NOT NULL,
            lease_until TIMESTAMPTZ NOT NULL
        )`,
	}
	for _, st := range stmts {
		if _, err := s.db.Exec(st); err != nil {
			return err
		}
	}
	return nil
}

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

// Leases (single-replica)
func (s *PGStore) TryAcquire(ctx context.Context, id, workerID string, ttl time.Duration) (bool, error) {
	// atomically acquire if expired or absent
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback() }()
	var curWorker string
	var leaseUntil time.Time
	err = tx.QueryRowContext(ctx, `SELECT worker_id, lease_until FROM leases WHERE pipeline_id=$1 FOR UPDATE`, id).Scan(&curWorker, &leaseUntil)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			_, err = tx.ExecContext(ctx, `INSERT INTO leases(pipeline_id,worker_id,lease_until) VALUES($1,$2,$3)`, id, workerID, time.Now().Add(ttl))
			if err != nil {
				return false, err
			}
			if err := tx.Commit(); err != nil {
				return false, err
			}
			return true, nil
		}
		return false, err
	}
	if time.Now().After(leaseUntil) {
		_, err = tx.ExecContext(ctx, `UPDATE leases SET worker_id=$2, lease_until=$3 WHERE pipeline_id=$1`, id, workerID, time.Now().Add(ttl))
		if err != nil {
			return false, err
		}
		if err := tx.Commit(); err != nil {
			return false, err
		}
		return true, nil
	}
	// not expired
	return false, nil
}

func (s *PGStore) Renew(ctx context.Context, id, workerID string, ttl time.Duration) error {
	res, err := s.db.ExecContext(ctx, `UPDATE leases SET lease_until=$3 WHERE pipeline_id=$1 AND worker_id=$2`, id, workerID, time.Now().Add(ttl))
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return errors.New("not owner")
	}
	return nil
}

func (s *PGStore) Release(ctx context.Context, id, workerID string) error {
	res, err := s.db.ExecContext(ctx, `DELETE FROM leases WHERE pipeline_id=$1 AND worker_id=$2`, id, workerID)
	if err != nil {
		return err
	}
	_ = res
	return nil
}
