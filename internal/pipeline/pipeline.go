package pipeline

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"

	"github.com/yourname/click-sink/internal/clickhouse"
	"github.com/yourname/click-sink/internal/config"
	kfk "github.com/yourname/click-sink/internal/kafka"
	"github.com/yourname/click-sink/internal/schema"
)

type Pipeline struct {
	cfg        *config.Config
	mapg       *schema.Mapping
	ch         *clickhouse.Client
	batchSize  int
	flushEvery time.Duration
	insertRate int // inserts/sec (0 = unlimited)
	obs        Observer
	totalRows  int64
}

// Observer provides callbacks for pipeline events.
type Observer interface {
	OnStart()
	OnBatchInserted(batchRows int, totalRows int64, at time.Time)
	OnError(err error)
	OnStop()
}

func New(cfg *config.Config, m *schema.Mapping) (*Pipeline, error) {
	client, err := clickhouse.NewClient(&cfg.ClickHouse)
	if err != nil {
		return nil, err
	}
	dur, _ := time.ParseDuration(cfg.ClickHouse.BatchFlushInterval)
	return &Pipeline{cfg: cfg, mapg: m, ch: client, batchSize: cfg.ClickHouse.BatchSize, flushEvery: dur, insertRate: cfg.ClickHouse.InsertRatePerSec}, nil
}

// NewWithObserver constructs a Pipeline with an observer for metrics.
func NewWithObserver(cfg *config.Config, m *schema.Mapping, obs Observer) (*Pipeline, error) {
	p, err := New(cfg, m)
	if err != nil {
		return nil, err
	}
	p.obs = obs
	return p, nil
}

func (p *Pipeline) Run(ctx context.Context) error {
	defer p.ch.Close()
	if p.obs != nil {
		p.obs.OnStart()
	}
	// Ensure table
	cols := make([]clickhouse.Column, len(p.mapg.Columns))
	for i, c := range p.mapg.Columns {
		cols[i] = clickhouse.Column{Name: c.Column, Type: c.Type}
	}
	if err := p.ch.EnsureTable(ctx, p.cfg.ClickHouse.Table, cols); err != nil {
		if p.obs != nil {
			p.obs.OnError(err)
		}
		return err
	}

	consumer, err := kfk.NewConsumer(&p.cfg.Kafka, "click-sink")
	if err != nil {
		return err
	}
	defer consumer.Close()
	msgs, errCh := consumer.Consume(ctx)

	rows := make([]clickhouse.Row, 0, p.batchSize)
	var mu sync.Mutex
	flush := func() error {
		mu.Lock()
		defer mu.Unlock()
		if len(rows) == 0 {
			return nil
		}
		r := rows
		rows = make([]clickhouse.Row, 0, p.batchSize)
		if err := p.ch.InsertBatch(ctx, p.cfg.ClickHouse.Table, cols, r); err != nil {
			if p.obs != nil {
				p.obs.OnError(err)
			}
			return err
		}
		total := atomic.AddInt64(&p.totalRows, int64(len(r)))
		if p.obs != nil {
			p.obs.OnBatchInserted(len(r), total, time.Now())
		}
		return nil
	}

	// Rate limiter: tokens per second
	var tick <-chan time.Time
	if p.insertRate > 0 {
		tick = time.Tick(time.Second / time.Duration(p.insertRate))
	}
	flushTimer := time.NewTimer(p.flushEvery)
	defer flushTimer.Stop()

	for {
		select {
		case <-ctx.Done():
			err := flush()
			if p.obs != nil {
				p.obs.OnStop()
			}
			return err
		case err := <-errCh:
			if p.obs != nil {
				p.obs.OnError(err)
			}
			return err
		case <-flushTimer.C:
			if err := flush(); err != nil {
				if p.obs != nil {
					p.obs.OnError(err)
				}
				return err
			}
			flushTimer.Reset(p.flushEvery)
		case m, ok := <-msgs:
			if !ok {
				err := flush()
				if p.obs != nil {
					p.obs.OnStop()
				}
				return err
			}
			row, ok := p.mapMessage(m.Value)
			if !ok {
				m.Ack()
				continue
			}
			mu.Lock()
			rows = append(rows, row)
			ready := len(rows) >= p.batchSize
			mu.Unlock()
			m.Ack()
			if ready {
				if tick != nil {
					<-tick
				}
				if err := flush(); err != nil {
					return err
				}
			}
		}
	}
}

func (p *Pipeline) mapMessage(b []byte) (clickhouse.Row, bool) {
	var v any
	if err := json.Unmarshal(b, &v); err != nil {
		return nil, false
	}
	flat := flatten("", v)
	row := make([]any, len(p.mapg.Columns))
	for i, c := range p.mapg.Columns {
		val, ok := flat[c.FieldPath]
		if !ok {
			row[i] = nil
			continue
		}
		row[i] = coerce(val, c.Type)
	}
	return row, true
}

func flatten(prefix string, v any) map[string]any {
	out := map[string]any{}
	switch t := v.(type) {
	case map[string]any:
		for k, val := range t {
			key := k
			if prefix != "" {
				key = prefix + "." + k
			}
			for kk, vv := range flatten(key, val) {
				out[kk] = vv
			}
		}
	case []any:
		out[prefix] = t
	default:
		out[prefix] = t
	}
	return out
}

func coerce(v any, typ string) any {
	switch typ {
	case "Int64", "Nullable(Int64)":
		switch t := v.(type) {
		case float64:
			return int64(t)
		case int64:
			return t
		case json.Number:
			i, _ := t.Int64()
			return i
		}
	case "Float64", "Nullable(Float64)":
		switch t := v.(type) {
		case float64:
			return t
		case json.Number:
			f, _ := t.Float64()
			return f
		}
	case "Bool", "Nullable(Bool)":
		switch t := v.(type) {
		case bool:
			return t
		}
	}
	// default to string
	b, err := json.Marshal(v)
	if err == nil {
		return string(b)
	}
	return v
}
