package pipeline

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/IBM/sarama"
	"github.com/yourname/click-sink/internal/clickhouse"
	"github.com/yourname/click-sink/internal/config"
	kfk "github.com/yourname/click-sink/internal/kafka"
	"github.com/yourname/click-sink/internal/metrics"
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
	claimObs   kfk.ClaimObserver
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

// WithClaimObserver attaches a partition claim observer; optional.
func (p *Pipeline) WithClaimObserver(co kfk.ClaimObserver) *Pipeline {
	p.claimObs = co
	return p
}

// Run starts consuming from Kafka, maps messages, and inserts batches into ClickHouse until ctx is done.
func (p *Pipeline) Run(ctx context.Context) error {
	defer p.ch.Close()
	if p.obs != nil {
		p.obs.OnStart()
	}
	// Ensure table exists
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

	consumer, err := kfk.NewConsumerWithObserver(&p.cfg.Kafka, "click-sink", sarama.OffsetNewest, p.claimObs)
	if err != nil {
		return err
	}
	defer consumer.Close()
	msgs, errCh := consumer.Consume(ctx)

	var (
		mu   sync.Mutex
		rows []clickhouse.Row
	)
	flush := func() error {
		mu.Lock()
		defer mu.Unlock()
		if len(rows) == 0 {
			return nil
		}
		r := rows
		rows = make([]clickhouse.Row, 0, p.batchSize)
		started := time.Now()
		if err := p.ch.InsertBatch(ctx, p.cfg.ClickHouse.Table, cols, r); err != nil {
			if p.obs != nil {
				p.obs.OnError(err)
			}
			metrics.IncError(p.cfg.Kafka.GroupID, "insert")
			return err
		}
		total := atomic.AddInt64(&p.totalRows, int64(len(r)))
		if p.obs != nil {
			p.obs.OnBatchInserted(len(r), total, time.Now())
		}
		// Use Kafka groupID as a proxy for pipeline identity when running standalone
		metrics.IncBatch(p.cfg.Kafka.GroupID)
		metrics.AddRows(p.cfg.Kafka.GroupID, len(r))
		metrics.ObserveInsertLatency(p.cfg.Kafka.GroupID, time.Since(started).Seconds())
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
			metrics.IncError(p.cfg.Kafka.GroupID, "kafka")
			return err
		case <-flushTimer.C:
			if err := flush(); err != nil {
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
	if v == nil {
		return nil
	}
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
		case string:
			// try parse integer
			if i, err := json.Number(t).Int64(); err == nil {
				return i
			}
			// fallback: try float then cast
			if f, err := json.Number(t).Float64(); err == nil {
				return int64(f)
			}
		}
	case "Float64", "Nullable(Float64)":
		switch t := v.(type) {
		case float64:
			return t
		case json.Number:
			f, _ := t.Float64()
			return f
		case string:
			if f, err := json.Number(t).Float64(); err == nil {
				return f
			}
		}
	case "Bool", "Nullable(Bool)":
		switch t := v.(type) {
		case bool:
			return t
		case string:
			if t == "true" || t == "TRUE" || t == "1" {
				return true
			}
			if t == "false" || t == "FALSE" || t == "0" {
				return false
			}
			// attempt to parse numeric truthiness
			if n, err := json.Number(t).Int64(); err == nil {
				return n != 0
			}
		}
	case "String", "Nullable(String)":
		switch t := v.(type) {
		case string:
			return t
		default:
			// attempt to marshal and unquote
			b, err := json.Marshal(v)
			if err == nil {
				var s string
				if err := json.Unmarshal(b, &s); err == nil {
					return s
				}
				return string(b)
			}
			return v
		}
	}
	// Date/DateTime/DateTime64 handling (and Nullable variants)
	isDate := typ == "Date" || typ == "Nullable(Date)"
	isDT := strings.HasPrefix(typ, "DateTime") || strings.HasPrefix(typ, "Nullable(DateTime")
	if isDate || isDT {
		switch t := v.(type) {
		case time.Time:
			return t
		case string:
			s := strings.TrimSpace(t)
			s = strings.Trim(s, "\"'")
			// Try RFC3339 first
			if tt, err := time.Parse(time.RFC3339Nano, s); err == nil {
				return tt
			}
			// Common ClickHouse formats
			layouts := []string{
				"2006-01-02 15:04:05.999999999",
				"2006-01-02 15:04:05",
				"2006-01-02",
			}
			for _, l := range layouts {
				if tt, err := time.Parse(l, s); err == nil {
					return tt
				}
			}
			// numeric epoch seconds/milliseconds
			if n, err := json.Number(s).Int64(); err == nil {
				// heuristically treat >1e12 as milliseconds
				if n > 1_000_000_000_000 {
					sec := n / 1000
					ns := (n % 1000) * int64(time.Millisecond)
					return time.Unix(sec, ns)
				}
				return time.Unix(n, 0)
			}
		case float64:
			return time.Unix(int64(t), 0)
		case int64:
			return time.Unix(t, 0)
		case json.Number:
			if n, err := t.Int64(); err == nil {
				return time.Unix(n, 0)
			}
		}
		// if nothing matched, return nil for Nullable types
		if strings.HasPrefix(typ, "Nullable(") {
			return nil
		}
	}
	// default to string
	b, err := json.Marshal(v)
	if err == nil {
		return string(b)
	}
	return v
}
