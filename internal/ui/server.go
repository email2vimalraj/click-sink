package ui

import (
	"context"
	"encoding/json"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	ch "github.com/yourname/click-sink/internal/clickhouse"
	"github.com/yourname/click-sink/internal/config"
	kaf "github.com/yourname/click-sink/internal/kafka"
	"github.com/yourname/click-sink/internal/pipeline"
	"github.com/yourname/click-sink/internal/schema"
	"github.com/yourname/click-sink/internal/store"
)

type Server struct {
	addr    string
	dataDir string
	sampleN int
	mu      sync.Mutex
	cfg     *config.Config // legacy single-pipeline fields (kept for backward compatibility/UI page)
	mapping *schema.Mapping
	running bool
	cancel  context.CancelFunc
	lastErr string
	started time.Time
	stats   *stats

	// multi-pipeline support
	pipelines map[string]*pipelineRuntime
	store     store.PipelineStore
}

func New(addr, dataDir string, sampleN int, tplFS fs.FS) (*Server, error) {
	s := &Server{addr: addr, dataDir: dataDir, sampleN: sampleN, pipelines: map[string]*pipelineRuntime{}}
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, err
	}
	// default to filesystem-backed store for now
	s.store = store.NewFSStore(dataDir)
	// Try load existing config/mapping
	if b, err := os.ReadFile(filepath.Join(dataDir, "config.yaml")); err == nil {
		if cfg, err2 := config.Load(filepath.Join(dataDir, "config.yaml")); err2 == nil {
			s.cfg = cfg
		} else {
			_ = err2
		}
		_ = b
	}
	if b, err := os.ReadFile(filepath.Join(dataDir, "mapping.yaml")); err == nil {
		if m, err2 := schema.ParseMapping(b); err2 == nil {
			s.mapping = m
		}
	}
	// Load existing pipelines via store
	_ = s.loadPipelines()
	return s, nil
}

// NewWithStore allows injecting a custom PipelineStore implementation.
func NewWithStore(addr string, sampleN int, st store.PipelineStore) (*Server, error) {
	s := &Server{addr: addr, dataDir: "", sampleN: sampleN, pipelines: map[string]*pipelineRuntime{}, store: st}
	// Load existing pipelines via store
	if err := s.loadPipelines(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Server) routes() http.Handler {
	mux := http.NewServeMux()
	// multi-pipeline endpoints
	mux.HandleFunc("/api/pipelines", s.handleAPIPipelines)
	mux.HandleFunc("/api/pipelines/", s.handleAPIPipeline)
	// validation endpoints
	mux.HandleFunc("/api/validate/kafka", s.handleValidateKafka)
	mux.HandleFunc("/api/validate/kafka/sample", s.handleValidateKafkaSample)
	mux.HandleFunc("/api/validate/clickhouse", s.handleValidateClickHouse)
	mux.HandleFunc("/api/validate/clickhouse/table", s.handleValidateClickHouseTable)
	return s.withCORS(mux)
}

func (s *Server) Start() error { return http.ListenAndServe(s.addr, s.routes()) }

// legacy single-pipeline HTML and direct control endpoints removed

// helpers

// legacy single-pipeline sampling endpoint removed; use /api/pipelines/{id}/sample

// stats implements pipeline.Observer for UI metrics
type stats struct {
	mu          sync.Mutex
	totalRows   int64
	lastBatch   int
	lastBatchAt time.Time
}

func (s *stats) OnStart()      {}
func (s *stats) OnError(error) {}
func (s *stats) OnStop()       {}
func (s *stats) OnBatchInserted(batchRows int, totalRows int64, at time.Time) {
	s.mu.Lock()
	s.totalRows = totalRows
	s.lastBatch = batchRows
	s.lastBatchAt = at
	s.mu.Unlock()
}
func (s *stats) TotalRows() int64       { s.mu.Lock(); defer s.mu.Unlock(); return s.totalRows }
func (s *stats) LastBatch() int         { s.mu.Lock(); defer s.mu.Unlock(); return s.lastBatch }
func (s *stats) LastBatchAt() time.Time { s.mu.Lock(); defer s.mu.Unlock(); return s.lastBatchAt }

// legacy single-pipeline JSON API endpoints removed; use /api/pipelines/*

// --- CORS helpers ---
func (s *Server) withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.cors(w)
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) cors(w http.ResponseWriter) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, PATCH, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, Accept, X-Requested-With")
	w.Header().Set("Access-Control-Max-Age", "600")
}

func (s *Server) corsJSON(w http.ResponseWriter) {
	s.cors(w)
	w.Header().Set("Content-Type", "application/json")
}

// --- Multi-pipeline types and helpers ---
type pipelineRuntime struct {
	id          string
	name        string
	description string
	cfg         *config.Config
	mapping     *schema.Mapping
	running     bool
	cancel      context.CancelFunc
	lastErr     string
	started     time.Time
	stats       *stats
	createdAt   time.Time
	updatedAt   time.Time
}

func (s *Server) loadPipelines() error {
	ctx := context.Background()
	pls, err := s.store.ListPipelines(ctx)
	if err != nil {
		return err
	}
	for _, p := range pls {
		var cfg *config.Config
		kcfg, _ := s.store.GetKafkaConfig(ctx, p.ID)
		hcfg, _ := s.store.GetClickHouseConfig(ctx, p.ID)
		if (kcfg != nil && (len(kcfg.Brokers) > 0 || kcfg.Topic != "")) || (hcfg != nil && (hcfg.DSN != "" || hcfg.Table != "")) {
			cfg = &config.Config{}
			if kcfg != nil {
				cfg.Kafka = *kcfg
			}
			if hcfg != nil {
				cfg.ClickHouse = *hcfg
			}
		}
		var mp *schema.Mapping
		if y, err := s.store.GetMappingYAML(ctx, p.ID); err == nil && len(y) > 0 {
			if m2, err2 := schema.ParseMapping(y); err2 == nil {
				mp = m2
			}
		}
		s.pipelines[p.ID] = &pipelineRuntime{id: p.ID, name: p.Name, description: p.Description, cfg: cfg, mapping: mp, stats: &stats{}, createdAt: p.CreatedAt, updatedAt: p.UpdatedAt}
	}
	return nil
}

func (s *Server) createPipeline(name string, description string) (*pipelineRuntime, error) {
	ctx := context.Background()
	p, err := s.store.CreatePipeline(ctx, name, description)
	if err != nil {
		return nil, err
	}
	pr := &pipelineRuntime{id: p.ID, name: p.Name, description: p.Description, stats: &stats{}, createdAt: p.CreatedAt, updatedAt: p.UpdatedAt}
	s.mu.Lock()
	s.pipelines[p.ID] = pr
	s.mu.Unlock()
	return pr, nil
}

func sanitizeID(s string) string {
	out := make([]rune, 0, len(s))
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' {
			out = append(out, r)
		} else if r == ' ' {
			out = append(out, '-')
		} else {
			out = append(out, '-')
		}
	}
	if len(out) == 0 {
		return "pl-" + strconv.FormatInt(time.Now().Unix(), 10)
	}
	return string(out)
}

// --- Multi-pipeline handlers ---
func (s *Server) handleAPIPipelines(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodOptions {
		s.cors(w)
		w.WriteHeader(http.StatusNoContent)
		return
	}
	switch r.Method {
	case http.MethodGet:
		s.mu.Lock()
		list := make([]any, 0, len(s.pipelines))
		for id, p := range s.pipelines {
			total := int64(0)
			if p.stats != nil {
				total = p.stats.TotalRows()
			}
			list = append(list, map[string]any{"id": id, "name": p.name, "description": p.description, "running": p.running, "lastErr": p.lastErr, "started": p.started, "totalRows": total, "createdAt": p.createdAt, "updatedAt": p.updatedAt})
		}
		s.mu.Unlock()
		s.corsJSON(w)
		_ = json.NewEncoder(w).Encode(list)
	case http.MethodPost:
		var req struct {
			Name        string `json:"name"`
			Description string `json:"description"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		if req.Name == "" {
			req.Name = "pipeline"
		}
		pr, err := s.createPipeline(req.Name, req.Description)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		s.corsJSON(w)
		_ = json.NewEncoder(w).Encode(map[string]any{"id": pr.id, "name": pr.name, "description": pr.description, "createdAt": pr.createdAt, "updatedAt": pr.updatedAt})
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (s *Server) handleAPIPipeline(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodOptions {
		s.cors(w)
		w.WriteHeader(http.StatusNoContent)
		return
	}
	base := "/api/pipelines/"
	rest := r.URL.Path[len(base):]
	if rest == "" {
		http.NotFound(w, r)
		return
	}
	parts := splitPath(rest)
	if len(parts) == 0 {
		http.NotFound(w, r)
		return
	}
	id := parts[0]
	s.mu.Lock()
	pr := s.pipelines[id]
	s.mu.Unlock()
	if pr == nil {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	// Route by subresource
	if len(parts) == 1 {
		switch r.Method {
		case http.MethodGet:
			total := int64(0)
			if pr.stats != nil {
				total = pr.stats.TotalRows()
			}
			s.corsJSON(w)
			_ = json.NewEncoder(w).Encode(map[string]any{"id": pr.id, "name": pr.name, "description": pr.description, "running": pr.running, "lastErr": pr.lastErr, "started": pr.started, "totalRows": total, "createdAt": pr.createdAt, "updatedAt": pr.updatedAt})
			return
		case http.MethodPut:
			var req struct {
				Name        string `json:"name"`
				Description string `json:"description"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, err.Error(), 400)
				return
			}
			s.mu.Lock()
			if req.Name != "" {
				pr.name = req.Name
			}
			pr.description = req.Description
			if pr.createdAt.IsZero() {
				pr.createdAt = time.Now()
			}
			pr.updatedAt = time.Now()
			s.mu.Unlock()
			_ = s.store.UpdatePipeline(r.Context(), pr.id, pr.name, pr.description)
			s.corsJSON(w)
			_ = json.NewEncoder(w).Encode(map[string]any{"id": pr.id, "name": pr.name, "description": pr.description, "createdAt": pr.createdAt, "updatedAt": pr.updatedAt})
			return
		case http.MethodDelete:
			if pr.running && pr.cancel != nil {
				pr.cancel()
			}
			// delete via store
			_ = s.store.DeletePipeline(r.Context(), pr.id)
			s.mu.Lock()
			delete(s.pipelines, id)
			s.mu.Unlock()
			s.corsJSON(w)
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "deleted"})
			return
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
	}
	sub := parts[1]
	switch sub {
	case "validate":
		// /api/pipelines/{id}/validate/{kind}
		if len(parts) < 3 {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		kind := parts[2]
		switch kind {
		case "kafka":
			if r.Method == http.MethodOptions {
				s.cors(w)
				w.WriteHeader(http.StatusNoContent)
				return
			}
			if r.Method != http.MethodPost {
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			if pr.cfg == nil {
				http.Error(w, "save kafka config first", 400)
				return
			}
			if err := kaf.ValidateConnectivity(&pr.cfg.Kafka); err != nil {
				http.Error(w, err.Error(), 500)
				return
			}
			s.corsJSON(w)
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
			return
		case "clickhouse":
			if r.Method == http.MethodOptions {
				s.cors(w)
				w.WriteHeader(http.StatusNoContent)
				return
			}
			if r.Method != http.MethodPost {
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			if pr.cfg == nil {
				http.Error(w, "save clickhouse config first", 400)
				return
			}
			client, err := ch.NewClient(&pr.cfg.ClickHouse)
			if err != nil {
				http.Error(w, err.Error(), 500)
				return
			}
			defer client.Close()
			if err := client.Ping(r.Context()); err != nil {
				http.Error(w, err.Error(), 500)
				return
			}
			s.corsJSON(w)
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
			return
		case "clickhouse-table":
			if r.Method == http.MethodOptions {
				s.cors(w)
				w.WriteHeader(http.StatusNoContent)
				return
			}
			if r.Method != http.MethodPost {
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			if pr.cfg == nil {
				http.Error(w, "save clickhouse config first", 400)
				return
			}
			var req struct {
				Table   string                        `json:"table"`
				Columns []struct{ Name, Type string } `json:"columns"`
				Create  bool                          `json:"create"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, err.Error(), 400)
				return
			}
			if strings.TrimSpace(req.Table) == "" {
				http.Error(w, "table name required", 400)
				return
			}
			client, err := ch.NewClient(&pr.cfg.ClickHouse)
			if err != nil {
				http.Error(w, err.Error(), 500)
				return
			}
			defer client.Close()
			exists, err := client.TableExists(r.Context(), pr.cfg.ClickHouse.Database, req.Table)
			if err != nil {
				http.Error(w, err.Error(), 500)
				return
			}
			if exists {
				s.corsJSON(w)
				_ = json.NewEncoder(w).Encode(map[string]any{"exists": true})
				return
			}
			if req.Create {
				cols := make([]ch.Column, 0, len(req.Columns))
				for _, c := range req.Columns {
					cols = append(cols, ch.Column{Name: c.Name, Type: c.Type})
				}
				if err := client.CreateTable(r.Context(), pr.cfg.ClickHouse.Database, req.Table, cols); err != nil {
					http.Error(w, err.Error(), 500)
					return
				}
				s.corsJSON(w)
				_ = json.NewEncoder(w).Encode(map[string]any{"created": true})
				return
			}
			s.corsJSON(w)
			_ = json.NewEncoder(w).Encode(map[string]any{"exists": false})
			return
		default:
			http.NotFound(w, r)
			return
		}
	case "kafka-config":
		switch r.Method {
		case http.MethodGet:
			s.corsJSON(w)
			var kcfg config.KafkaConfig
			if pr.cfg != nil {
				kcfg = pr.cfg.Kafka
			} else if kc, err := s.store.GetKafkaConfig(r.Context(), pr.id); err == nil && kc != nil {
				kcfg = *kc
			}
			_ = json.NewEncoder(w).Encode(kcfg)
		case http.MethodPut:
			var kcfg config.KafkaConfig
			if err := json.NewDecoder(r.Body).Decode(&kcfg); err != nil {
				http.Error(w, err.Error(), 400)
				return
			}
			if pr.cfg == nil {
				pr.cfg = &config.Config{}
			}
			pr.cfg.Kafka = kcfg
			if err := s.store.PutKafkaConfig(r.Context(), pr.id, &kcfg); err != nil {
				http.Error(w, err.Error(), 500)
				return
			}
			pr.updatedAt = time.Now()
			_ = s.store.UpdatePipeline(r.Context(), pr.id, pr.name, pr.description)
			s.corsJSON(w)
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
		return
	case "clickhouse-config":
		switch r.Method {
		case http.MethodGet:
			s.corsJSON(w)
			var hcfg config.ClickHouseConfig
			if pr.cfg != nil {
				hcfg = pr.cfg.ClickHouse
			} else if hc, err := s.store.GetClickHouseConfig(r.Context(), pr.id); err == nil && hc != nil {
				hcfg = *hc
			}
			_ = json.NewEncoder(w).Encode(hcfg)
		case http.MethodPut:
			var hcfg config.ClickHouseConfig
			if err := json.NewDecoder(r.Body).Decode(&hcfg); err != nil {
				http.Error(w, err.Error(), 400)
				return
			}
			if pr.cfg == nil {
				pr.cfg = &config.Config{}
			}
			pr.cfg.ClickHouse = hcfg
			if err := s.store.PutClickHouseConfig(r.Context(), pr.id, &hcfg); err != nil {
				http.Error(w, err.Error(), 500)
				return
			}
			pr.updatedAt = time.Now()
			_ = s.store.UpdatePipeline(r.Context(), pr.id, pr.name, pr.description)
			s.corsJSON(w)
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
		return
	case "clickhouse":
		// nested: /api/pipelines/{id}/clickhouse/{databases|tables|schema}
		if len(parts) < 3 {
			http.NotFound(w, r)
			return
		}
		if pr.cfg == nil {
			http.Error(w, "save clickhouse config first", 400)
			return
		}
		action := parts[2]
		client, err := ch.NewClient(&pr.cfg.ClickHouse)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		defer client.Close()
		switch action {
		case "databases":
			if r.Method != http.MethodGet {
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			dbs, err := client.ListDatabases(r.Context())
			if err != nil {
				http.Error(w, err.Error(), 500)
				return
			}
			s.corsJSON(w)
			_ = json.NewEncoder(w).Encode(map[string]any{"databases": dbs})
		case "tables":
			if r.Method != http.MethodGet {
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			db := r.URL.Query().Get("db")
			tbls, err := client.ListTables(r.Context(), db)
			if err != nil {
				http.Error(w, err.Error(), 500)
				return
			}
			s.corsJSON(w)
			_ = json.NewEncoder(w).Encode(map[string]any{"tables": tbls})
		case "schema":
			if r.Method != http.MethodGet {
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			db := r.URL.Query().Get("db")
			table := r.URL.Query().Get("table")
			cols, err := client.GetTableSchema(r.Context(), db, table)
			if err != nil {
				http.Error(w, err.Error(), 400)
				return
			}
			// Normalize to {name,type}
			out := make([]map[string]string, 0, len(cols))
			for _, c := range cols {
				out = append(out, map[string]string{"name": c.Name, "type": c.Type})
			}
			s.corsJSON(w)
			_ = json.NewEncoder(w).Encode(map[string]any{"columns": out})
		default:
			http.NotFound(w, r)
		}
		return
	case "config":
		switch r.Method {
		case http.MethodGet:
			s.corsJSON(w)
			if pr.cfg == nil {
				// try load from store
				kc, _ := s.store.GetKafkaConfig(r.Context(), pr.id)
				hc, _ := s.store.GetClickHouseConfig(r.Context(), pr.id)
				cfg := &config.Config{}
				if kc != nil {
					cfg.Kafka = *kc
				}
				if hc != nil {
					cfg.ClickHouse = *hc
				}
				_ = json.NewEncoder(w).Encode(cfg)
				return
			}
			_ = json.NewEncoder(w).Encode(pr.cfg)
		case http.MethodPut:
			var cfg config.Config
			if err := json.NewDecoder(r.Body).Decode(&cfg); err != nil {
				http.Error(w, err.Error(), 400)
				return
			}
			// persist into split configs via store
			_ = s.store.PutKafkaConfig(r.Context(), pr.id, &cfg.Kafka)
			_ = s.store.PutClickHouseConfig(r.Context(), pr.id, &cfg.ClickHouse)
			s.mu.Lock()
			pr.cfg = &cfg
			s.mu.Unlock()
			s.corsJSON(w)
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	case "mapping":
		switch r.Method {
		case http.MethodGet:
			s.corsJSON(w)
			if pr.mapping == nil {
				// Try load from store
				if y, err := s.store.GetMappingYAML(r.Context(), pr.id); err == nil && len(y) > 0 {
					if m2, err2 := schema.ParseMapping(y); err2 == nil {
						s.mu.Lock()
						pr.mapping = m2
						s.mu.Unlock()
						_ = json.NewEncoder(w).Encode(m2)
						return
					}
				}
				// Return an empty mapping with columns: [] instead of null
				empty := &schema.Mapping{Columns: []schema.MapColumn{}}
				_ = json.NewEncoder(w).Encode(empty)
				return
			}
			_ = json.NewEncoder(w).Encode(pr.mapping)
		case http.MethodPut:
			var mp schema.Mapping
			if err := json.NewDecoder(r.Body).Decode(&mp); err != nil {
				http.Error(w, err.Error(), 400)
				return
			}
			by, err := mp.ToYAML()
			if err != nil {
				http.Error(w, err.Error(), 400)
				return
			}
			if err := s.store.PutMappingYAML(r.Context(), pr.id, by); err != nil {
				http.Error(w, err.Error(), 500)
				return
			}
			s.mu.Lock()
			pr.mapping = &mp
			s.mu.Unlock()
			s.corsJSON(w)
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	case "status":
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		total := int64(0)
		lastBatch := 0
		lastAt := time.Time{}
		if pr.stats != nil {
			total = pr.stats.TotalRows()
			lastBatch = pr.stats.LastBatch()
			lastAt = pr.stats.LastBatchAt()
		}
		s.corsJSON(w)
		_ = json.NewEncoder(w).Encode(map[string]any{"running": pr.running, "started": pr.started, "lastErr": pr.lastErr, "totalRows": total, "lastBatch": lastBatch, "lastBatchAt": lastAt})
	case "state":
		// desired state CRUD via store
		switch r.Method {
		case http.MethodGet:
			st, err := s.store.GetState(r.Context(), pr.id)
			if err != nil {
				http.Error(w, err.Error(), 500)
				return
			}
			s.corsJSON(w)
			_ = json.NewEncoder(w).Encode(st)
		case http.MethodPut, http.MethodPatch:
			var body struct {
				Desired  string `json:"desired"`
				Replicas int    `json:"replicas"`
			}
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				http.Error(w, err.Error(), 400)
				return
			}
			if body.Replicas <= 0 {
				body.Replicas = 1
			}
			var ds store.DesiredState
			if strings.ToLower(body.Desired) == string(store.DesiredStarted) {
				ds = store.DesiredStarted
			} else {
				ds = store.DesiredStopped
			}
			if err := s.store.SetDesiredState(r.Context(), pr.id, ds, body.Replicas); err != nil {
				http.Error(w, err.Error(), 500)
				return
			}
			s.corsJSON(w)
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "ok"})
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	case "assignments":
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		as, err := s.store.ListAssignments(r.Context(), pr.id)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		// normalize for UI
		rows := make([]map[string]any, 0, len(as))
		for _, a := range as {
			rows = append(rows, map[string]any{
				"pipelineId": a.PipelineID,
				"slot":       a.Slot,
				"workerId":   a.WorkerID,
				"leaseUntil": a.LeaseUntil,
			})
		}
		s.corsJSON(w)
		_ = json.NewEncoder(w).Encode(map[string]any{"assignments": rows})
	case "start":
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if pr.running {
			s.corsJSON(w)
			_ = json.NewEncoder(w).Encode(map[string]string{"status": "already running"})
			return
		}
		if pr.cfg == nil || pr.mapping == nil {
			http.Error(w, "config and mapping required", 400)
			return
		}
		pr.stats = &stats{}
		p, err := pipeline.NewWithObserver(pr.cfg, pr.mapping, pr.stats)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		ctx, cancel := context.WithCancel(context.Background())
		s.mu.Lock()
		pr.cancel = cancel
		pr.running = true
		pr.started = time.Now()
		pr.lastErr = ""
		s.mu.Unlock()
		go func() {
			if err := p.Run(ctx); err != nil {
				s.mu.Lock()
				pr.lastErr = err.Error()
				pr.running = false
				s.mu.Unlock()
			} else {
				s.mu.Lock()
				pr.running = false
				s.mu.Unlock()
			}
		}()
		s.corsJSON(w)
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "started"})
	case "stop":
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if pr.cancel != nil {
			pr.cancel()
		}
		s.corsJSON(w)
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "stopping"})
	case "sample":
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if pr.cfg == nil {
			http.Error(w, "save config first", 400)
			return
		}
		n := s.sampleN
		if q := r.URL.Query().Get("limit"); q != "" {
			if v, err := strconv.Atoi(q); err == nil && v > 0 {
				n = v
			}
		}
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()
		y, err := schema.DetectAndRecommend(ctx, pr.cfg, n)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		mp, err := schema.ParseMapping(y)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		type field struct {
			FieldPath string `json:"fieldPath"`
			Column    string `json:"column"`
			Type      string `json:"type"`
		}
		out := make([]field, 0, len(mp.Columns))
		for _, c := range mp.Columns {
			out = append(out, field{c.FieldPath, c.Column, c.Type})
		}
		s.corsJSON(w)
		_ = json.NewEncoder(w).Encode(out)
	default:
		http.NotFound(w, r)
	}
}

func splitPath(s string) []string {
	var parts []string
	cur := ""
	for i := 0; i < len(s); i++ {
		if s[i] == '/' {
			if cur != "" {
				parts = append(parts, cur)
				cur = ""
			}
		} else {
			cur += string(s[i])
		}
	}
	if cur != "" {
		parts = append(parts, cur)
	}
	return parts
}

// meta persistence is handled by the store

// --- Validation Handlers ---
// Kafka: POST body optional; if absent, try server-level cfg or pipeline cfg
func (s *Server) handleValidateKafka(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodOptions {
		s.cors(w)
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var kcfg config.KafkaConfig
	// Prefer body
	if r.Body != nil {
		_ = json.NewDecoder(r.Body).Decode(&kcfg)
	}
	// Fallback to legacy cfg
	if len(kcfg.Brokers) == 0 || kcfg.Topic == "" {
		s.mu.Lock()
		if s.cfg != nil {
			kcfg = s.cfg.Kafka
		}
		s.mu.Unlock()
	}
	if len(kcfg.Brokers) == 0 || kcfg.Topic == "" {
		http.Error(w, "kafka brokers/topic required", 400)
		return
	}
	if err := kaf.ValidateConnectivity(&kcfg); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	s.corsJSON(w)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

// Kafka sample + schema infer
func (s *Server) handleValidateKafkaSample(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodOptions {
		s.cors(w)
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		Kafka config.KafkaConfig `json:"kafka"`
		Limit int                `json:"limit"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)
	if req.Limit <= 0 {
		req.Limit = s.sampleN
	}
	kcfg := req.Kafka
	if len(kcfg.Brokers) == 0 || kcfg.Topic == "" {
		s.mu.Lock()
		if s.cfg != nil {
			kcfg = s.cfg.Kafka
		}
		s.mu.Unlock()
	}
	if len(kcfg.Brokers) == 0 || kcfg.Topic == "" {
		http.Error(w, "kafka brokers/topic required", 400)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	// Build a temp full config for schema call
	cfg := &config.Config{Kafka: kcfg}
	y, err := schema.DetectAndRecommend(ctx, cfg, req.Limit)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	// Return both sample-derived mapping YAML and parsed columns
	mp, err := schema.ParseMapping(y)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	type field struct {
		FieldPath string `json:"fieldPath"`
		Column    string `json:"column"`
		Type      string `json:"type"`
	}
	cols := make([]field, 0, len(mp.Columns))
	for _, c := range mp.Columns {
		cols = append(cols, field{c.FieldPath, c.Column, c.Type})
	}
	s.corsJSON(w)
	_ = json.NewEncoder(w).Encode(map[string]any{"mappingYAML": string(y), "fields": cols})
}

// ClickHouse connectivity validation
func (s *Server) handleValidateClickHouse(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodOptions {
		s.cors(w)
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var hcfg config.ClickHouseConfig
	_ = json.NewDecoder(r.Body).Decode(&hcfg)
	if hcfg.DSN == "" {
		s.mu.Lock()
		if s.cfg != nil {
			hcfg = s.cfg.ClickHouse
		}
		s.mu.Unlock()
	}
	if hcfg.DSN == "" {
		http.Error(w, "clickhouse dsn required", 400)
		return
	}
	client, err := ch.NewClient(&hcfg)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	defer client.Close()
	if err := client.Ping(r.Context()); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	s.corsJSON(w)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

// ClickHouse table check/create
func (s *Server) handleValidateClickHouseTable(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodOptions {
		s.cors(w)
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		ClickHouse config.ClickHouseConfig       `json:"clickhouse"`
		Table      string                        `json:"table"`
		Columns    []struct{ Name, Type string } `json:"columns"`
		Create     bool                          `json:"create"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	hcfg := req.ClickHouse
	if hcfg.DSN == "" {
		s.mu.Lock()
		if s.cfg != nil {
			hcfg = s.cfg.ClickHouse
		}
		s.mu.Unlock()
	}
	if hcfg.DSN == "" {
		http.Error(w, "clickhouse dsn required", 400)
		return
	}
	client, err := ch.NewClient(&hcfg)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	defer client.Close()
	exists, err := client.TableExists(r.Context(), hcfg.Database, req.Table)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	if exists {
		s.corsJSON(w)
		_ = json.NewEncoder(w).Encode(map[string]any{"exists": true})
		return
	}
	if req.Create {
		cols := make([]ch.Column, 0, len(req.Columns))
		for _, c := range req.Columns {
			cols = append(cols, ch.Column{Name: c.Name, Type: c.Type})
		}
		if err := client.CreateTable(r.Context(), hcfg.Database, req.Table, cols); err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		s.corsJSON(w)
		_ = json.NewEncoder(w).Encode(map[string]any{"created": true})
		return
	}
	s.corsJSON(w)
	_ = json.NewEncoder(w).Encode(map[string]any{"exists": false})
}
