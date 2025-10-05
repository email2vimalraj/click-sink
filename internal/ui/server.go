package ui

import (
	"context"
	"encoding/json"
	"html/template"
	"io/fs"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/yourname/click-sink/internal/config"
	"github.com/yourname/click-sink/internal/pipeline"
	"github.com/yourname/click-sink/internal/schema"
	"gopkg.in/yaml.v3"
)

type Server struct {
	addr    string
	dataDir string
	sampleN int
	mu      sync.Mutex
	cfg     *config.Config
	mapping *schema.Mapping
	running bool
	cancel  context.CancelFunc
	lastErr string
	started time.Time
	tpl     *template.Template
	stats   *stats
}

func New(addr, dataDir string, sampleN int, tplFS fs.FS) (*Server, error) {
	s := &Server{addr: addr, dataDir: dataDir, sampleN: sampleN}
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, err
	}
	// Load templates
	funcMap := template.FuncMap{"toYAML": func(m *schema.Mapping) string { b, _ := yaml.Marshal(m); return string(b) }}
	t, err := template.New("").Funcs(funcMap).ParseFS(tplFS, "*.html")
	if err != nil {
		return nil, err
	}
	s.tpl = t
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
	return s, nil
}

func (s *Server) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/", s.handleIndex)
	mux.HandleFunc("/save-config", s.handleSaveConfig)
	mux.HandleFunc("/sample", s.handleSample)
	mux.HandleFunc("/api/sample", s.handleSampleJSON)
	mux.HandleFunc("/api/config", s.handleAPIConfig)
	mux.HandleFunc("/api/mapping", s.handleAPIMapping)
	mux.HandleFunc("/api/start", s.handleAPIStart)
	mux.HandleFunc("/api/stop", s.handleAPIStop)
	mux.HandleFunc("/save-mapping", s.handleSaveMapping)
	mux.HandleFunc("/start", s.handleStart)
	mux.HandleFunc("/stop", s.handleStop)
	mux.HandleFunc("/api/status", s.handleStatus)
	return s.withCORS(mux)
}

func (s *Server) Start() error { return http.ListenAndServe(s.addr, s.routes()) }

func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	data := struct {
		Cfg     *config.Config
		Mapping *schema.Mapping
		Running bool
		Started time.Time
		LastErr string
	}{s.cfg, s.mapping, s.running, s.started, s.lastErr}
	s.mu.Unlock()
	_ = s.tpl.ExecuteTemplate(w, "index.html", data)
}

func (s *Server) handleSaveConfig(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	cfg := &config.Config{
		Kafka: config.KafkaConfig{
			Brokers:          splitCSV(r.FormValue("kafka_brokers")),
			Topic:            r.FormValue("kafka_topic"),
			GroupID:          r.FormValue("kafka_group"),
			SecurityProtocol: r.FormValue("kafka_security"),
			SASLUsername:     r.FormValue("kafka_sasl_user"),
			SASLPassword:     r.FormValue("kafka_sasl_pass"),
			SASLMechanism:    r.FormValue("kafka_sasl_mech"),
		},
		ClickHouse: config.ClickHouseConfig{
			DSN:                  r.FormValue("ch_dsn"),
			Database:             r.FormValue("ch_db"),
			Table:                r.FormValue("ch_table"),
			BatchSize:            atoiDefault(r.FormValue("ch_batch"), 500),
			BatchFlushInterval:   r.FormValue("ch_flush"),
			InsertRatePerSec:     atoiDefault(r.FormValue("ch_rate"), 0),
			CreateTableIfMissing: true,
		},
	}
	// Validate via Load marshaling path
	b, err := yamlMarshal(cfg)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	if err := os.WriteFile(filepath.Join(s.dataDir, "config.yaml"), b, 0o644); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	s.mu.Lock()
	s.cfg = cfg
	s.mu.Unlock()
	http.Redirect(w, r, "/", http.StatusSeeOther)
}

func (s *Server) handleSample(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	cfg := s.cfg
	s.mu.Unlock()
	if cfg == nil {
		http.Error(w, "save config first", 400)
		return
	}
	// Run detect and set mapping
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	m, err := schema.DetectAndRecommend(ctx, cfg, s.sampleN)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	mp, err := schema.ParseMapping(m)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	s.mu.Lock()
	s.mapping = mp
	s.mu.Unlock()
	if err := os.WriteFile(filepath.Join(s.dataDir, "mapping.yaml"), m, 0o644); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	http.Redirect(w, r, "/", http.StatusSeeOther)
}

func (s *Server) handleSaveMapping(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	y := r.FormValue("mapping_yaml")
	mp, err := schema.ParseMapping([]byte(y))
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	if err := os.WriteFile(filepath.Join(s.dataDir, "mapping.yaml"), []byte(y), 0o644); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	s.mu.Lock()
	s.mapping = mp
	s.mu.Unlock()
	http.Redirect(w, r, "/", http.StatusSeeOther)
}

func (s *Server) handleStart(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		http.Redirect(w, r, "/", http.StatusSeeOther)
		return
	}
	cfg := s.cfg
	mp := s.mapping
	s.mu.Unlock()
	if cfg == nil || mp == nil {
		http.Error(w, "config and mapping required", 400)
		return
	}
	// attach observer for metrics
	s.stats = &stats{}
	p, err := pipeline.NewWithObserver(cfg, mp, s.stats)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.mu.Lock()
	s.cancel = cancel
	s.running = true
	s.started = time.Now()
	s.lastErr = ""
	s.mu.Unlock()
	go func() {
		if err := p.Run(ctx); err != nil {
			s.mu.Lock()
			s.lastErr = err.Error()
			s.running = false
			s.mu.Unlock()
		} else {
			s.mu.Lock()
			s.running = false
			s.mu.Unlock()
		}
	}()
	http.Redirect(w, r, "/", http.StatusSeeOther)
}

func (s *Server) handleStop(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	cancel := s.cancel
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	http.Redirect(w, r, "/", http.StatusSeeOther)
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	st := s.stats
	resp := struct {
		Running     bool      `json:"running"`
		Started     time.Time `json:"started"`
		LastErr     string    `json:"lastErr"`
		TotalRows   int64     `json:"totalRows"`
		LastBatch   int       `json:"lastBatch"`
		LastBatchAt time.Time `json:"lastBatchAt"`
	}{s.running, s.started, s.lastErr, 0, 0, time.Time{}}
	if st != nil {
		resp.TotalRows = st.TotalRows()
		resp.LastBatch = st.LastBatch()
		resp.LastBatchAt = st.LastBatchAt()
	}
	s.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

// helpers
func splitCSV(s string) []string {
	var out []string
	cur := ""
	for _, r := range s {
		if r == ',' {
			if cur != "" {
				out = append(out, cur)
				cur = ""
			}
			continue
		}
		if r == ' ' || r == '\n' || r == '\t' {
			continue
		}
		cur += string(r)
	}
	if cur != "" {
		out = append(out, cur)
	}
	return out
}

func atoiDefault(s string, d int) int {
	if v, err := strconv.Atoi(s); err == nil {
		return v
	}
	return d
}

func yamlMarshal(cfg *config.Config) ([]byte, error) { return yaml.Marshal(cfg) }

// Sampling JSON endpoint: returns suggested fields from sample payloads
func (s *Server) handleSampleJSON(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	cfg := s.cfg
	s.mu.Unlock()
	if cfg == nil {
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
	// Reuse detection to get recommended field list
	y, err := schema.DetectAndRecommend(ctx, cfg, n)
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
		out = append(out, field{FieldPath: c.FieldPath, Column: c.Column, Type: c.Type})
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(out)
}

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

// --- JSON API for Next.js ---

// handleAPIConfig supports GET (return JSON config) and POST (JSON body to save config)
func (s *Server) handleAPIConfig(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodOptions {
		s.cors(w)
		w.WriteHeader(http.StatusNoContent)
		return
	}
	switch r.Method {
	case http.MethodGet:
		s.mu.Lock()
		cfg := s.cfg
		s.mu.Unlock()
		if cfg == nil {
			cfg = &config.Config{}
		}
		s.corsJSON(w)
		_ = json.NewEncoder(w).Encode(cfg)
	case http.MethodPost:
		var cfg config.Config
		if err := json.NewDecoder(r.Body).Decode(&cfg); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		b, err := yamlMarshal(&cfg)
		if err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		if err := os.WriteFile(filepath.Join(s.dataDir, "config.yaml"), b, 0o644); err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		s.mu.Lock()
		s.cfg = &cfg
		s.mu.Unlock()
		s.corsJSON(w)
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleAPIMapping supports GET (return mapping) and POST (save mapping)
func (s *Server) handleAPIMapping(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodOptions {
		s.cors(w)
		w.WriteHeader(http.StatusNoContent)
		return
	}
	switch r.Method {
	case http.MethodGet:
		s.mu.Lock()
		mp := s.mapping
		s.mu.Unlock()
		if mp == nil {
			mp = &schema.Mapping{}
		}
		s.corsJSON(w)
		_ = json.NewEncoder(w).Encode(mp)
	case http.MethodPost:
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
		if err := os.WriteFile(filepath.Join(s.dataDir, "mapping.yaml"), by, 0o644); err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		s.mu.Lock()
		s.mapping = &mp
		s.mu.Unlock()
		s.corsJSON(w)
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleAPIStart and handleAPIStop for programmatic control
func (s *Server) handleAPIStart(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodOptions {
		s.cors(w)
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	// reuse form-based start
	s.handleStart(w, r)
}

func (s *Server) handleAPIStop(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodOptions {
		s.cors(w)
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	s.handleStop(w, r)
}

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
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
}

func (s *Server) corsJSON(w http.ResponseWriter) {
	s.cors(w)
	w.Header().Set("Content-Type", "application/json")
}
