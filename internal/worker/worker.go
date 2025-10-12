package worker

import (
	"context"
	"log"
	"os"
	"sync"
	"time"

	"github.com/yourname/click-sink/internal/config"
	"github.com/yourname/click-sink/internal/pipeline"
	"github.com/yourname/click-sink/internal/schema"
	"github.com/yourname/click-sink/internal/store"
)

// Runner manages pipeline processes to match desired state using a PipelineStore for coordination.
type Runner struct {
	Store          store.PipelineStore
	WorkerID       string
	ReconcileEvery time.Duration
	LeaseTTL       time.Duration

	mu      sync.Mutex
	running map[string]context.CancelFunc // pipelineID -> cancel
}

func NewRunner(st store.PipelineStore, workerID string, reconcileEvery, leaseTTL time.Duration) *Runner {
	if reconcileEvery <= 0 {
		reconcileEvery = 5 * time.Second
	}
	if leaseTTL <= 0 {
		leaseTTL = 20 * time.Second
	}
	return &Runner{Store: st, WorkerID: workerID, ReconcileEvery: reconcileEvery, LeaseTTL: leaseTTL, running: map[string]context.CancelFunc{}}
}

// Run starts the reconcile loop until ctx is done.
func (r *Runner) Run(ctx context.Context) error {
	ticker := time.NewTicker(r.ReconcileEvery)
	defer ticker.Stop()
	for {
		if err := r.reconcileOnce(ctx); err != nil {
			log.Printf("worker: reconcile error: %v", err)
		}
		select {
		case <-ctx.Done():
			r.stopAll()
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func (r *Runner) stopAll() {
	r.mu.Lock()
	defer r.mu.Unlock()
	for id, cancel := range r.running {
		cancel()
		delete(r.running, id)
		_ = r.Store.Release(context.Background(), id, r.WorkerID)
	}
}

func (r *Runner) reconcileOnce(ctx context.Context) error {
	pls, err := r.Store.ListPipelines(ctx)
	if err != nil {
		return err
	}
	now := time.Now()
	for _, p := range pls {
		st, err := r.Store.GetState(ctx, p.ID)
		if err != nil {
			log.Printf("worker: get state %s: %v", p.ID, err)
			continue
		}
		desiredStart := st != nil && st.Desired == store.DesiredStarted && st.Replicas > 0
		r.mu.Lock()
		_, isRunning := r.running[p.ID]
		r.mu.Unlock()

		if desiredStart {
			if isRunning {
				// renew lease
				_ = r.Store.Renew(ctx, p.ID, r.WorkerID, r.LeaseTTL)
				continue
			}
			// try acquire lease
			ok, err := r.Store.TryAcquire(ctx, p.ID, r.WorkerID, r.LeaseTTL)
			if err != nil {
				log.Printf("worker: try acquire %s: %v", p.ID, err)
				continue
			}
			if !ok {
				continue
			}
			// load config and mapping
			kcfg, _ := r.Store.GetKafkaConfig(ctx, p.ID)
			hcfg, _ := r.Store.GetClickHouseConfig(ctx, p.ID)
			if kcfg == nil || hcfg == nil {
				log.Printf("worker: %s missing configs", p.ID)
				_ = r.Store.Release(ctx, p.ID, r.WorkerID)
				continue
			}
			cfg := &config.Config{Kafka: *kcfg, ClickHouse: *hcfg}
			y, _ := r.Store.GetMappingYAML(ctx, p.ID)
			if len(y) == 0 {
				log.Printf("worker: %s missing mapping", p.ID)
				_ = r.Store.Release(ctx, p.ID, r.WorkerID)
				continue
			}
			mp, err := schema.ParseMapping(y)
			if err != nil {
				log.Printf("worker: parse mapping %s: %v", p.ID, err)
				_ = r.Store.Release(ctx, p.ID, r.WorkerID)
				continue
			}
			obs := &logObserver{pipelineID: p.ID}
			pl, err := pipeline.NewWithObserver(cfg, mp, obs)
			if err != nil {
				log.Printf("worker: init pipeline %s: %v", p.ID, err)
				_ = r.Store.Release(ctx, p.ID, r.WorkerID)
				continue
			}
			runCtx, cancel := context.WithCancel(ctx)
			r.mu.Lock()
			r.running[p.ID] = cancel
			r.mu.Unlock()
			go func(pid string) {
				obs.OnStart()
				err := pl.Run(runCtx)
				if err != nil {
					log.Printf("worker: pipeline %s error: %v", pid, err)
					obs.OnError(err)
				}
				obs.OnStop()
				r.mu.Lock()
				delete(r.running, pid)
				r.mu.Unlock()
				_ = r.Store.Release(context.Background(), pid, r.WorkerID)
			}(p.ID)
			_ = now
		} else {
			if isRunning {
				r.mu.Lock()
				cancel := r.running[p.ID]
				delete(r.running, p.ID)
				r.mu.Unlock()
				cancel()
				_ = r.Store.Release(ctx, p.ID, r.WorkerID)
			}
		}
	}
	return nil
}

type logObserver struct {
	pipelineID string
}

func (l *logObserver) OnStart()          { log.Printf("pipeline %s: started", l.pipelineID) }
func (l *logObserver) OnError(err error) { log.Printf("pipeline %s: error: %v", l.pipelineID, err) }
func (l *logObserver) OnStop()           { log.Printf("pipeline %s: stopped", l.pipelineID) }
func (l *logObserver) OnBatchInserted(n int, total int64, at time.Time) {
	log.Printf("pipeline %s: batch=%d total=%d at=%s", l.pipelineID, n, total, at.Format(time.RFC3339))
}

// DefaultWorkerID attempts to form a stable worker id.
func DefaultWorkerID() string {
	host, _ := os.Hostname()
	return host + ":" + os.Getenv("PORT")
}
