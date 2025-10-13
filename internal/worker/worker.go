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
	running map[string]*runningPipeline // pipelineID -> runningPipeline
}

func NewRunner(st store.PipelineStore, workerID string, reconcileEvery, leaseTTL time.Duration) *Runner {
	if reconcileEvery <= 0 {
		reconcileEvery = 5 * time.Second
	}
	if leaseTTL <= 0 {
		leaseTTL = 20 * time.Second
	}
	return &Runner{Store: st, WorkerID: workerID, ReconcileEvery: reconcileEvery, LeaseTTL: leaseTTL, running: map[string]*runningPipeline{}}
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
	for id, rp := range r.running {
		for slot, cancel := range rp.slots {
			cancel()
			_ = r.Store.ReleaseSlot(context.Background(), id, r.WorkerID, slot)
		}
		delete(r.running, id)
	}
}

func (r *Runner) reconcileOnce(ctx context.Context) error {
	pls, err := r.Store.ListPipelines(ctx)
	if err != nil {
		return err
	}
	// reconcile current desired state and owned slots
	for _, p := range pls {
		st, err := r.Store.GetState(ctx, p.ID)
		if err != nil {
			log.Printf("worker: get state %s: %v", p.ID, err)
			continue
		}
		desiredStart := st != nil && st.Desired == store.DesiredStarted && st.Replicas > 0
		r.mu.Lock()
		rp := r.running[p.ID]
		isRunning := rp != nil && len(rp.slots) > 0
		r.mu.Unlock()

		if desiredStart {
			// Ensure structure
			if rp == nil {
				rp = &runningPipeline{slots: map[int]context.CancelFunc{}}
				r.mu.Lock()
				r.running[p.ID] = rp
				r.mu.Unlock()
			}
			// Try acquire a new slot if we have capacity
			slot, ok, err := r.Store.TryAcquireSlot(ctx, p.ID, r.WorkerID, r.LeaseTTL)
			if err != nil {
				log.Printf("worker: try acquire slot %s: %v", p.ID, err)
			} else if ok {
				// start a member for this slot
				kcfg, _ := r.Store.GetKafkaConfig(ctx, p.ID)
				hcfg, _ := r.Store.GetClickHouseConfig(ctx, p.ID)
				if kcfg == nil || hcfg == nil {
					log.Printf("worker: %s missing configs", p.ID)
					_ = r.Store.ReleaseSlot(ctx, p.ID, r.WorkerID, slot)
				} else {
					cfg := &config.Config{Kafka: *kcfg, ClickHouse: *hcfg}
					y, _ := r.Store.GetMappingYAML(ctx, p.ID)
					if len(y) == 0 {
						log.Printf("worker: %s missing mapping", p.ID)
						_ = r.Store.ReleaseSlot(ctx, p.ID, r.WorkerID, slot)
					} else if mp, err := schema.ParseMapping(y); err != nil {
						log.Printf("worker: parse mapping %s: %v", p.ID, err)
						_ = r.Store.ReleaseSlot(ctx, p.ID, r.WorkerID, slot)
					} else {
						obs := &logObserver{pipelineID: p.ID}
						pl, err := pipeline.NewWithObserver(cfg, mp, obs)
						if err != nil {
							log.Printf("worker: init pipeline %s: %v", p.ID, err)
							_ = r.Store.ReleaseSlot(ctx, p.ID, r.WorkerID, slot)
						} else {
							runCtx, cancel := context.WithCancel(ctx)
							r.mu.Lock()
							rp.slots[slot] = cancel
							r.mu.Unlock()
							go func(pid string, sl int) {
								obs.OnStart()
								err := pl.Run(runCtx)
								if err != nil {
									log.Printf("worker: pipeline %s slot %d error: %v", pid, sl, err)
									obs.OnError(err)
								}
								obs.OnStop()
								r.mu.Lock()
								delete(rp.slots, sl)
								r.mu.Unlock()
								_ = r.Store.ReleaseSlot(context.Background(), pid, r.WorkerID, sl)
							}(p.ID, slot)
						}
					}
				}
			}
			// Renew all slots we own for this pipeline
			r.mu.Lock()
			slots := make([]int, 0, len(rp.slots))
			for sl := range rp.slots {
				slots = append(slots, sl)
			}
			r.mu.Unlock()
			if len(slots) > 0 {
				_ = r.Store.RenewSlots(ctx, p.ID, r.WorkerID, slots, r.LeaseTTL)
			}
		} else {
			if isRunning {
				// stop all owned slots
				r.mu.Lock()
				for sl, cancel := range rp.slots {
					cancel()
					_ = r.Store.ReleaseSlot(ctx, p.ID, r.WorkerID, sl)
				}
				delete(r.running, p.ID)
				r.mu.Unlock()
			}
		}
	}
	return nil
}

type runningPipeline struct{ slots map[int]context.CancelFunc }

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

// no-op client id customization; sarama uses GroupID for coordination
