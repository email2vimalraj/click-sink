package worker

import (
	"context"
	"log"
	"math/rand"
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
	// When true, skip slot leases entirely and start one local instance per started pipeline.
	DisableLeases bool
	// Optional: cap how many slots this worker can own per pipeline (0 = unlimited)
	MaxSlotsPerPipeline int

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
	log.Printf("worker: starting id=%s mode=%s maxSlotsPerPipeline=%d reconcileEvery=%s leaseTTL=%s", r.WorkerID, func() string {
		if r.DisableLeases {
			return "no-leases"
		}
		return "leases"
	}(), r.MaxSlotsPerPipeline, r.ReconcileEvery.String(), r.LeaseTTL.String())
	ticker := time.NewTicker(r.ReconcileEvery)
	defer ticker.Stop()
	for {
		// Heartbeat to store for visibility
		mode := "leases"
		if r.DisableLeases {
			mode = "no-leases"
		}
		_ = r.Store.UpsertWorkerHeartbeat(ctx, r.WorkerID, mode, "dev")
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
			if !r.DisableLeases {
				_ = r.Store.ReleaseSlot(context.Background(), id, r.WorkerID, slot)
			}
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
			if r.DisableLeases {
				// simple mode: if not already running locally, start one instance
				r.mu.Lock()
				_, have := rp.slots[0]
				r.mu.Unlock()
				if !have {
					kcfg, _ := r.Store.GetKafkaConfig(ctx, p.ID)
					hcfg, _ := r.Store.GetClickHouseConfig(ctx, p.ID)
					if kcfg == nil || hcfg == nil {
						log.Printf("worker: %s missing configs", p.ID)
					} else {
						cfg := &config.Config{Kafka: *kcfg, ClickHouse: *hcfg}
						y, _ := r.Store.GetMappingYAML(ctx, p.ID)
						if len(y) == 0 {
							log.Printf("worker: %s missing mapping", p.ID)
						} else if mp, err := schema.ParseMapping(y); err != nil {
							log.Printf("worker: parse mapping %s: %v", p.ID, err)
						} else {
							obs := &logObserver{pipelineID: p.ID}
							pl, err := pipeline.NewWithObserver(cfg, mp, obs)
							if err != nil {
								log.Printf("worker: init pipeline %s: %v", p.ID, err)
							} else {
								// record consumer group partition assignments for UI
								pl.WithClaimObserver(&claimRecorder{store: r.Store, pipelineID: p.ID, workerID: r.WorkerID})
								runCtx, cancel := context.WithCancel(ctx)
								r.mu.Lock()
								rp.slots[0] = cancel
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
									delete(rp.slots, 0)
									r.mu.Unlock()
								}(p.ID)
							}
						}
					}
				}
			} else {
				// Try acquire a new slot if we have capacity
				if r.MaxSlotsPerPipeline > 0 {
					r.mu.Lock()
					if len(rp.slots) >= r.MaxSlotsPerPipeline {
						r.mu.Unlock()
						// Already at cap; just renew below
					} else {
						r.mu.Unlock()
						// Double-check at the store level to avoid race where local map isn't updated yet
						skipAcquire := false
						if as, err := r.Store.ListAssignments(ctx, p.ID); err == nil {
							owned := 0
							now := time.Now()
							for _, a := range as {
								if a.WorkerID == r.WorkerID && a.LeaseUntil.After(now) {
									owned++
								}
							}
							if owned >= r.MaxSlotsPerPipeline {
								skipAcquire = true
							}
						}
						if !skipAcquire {
							// tiny jitter to reduce thundering herd and improve fairness
							time.Sleep(time.Duration(rand.Intn(300)) * time.Millisecond)
							slot, ok, err := r.Store.TryAcquireSlot(ctx, p.ID, r.WorkerID, r.LeaseTTL)
							if err != nil {
								log.Printf("worker: try acquire slot %s: %v", p.ID, err)
							} else if ok {
								log.Printf("worker: acquired slot %d for pipeline %s", slot, p.ID)
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
											// record consumer group partition assignments for UI
											pl.WithClaimObserver(&claimRecorder{store: r.Store, pipelineID: p.ID, workerID: r.WorkerID})
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
						}
					}
				} else {
					time.Sleep(time.Duration(rand.Intn(300)) * time.Millisecond)
					slot, ok, err := r.Store.TryAcquireSlot(ctx, p.ID, r.WorkerID, r.LeaseTTL)
					if err != nil {
						log.Printf("worker: try acquire slot %s: %v", p.ID, err)
					} else if ok {
						log.Printf("worker: acquired slot %d for pipeline %s", slot, p.ID)
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
									// record consumer group partition assignments for UI
									pl.WithClaimObserver(&claimRecorder{store: r.Store, pipelineID: p.ID, workerID: r.WorkerID})
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
				}
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
								// record consumer group partition assignments for UI
								pl.WithClaimObserver(&claimRecorder{store: r.Store, pipelineID: p.ID, workerID: r.WorkerID})
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

				// Graceful scale-down: if desired replicas decreased, stop extra slots (highest first)
				if st != nil && st.Replicas > 0 {
					// Determine extra slots (> replicas-1)
					r.mu.Lock()
					extra := []int{}
					for sl := range rp.slots {
						if sl >= st.Replicas {
							extra = append(extra, sl)
						}
					}
					r.mu.Unlock()
					if len(extra) > 0 {
						// Sort descending
						// small inline sort to avoid import; bubble for tiny N
						for i := 0; i < len(extra); i++ {
							for j := i + 1; j < len(extra); j++ {
								if extra[i] < extra[j] {
									extra[i], extra[j] = extra[j], extra[i]
								}
							}
						}
						for _, sl := range extra {
							r.mu.Lock()
							cancel := rp.slots[sl]
							delete(rp.slots, sl)
							r.mu.Unlock()
							if cancel != nil {
								cancel()
							}
							_ = r.Store.ReleaseSlot(ctx, p.ID, r.WorkerID, sl)
							log.Printf("worker: pipeline %s released extra slot %d due to scale down", p.ID, sl)
						}
					}
				}

				// Enforce per-worker cap locally: if more than allowed, release highest-indexed extras
				if r.MaxSlotsPerPipeline > 0 {
					r.mu.Lock()
					local := make([]int, 0, len(rp.slots))
					for sl := range rp.slots {
						local = append(local, sl)
					}
					r.mu.Unlock()
					if len(local) > r.MaxSlotsPerPipeline {
						// sort desc
						for i := 0; i < len(local); i++ {
							for j := i + 1; j < len(local); j++ {
								if local[i] < local[j] {
									local[i], local[j] = local[j], local[i]
								}
							}
						}
						for idx := r.MaxSlotsPerPipeline; idx < len(local); idx++ {
							sl := local[idx]
							r.mu.Lock()
							cancel := rp.slots[sl]
							delete(rp.slots, sl)
							r.mu.Unlock()
							if cancel != nil {
								cancel()
							}
							_ = r.Store.ReleaseSlot(ctx, p.ID, r.WorkerID, sl)
							log.Printf("worker: pipeline %s released slot %d to enforce max-slots-per-pipeline=%d", p.ID, sl, r.MaxSlotsPerPipeline)
						}
					}
				}
			}
		} else {
			if isRunning {
				// stop all owned slots
				r.mu.Lock()
				for sl, cancel := range rp.slots {
					cancel()
					if !r.DisableLeases {
						_ = r.Store.ReleaseSlot(ctx, p.ID, r.WorkerID, sl)
					}
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

// claimRecorder writes partition claim info into the store for UI consumption
type claimRecorder struct {
	store      store.PipelineStore
	pipelineID string
	workerID   string
}

func (c *claimRecorder) OnPartitionAssigned(groupID, clientID, topic string, partition int32) {
	_ = c.store.UpsertClaim(context.Background(), c.pipelineID, c.workerID, topic, int(partition))
}
func (c *claimRecorder) OnPartitionReleased(groupID, clientID, topic string, partition int32) {
	_ = c.store.RemoveClaim(context.Background(), c.pipelineID, c.workerID, topic, int(partition))
}

// DefaultWorkerID attempts to form a stable worker id.
func DefaultWorkerID() string {
	host, _ := os.Hostname()
	return host + ":" + os.Getenv("PORT")
}

// no-op client id customization; sarama uses GroupID for coordination
