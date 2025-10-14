# Architecture

This document explains how the system is structured, how pipelines run, and how scaling works with replicas, slots, and leases. It also contrasts the lease-based design with a simpler “no-leases” mode so you can choose the right operational model.

## High-level overview

- UI/API server (Go):
  - CRUD for pipelines (name/description)
  - Manage Kafka and ClickHouse configs separately
  - Mapping: infer from samples and save schema mapping
  - Desired state: set started/stopped and replicas
  - Assignments: inspect which workers currently hold replica slots
- Worker (Go):
  - Periodically reconciles desired state from the store
  - In lease mode: acquires/renews slot leases and runs one pipeline instance per acquired slot
  - In no-leases mode: simply starts one local instance for each started pipeline
- Store (Postgres preferred):
  - Persistence for pipelines, configs, mapping, desired state
  - Coordination via a leases table (slot-based) for replicas
- Kafka: Sarama consumer group; partition assignment is handled by Kafka once group members are running
- ClickHouse: Writes batches to a MergeTree table with basic type coercions and rate limiting

## Key concepts

- Pipeline: A unit that consumes from one Kafka topic and writes to one ClickHouse table. Identified by `pipelines.id`.
- Desired state: What the operator wants the pipeline to do.
  - desired ∈ { started, stopped }
  - replicas ∈ ℤ, replicas ≥ 1
- Replica: A logical unit of parallelism for a pipeline. If `replicas = R`, we want up to R concurrent process “members” for that pipeline across the cluster.
- Slot: An index in [0..R-1] for a given pipeline that represents one replica seat.
- Assignment / Lease: A record in Postgres that says “worker W owns slot S for pipeline P until time T”. This is used to coordinate who runs which replica.

## Data model (Postgres)

- pipelines(id, name, description, created_at, updated_at)
- kafka_configs(pipeline_id, yaml)
- clickhouse_configs(pipeline_id, yaml)
- mappings(pipeline_id, yaml)
- pipeline_state(pipeline_id, desired, replicas, updated_at)
- leases(pipeline_id, slot, worker_id, lease_until, PRIMARY KEY(pipeline_id, slot))

Notes:

- `pipeline_state.replicas` is the desired replica count.
- `leases` has one row per active slot assignment.

## Control loop (worker)

Periodically (every reconcile interval):

- Fetch pipelines and their `pipeline_state`.
- For each pipeline:
  - If desired = stopped: stop any local instances; release owned leases (in lease mode).
  - If desired = started:
    - In lease mode:
      - TryAcquireSlot(pipeline_id, worker_id, ttl): serializable tx chooses the first free slot in [0..replicas-1] and inserts a lease row with `lease_until = now() + ttl`.
      - If acquired, start one pipeline instance for that slot.
      - RenewSlots for all locally owned slots to push `lease_until` forward.
    - In no-leases mode:
      - If not already running locally, start one pipeline instance (one per worker per pipeline).

## Lease lifecycle (slot-based)

- Acquire:
  - SERIALIZABLE transaction:
    - Read desired replicas from `pipeline_state`.
    - Lock current active leases (`FOR UPDATE`).
    - Pick the first free slot in [0..replicas-1]. If none, exit.
    - Insert `leases(pipeline_id, slot, worker_id, lease_until)`.
- Renew:
  - Update `lease_until = now() + ttl` for all slots owned by the worker.
- Release:
  - Delete the lease row when the worker stops that slot.
- Expire:
  - If a worker crashes or network partitions occur, other workers will be able to acquire the slot once `lease_until < now()`.

Tuning:

- `leaseTTL` (default ~20s): how long a lease remains valid without renewal.
- `reconcileEvery` (default ~5s): how often workers attempt to acquire/renew.
- Keep `leaseTTL` > `reconcileEvery` by a comfortable margin. Too small TTL can cause churn; too large delays failover.

## Kafka consumer groups and replicas

- Each running instance joins the Kafka consumer group (specified in pipeline’s Kafka config: `groupId`).
- Kafka coordinates partition assignment across all active members. Our slot leases only control “how many members exist” clusterwide; Kafka still handles which partitions go where.
- Scaling replicas up increases the number of group members; scaling down removes members and Kafka rebalances.

## Scenarios (with leases)

1. Single worker, replicas=1

- One slot (0) is acquired by the worker; one instance runs.

2. Multiple workers, replicas=1

- Only one worker acquires slot 0; the others see no free slots and do nothing. Exactly one instance runs.

3. Multiple workers, replicas=R>1

- Up to R slots are acquired across workers; up to R instances run concurrently.
- Work is automatically spread by Kafka’s group rebalancing.

4. Scale up (replicas: 1 → 3)

- Next reconcile, more slots become available [1,2]. Workers acquire them and start more instances until three are running.

5. Scale down (replicas: 3 → 1)

- On reconcile, there are extra leases for slots ≥ 1. Workers will naturally fail to renew those if you implement a graceful policy; in this implementation, new acquisitions stop once the target is lower. Existing slots will eventually be given up by stopping instances (you can force a stop by toggling desired=stopped then started).

6. Worker crash

- The crashed worker won’t renew; after `lease_until` passes, another worker acquires that slot and restarts the instance.

7. Store outage (Postgres unavailable)

- Acquire/renew operations fail; existing running instances may continue but leases won’t renew and will expire. When Postgres returns, workers will compete to reacquire slots.

8. Multiple pipelines

- The algorithm is per-pipeline. Different pipelines are independent and can be owned by different workers simultaneously.

## No-leases mode (simple mode)

- How it works:
  - Each worker starts one local instance per started pipeline, ignoring `replicas` and leases.
  - Effective parallelism becomes the number of workers, not the configured replicas.
- Pros:
  - Simpler mental model; no DB-based coordination.
  - Useful in small or homogeneous environments where “one per node” is intended.
- Cons:
  - No upper bound control via replicas; if you run 5 workers, you get 5 instances.
  - Failover is implicit (start another worker), not coordinated.
  - Cluster changes (adding/removing workers) immediately change parallelism.
- When to use:
  - Single-node deployments.
  - Dev/test.
  - Intentional “one-per-node” topology without fine-grained replica control.

## UI and API

- Set desired state and replicas:
  - `GET|PUT|PATCH /api/pipelines/{id}/state`
- Inspect assignments (lease holders):
  - `GET /api/pipelines/{id}/assignments`
- Run page shows:
  - Current status metrics (batches/rows)
  - Desired state + replicas editor
  - Active assignments (slot → worker, leaseUntil)

## Failure modes and edge cases

- Clock skew: lease times are based on DB/server clocks. Keep clocks synced (NTP).
- Thundering herd: many workers might attempt acquisition at once; SERIALIZABLE tx with small scope reduces conflicts.
- Hot pipelines: increase `reconcileEvery` or randomize small jitter if needed to spread contention.
- TTL too small: frequent expirations and slot churn.
- TTL too large: slower failover.

## Vertical concurrency (future refinements)

- Today: a worker can acquire multiple slots; each slot maps to one process member.
- Planned: `replicas-per-instance` to cap how many slots a single worker can own per pipeline (for fairness/capacity).
- Optional: within one member, support multiple internal threads if ClickHouse/CPU can benefit.

## FS store vs. Postgres store

- FS store is best-effort/local and not suitable for multi-node coordination. It implements stub slot semantics for local dev compatibility only.
- Postgres store is the recommended backend for distributed coordination and persistence.

## Choosing between leases and no-leases

- Choose leases if you need:
  - Controlled parallelism (honor `replicas` exactly)
  - Predictable failover and stable cluster behavior
  - Multiple workers with bounded number of instances
- Choose no-leases if you need:
  - Simplicity and you’re fine with “one instance per worker per pipeline”
  - Single-node or small deployments where replica control isn’t critical

## Operational tips

- Start UI with `--store=pg` and run Postgres via docker-compose.
- Start workers:
  - Coordinated mode: `click-sink worker --store=pg --pg-dsn <dsn>`
  - No-leases mode: `click-sink worker --store=pg --pg-dsn <dsn> --no-leases`
- Set `replicas` via the UI when in coordinated mode; leave it at 1 for most topics unless you need higher throughput.
- Monitor assignments to ensure the expected number of members are active.

## Security and idempotency notes

- Kafka consumer group ensures at-least-once delivery; duplicates can occur on retries or rebalances.
- ClickHouse inserts are not deduplicated by default in this project. If duplicates matter, consider a primary key with ReplacingMergeTree or add an idempotency key in the mapping.

---

If you’d like, we can add a small banner in the UI indicating whether the worker is running in lease or no-leases mode, and disable the replicas editor when no-leases is enabled to reduce confusion.
