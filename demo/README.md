# Click Sink Demo Stack

This docker-compose brings up a complete demo stack:

- Zookeeper + Kafka
- Redpanda Console (for Kafka UI)
- ClickHouse
- Postgres + pgAdmin
- click-sink API server
- click-sink Workers (scalable via replicas)
- click-sink UI server (reusing the API binary on a different port)
- Optional nginx proxy to route /api to API and everything else to UI

## Prereqs

- Docker and docker-compose installed

## Start

```bash
cd demo
docker compose up --build -d
```

It brings up:

- API: http://localhost:8081 (or via nginx at http://localhost/api)
- UI: http://localhost:8082 (or via nginx at http://localhost/)
- Redpanda Console: http://localhost:8080
- ClickHouse: native 9000, http 8123
- Postgres: 5432
- pgAdmin: http://localhost:5050 (login admin@example.com / admin)

## Scaling workers

```bash
docker compose up -d --scale click-sink-worker=3
```

## Config for click-sink

- The API and UI containers use Postgres store by default.
- Workers use Postgres store and will pick up desired state set via the API.

## Nginx proxy

- Edit `nginx.conf` if you need custom routing.

## Cleanup

```bash
docker compose down -v
```
