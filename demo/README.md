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

## Test Kafka over SSL (TLS)

An optional override adds a TLS-enabled Kafka broker (`kafka-ssl`) using Redpanda with a self-signed CA generated at runtime.

Start the stack with SSL override:

```bash
cd demo
docker compose -f docker-compose.yml -f docker-compose.ssl.yml up -d
```

Retrieve the CA certificate (to paste into the UI’s CA field):

```bash
# Show the CA PEM in the terminal
docker compose -f docker-compose.yml -f docker-compose.ssl.yml exec tls-gen cat /certs/ca.crt

# Or copy it to your host
docker compose -f docker-compose.yml -f docker-compose.ssl.yml cp tls-gen:/certs/ca.crt ./ca.crt
```

In the UI (Pipeline → Kafka):

- Brokers: `kafka-ssl:9094`
- Security Protocol: `SSL`
- TLS Server Name (SNI): `kafka-ssl`
- CA Certificate (PEM): paste the contents of `ca.crt`
- Leave Client Cert/Key empty (server-auth only)

Click “Save” then “Validate”. You should see “Kafka connectivity OK”. You can also use “Sample & Infer” to verify end-to-end over TLS.

Notes:

- For a quick smoke test without handling the CA, you can check “InsecureSkipVerify” in the UI. This skips certificate validation (not recommended beyond local testing).
- To test mTLS (client authentication), extend the `tls-gen` step to generate a client certificate and set `require_client_auth=true`, then paste the client cert/key in the UI.
