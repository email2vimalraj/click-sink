# click-sink Next.js UI

A minimal Next.js frontend for the Go-based click-sink API.

## Prereqs

- Node 18+

## Setup

```bash
cd web/nextjs
npm install
npm run dev
```

The app runs on http://localhost:3000 and calls the Go API at http://localhost:8081 by default.

You can override the API base:

```bash
export NEXT_PUBLIC_API_BASE=http://localhost:8081
npm run dev
```

## Pages

- /: Home
- /config: Configure Kafka and ClickHouse
- /mapping: Sample fields and edit mapping
- /pipeline: Start/stop and view status
