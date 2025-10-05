const BASE = process.env.NEXT_PUBLIC_API_BASE || "http://localhost:8081";

async function json<T>(path: string, init?: RequestInit): Promise<T> {
  const r = await fetch(`${BASE}${path}`, {
    ...init,
    headers: { "Content-Type": "application/json", ...(init?.headers || {}) },
    cache: "no-store",
  });
  if (!r.ok) throw new Error(await r.text());
  return r.json();
}

export type Status = {
  running: boolean;
  started: string;
  lastErr: string;
  totalRows: number;
  lastBatch: number;
  lastBatchAt: string;
};

export type Config = any;
export type Mapping = {
  columns: { fieldPath: string; column: string; type: string }[];
};

export const api = {
  status: () => json<Status>("/api/status"),
  getConfig: () => json<Config>("/api/config"),
  saveConfig: (cfg: Config) =>
    json("/api/config", { method: "POST", body: JSON.stringify(cfg) }),
  getMapping: () => json<Mapping>("/api/mapping"),
  saveMapping: (m: Mapping) =>
    json("/api/mapping", { method: "POST", body: JSON.stringify(m) }),
  sample: (limit = 100) =>
    json<{ fieldPath: string; column: string; type: string }[]>(
      `/api/sample?limit=${limit}`
    ),
  start: () => json("/api/start", { method: "POST" }),
  stop: () => json("/api/stop", { method: "POST" }),
};
