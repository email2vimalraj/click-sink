import { useEffect, useState } from "react";
import { api, Config } from "../lib/api";
import Link from "next/link";

export default function ConfigPage() {
  const [cfg, setCfg] = useState<Config>({ kafka: {}, clickHouse: {} });
  const [saving, setSaving] = useState(false);
  const [err, setErr] = useState<string | null>(null);
  useEffect(() => {
    api
      .getConfig()
      .then(setCfg)
      .catch((e) => setErr(String(e)));
  }, []);
  const save = async () => {
    setSaving(true);
    setErr(null);
    try {
      await api.saveConfig(cfg);
    } catch (e: any) {
      setErr(String(e));
    } finally {
      setSaving(false);
    }
  };
  return (
    <main style={{ padding: 24 }}>
      <p>
        <Link href="/">← Home</Link>
      </p>
      <h1>Config</h1>
      {err && <p style={{ color: "red" }}>{err}</p>}
      <h2>Kafka</h2>
      <label>Brokers</label>
      <input
        style={{ display: "block" }}
        value={(cfg.kafka?.brokers || []).join(",")}
        onChange={(e) =>
          setCfg({
            ...cfg,
            kafka: {
              ...cfg.kafka,
              brokers: e.target.value
                .split(",")
                .map((s) => s.trim())
                .filter(Boolean),
            },
          })
        }
      />
      <label>Topic</label>
      <input
        style={{ display: "block" }}
        value={cfg.kafka?.topic || ""}
        onChange={(e) =>
          setCfg({ ...cfg, kafka: { ...cfg.kafka, topic: e.target.value } })
        }
      />
      <label>GroupID</label>
      <input
        style={{ display: "block" }}
        value={cfg.kafka?.groupID || ""}
        onChange={(e) =>
          setCfg({ ...cfg, kafka: { ...cfg.kafka, groupID: e.target.value } })
        }
      />
      <h2>ClickHouse</h2>
      <label>DSN</label>
      <input
        style={{ display: "block" }}
        value={cfg.clickHouse?.dsn || ""}
        onChange={(e) =>
          setCfg({
            ...cfg,
            clickHouse: { ...cfg.clickHouse, dsn: e.target.value },
          })
        }
      />
      <label>Database</label>
      <input
        style={{ display: "block" }}
        value={cfg.clickHouse?.database || ""}
        onChange={(e) =>
          setCfg({
            ...cfg,
            clickHouse: { ...cfg.clickHouse, database: e.target.value },
          })
        }
      />
      <label>Table</label>
      <input
        style={{ display: "block" }}
        value={cfg.clickHouse?.table || ""}
        onChange={(e) =>
          setCfg({
            ...cfg,
            clickHouse: { ...cfg.clickHouse, table: e.target.value },
          })
        }
      />
      <label>Batch Size</label>
      <input
        style={{ display: "block" }}
        type="number"
        value={cfg.clickHouse?.batchSize || 0}
        onChange={(e) =>
          setCfg({
            ...cfg,
            clickHouse: {
              ...cfg.clickHouse,
              batchSize: Number(e.target.value),
            },
          })
        }
      />
      <label>Flush Interval (e.g. 2s)</label>
      <input
        style={{ display: "block" }}
        value={cfg.clickHouse?.batchFlushInterval || ""}
        onChange={(e) =>
          setCfg({
            ...cfg,
            clickHouse: {
              ...cfg.clickHouse,
              batchFlushInterval: e.target.value,
            },
          })
        }
      />
      <label>Insert Rate/sec</label>
      <input
        style={{ display: "block" }}
        type="number"
        value={cfg.clickHouse?.insertRatePerSec || 0}
        onChange={(e) =>
          setCfg({
            ...cfg,
            clickHouse: {
              ...cfg.clickHouse,
              insertRatePerSec: Number(e.target.value),
            },
          })
        }
      />
      <div style={{ marginTop: 12 }}>
        <button onClick={save} disabled={saving}>
          {saving ? "Saving..." : "Save"}
        </button>
      </div>
    </main>
  );
}
