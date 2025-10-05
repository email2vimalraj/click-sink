import { useRouter } from "next/router";
import { useEffect, useState } from "react";
import Link from "next/link";
import { api, Config } from "../../../lib/api";

export default function PipelineConfig() {
  const router = useRouter();
  const { id } = router.query;
  const [cfg, setCfg] = useState<Config>({ kafka: {}, clickHouse: {} });
  const [err, setErr] = useState<string | undefined>();
  useEffect(() => {
    if (typeof id === "string")
      api
        .getPipelineConfig(id)
        .then(setCfg)
        .catch((e) => setErr(String(e)));
  }, [id]);
  const save = async () => {
    if (typeof id !== "string") return;
    try {
      await api.savePipelineConfig(id, cfg);
      alert("Saved");
    } catch (e: any) {
      alert(String(e));
    }
  };
  return (
    <main style={{ padding: 24 }}>
      <p>
        <Link href={`/pipelines`}>← Pipelines</Link> |{" "}
        <Link href={`/pipelines/${id}/mapping`}>Mapping</Link> |{" "}
        <Link href={`/pipelines/${id}/run`}>Run</Link>
      </p>
      <h1>Pipeline {id} - Config</h1>
      {err && <p style={{ color: "red" }}>{err}</p>}
      <h2>Kafka</h2>
      <input
        placeholder="brokers a,b"
        style={{ display: "block" }}
        value={(cfg.kafka?.brokers || []).join(",")}
        onChange={(e) =>
          setCfg({
            ...cfg,
            kafka: {
              ...cfg.kafka,
              brokers: e.target.value
                .split(",")
                .map((s: string) => s.trim())
                .filter(Boolean),
            },
          })
        }
      />
      <input
        placeholder="topic"
        style={{ display: "block" }}
        value={cfg.kafka?.topic || ""}
        onChange={(e) =>
          setCfg({ ...cfg, kafka: { ...cfg.kafka, topic: e.target.value } })
        }
      />
      <input
        placeholder="groupID"
        style={{ display: "block" }}
        value={cfg.kafka?.groupID || ""}
        onChange={(e) =>
          setCfg({ ...cfg, kafka: { ...cfg.kafka, groupID: e.target.value } })
        }
      />
      <h2>ClickHouse</h2>
      <input
        placeholder="dsn"
        style={{ display: "block" }}
        value={cfg.clickHouse?.dsn || ""}
        onChange={(e) =>
          setCfg({
            ...cfg,
            clickHouse: { ...cfg.clickHouse, dsn: e.target.value },
          })
        }
      />
      <input
        placeholder="database"
        style={{ display: "block" }}
        value={cfg.clickHouse?.database || ""}
        onChange={(e) =>
          setCfg({
            ...cfg,
            clickHouse: { ...cfg.clickHouse, database: e.target.value },
          })
        }
      />
      <input
        placeholder="table"
        style={{ display: "block" }}
        value={cfg.clickHouse?.table || ""}
        onChange={(e) =>
          setCfg({
            ...cfg,
            clickHouse: { ...cfg.clickHouse, table: e.target.value },
          })
        }
      />
      <input
        placeholder="batch size"
        type="number"
        style={{ display: "block" }}
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
      <input
        placeholder="flush interval"
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
      <input
        placeholder="rate/sec"
        type="number"
        style={{ display: "block" }}
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
        <button onClick={save}>Save</button>
      </div>
    </main>
  );
}
