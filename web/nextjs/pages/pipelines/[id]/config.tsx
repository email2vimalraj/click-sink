import { useRouter } from "next/router";
import { useEffect, useState } from "react";
import Link from "next/link";
import { api, Config } from "../../../lib/api";

export default function PipelineConfig() {
  const router = useRouter();
  const { id } = router.query;
  const [cfg, setCfg] = useState<Config>({ kafka: {}, clickHouse: {} });
  const [meta, setMeta] = useState<any>(null);
  const [err, setErr] = useState<string | undefined>();
  useEffect(() => {
    if (typeof id === "string") {
      api
        .getPipeline(id)
        .then(setMeta)
        .catch(() => {});
      api
        .getPipelineConfig(id)
        .then(setCfg)
        .catch((e) => setErr(String(e)));
    }
  }, [id]);

  const rename = async () => {
    if (typeof id !== "string") return;
    const name = prompt("New name", meta?.name || "");
    if (!name) return;
    await api.updatePipeline(id, { name });
    const m = await api.getPipeline(id);
    setMeta(m);
  };
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
    <main className="min-h-screen p-6">
      <div className="mx-auto max-w-4xl">
        <p className="mb-4 text-sm text-slate-600">
          <Link className="hover:underline" href={`/pipelines`}>
            ← Pipelines
          </Link>
          <span className="mx-2 text-slate-300">|</span>
          <Link
            className="text-indigo-600 hover:underline"
            href={`/pipelines/${id}/mapping`}
          >
            Mapping
          </Link>
          <span className="mx-2 text-slate-300">|</span>
          <Link
            className="text-indigo-600 hover:underline"
            href={`/pipelines/${id}/run`}
          >
            Run
          </Link>
        </p>
        <h1>Pipeline {id} - Config</h1>
        <div className="mb-4 flex items-center gap-2">
          <span>
            Name: <strong>{meta?.name || id}</strong>
          </span>
          <button
            className="bg-slate-200 text-slate-900 hover:bg-slate-300"
            onClick={rename}
          >
            Rename
          </button>
        </div>
        {err && <p className="mb-4 text-sm text-red-600">{err}</p>}
        <div className="grid grid-cols-1 gap-6 md:grid-cols-2">
          <div className="space-y-2">
            <h2>Kafka</h2>
            <input
              placeholder="brokers a,b"
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
              value={cfg.kafka?.topic || ""}
              onChange={(e) =>
                setCfg({
                  ...cfg,
                  kafka: { ...cfg.kafka, topic: e.target.value },
                })
              }
            />
            <input
              placeholder="groupID"
              value={cfg.kafka?.groupID || ""}
              onChange={(e) =>
                setCfg({
                  ...cfg,
                  kafka: { ...cfg.kafka, groupID: e.target.value },
                })
              }
            />
          </div>
          <div className="space-y-2">
            <h2>ClickHouse</h2>
            <input
              placeholder="dsn"
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
              placeholder="flush interval (e.g. 1s, 500ms)"
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
          </div>
        </div>
        <div className="mt-4">
          <button onClick={save}>Save</button>
        </div>
      </div>
    </main>
  );
}
