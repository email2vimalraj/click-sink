import { useRouter } from "next/router";
import { useEffect, useState } from "react";
import Link from "next/link";
import { api } from "../../../lib/api";

export default function PipelineClickHouse() {
  const router = useRouter();
  const { id } = router.query;
  const [cfg, setCfg] = useState<any>({
    dsn: "",
    database: "",
    table: "",
    batchSize: 1000,
    batchFlushInterval: "1s",
    insertRatePerSec: 0,
  });
  const [status, setStatus] = useState<string>("");
  const [err, setErr] = useState<string | undefined>();
  const [exists, setExists] = useState<boolean | null>(null);
  const [mappingCols, setMappingCols] = useState<
    { fieldPath: string; column: string; type: string }[]
  >([]);

  useEffect(() => {
    if (typeof id === "string") {
      api
        .getClickHouseConfig(id)
        .then(setCfg)
        .catch(() => {});
      api
        .getPipelineMapping(id)
        .then((m) => setMappingCols(m?.columns || []))
        .catch(() => setMappingCols([]));
    }
  }, [id]);

  const save = async () => {
    if (typeof id !== "string") return;
    try {
      await api.saveClickHouseConfig(id, cfg);
      setStatus("Saved");
    } catch (e: any) {
      setErr(String(e));
    }
  };

  const validate = async () => {
    if (typeof id !== "string") return;
    setStatus("Validating...");
    setErr(undefined);
    try {
      await api.validateClickHouse(id as string);
      setStatus("ClickHouse connectivity OK");
    } catch (e: any) {
      setErr(String(e));
      setStatus("");
    }
  };

  const checkTable = async () => {
    if (typeof id !== "string") return;
    setStatus("Checking table...");
    setErr(undefined);
    try {
      const r: any = await api.validateClickHouseTable(id, {
        table: cfg.table || cfg.Table,
      });
      setExists(Boolean(r.exists));
      setStatus(r.exists ? "Table exists" : "Table not found");
    } catch (e: any) {
      setErr(String(e));
      setStatus("");
    }
  };

  // Removed: table creation is handled during Mapping stage.

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
            href={`/pipelines/${id}/kafka`}
          >
            Kafka
          </Link>
        </p>
        <h1>Pipeline {id} - ClickHouse</h1>
        {err && <p className="mb-2 text-sm text-red-600">{err}</p>}
        {status && <p className="mb-2 text-sm text-green-700">{status}</p>}
        <div className="grid grid-cols-1 gap-3">
          <input
            placeholder="dsn"
            value={cfg.dsn || cfg.DSN || ""}
            onChange={(e) => setCfg({ ...cfg, dsn: e.target.value })}
          />
          <input
            placeholder="database"
            value={cfg.database || cfg.Database || ""}
            onChange={(e) => setCfg({ ...cfg, database: e.target.value })}
          />
          <input
            placeholder="table"
            value={cfg.table || cfg.Table || ""}
            onChange={(e) => setCfg({ ...cfg, table: e.target.value })}
          />
          <input
            placeholder="batch size"
            type="number"
            value={cfg.batchSize || cfg.BatchSize || 0}
            onChange={(e) =>
              setCfg({ ...cfg, batchSize: Number(e.target.value) })
            }
          />
          <input
            placeholder="flush interval (e.g. 1s, 500ms)"
            value={cfg.batchFlushInterval || cfg.BatchFlushInterval || ""}
            onChange={(e) =>
              setCfg({ ...cfg, batchFlushInterval: e.target.value })
            }
          />
          <input
            placeholder="rate/sec"
            type="number"
            value={cfg.insertRatePerSec || cfg.InsertRatePerSec || 0}
            onChange={(e) =>
              setCfg({ ...cfg, insertRatePerSec: Number(e.target.value) })
            }
          />
        </div>
        <div className="mt-4 flex gap-2">
          <button onClick={save}>Save</button>
          <button
            className="bg-slate-200 text-slate-900 hover:bg-slate-300"
            onClick={validate}
          >
            Validate
          </button>
          <button onClick={checkTable}>Check Table</button>
          {/* <button onClick={createTable} disabled={exists === true}>
            Create Table
          </button> */}
        </div>
        <div className="mt-2 text-sm text-slate-600">
          Mapping columns loaded: <strong>{mappingCols.length}</strong>
          {mappingCols.length === 0 && (
            <>
              <span className="mx-2 text-slate-300">|</span>
              <Link
                className="text-indigo-600 hover:underline"
                href={`/pipelines/${id}/mapping`}
              >
                Define mapping
              </Link>
            </>
          )}
        </div>
      </div>
    </main>
  );
}
