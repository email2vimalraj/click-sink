import { useRouter } from "next/router";
import { useEffect, useState } from "react";
import Link from "next/link";
import { api } from "../../../lib/api";

type KafkaCfg = any;
type ClickHouseCfg = any;

function normalizeKafka(c: KafkaCfg) {
  return {
    brokers: c?.brokers || c?.Brokers || [],
    topic: c?.topic || c?.Topic || "",
    groupID: c?.groupID || c?.groupId || c?.GroupID || "",
    securityProtocol: c?.securityProtocol || c?.SecurityProtocol || "",
    saslUsername: c?.saslUsername || c?.SASLUsername || "",
    saslPassword: c?.saslPassword || c?.SASLPassword || "",
    saslMechanism: c?.saslMechanism || c?.SASLMechanism || "",
  };
}

function normalizeCH(c: ClickHouseCfg) {
  return {
    dsn: c?.dsn || c?.DSN || "",
    database: c?.database || c?.Database || "",
    table: c?.table || c?.Table || "",
    batchSize: c?.batchSize || c?.BatchSize || 0,
    batchFlushInterval: c?.batchFlushInterval || c?.BatchFlushInterval || "",
    insertRatePerSec: c?.insertRatePerSec || c?.InsertRatePerSec || 0,
  };
}

export default function PipelineConfig() {
  const router = useRouter();
  const { id } = router.query;
  const [meta, setMeta] = useState<any>(null);
  const [kafkaCfg, setKafkaCfg] = useState<KafkaCfg | null>(null);
  const [chCfg, setChCfg] = useState<ClickHouseCfg | null>(null);
  const [err, setErr] = useState<string | undefined>();

  useEffect(() => {
    if (typeof id === "string") {
      api
        .getPipeline(id)
        .then(setMeta)
        .catch(() => {});
      api
        .getKafkaConfig(id)
        .then(setKafkaCfg)
        .catch(() => setKafkaCfg(null));
      api
        .getClickHouseConfig(id)
        .then(setChCfg)
        .catch(() => setChCfg(null));
    }
  }, [id]);

  const k = normalizeKafka(kafkaCfg || {});
  const h = normalizeCH(chCfg || {});
  const hasKafka = (k.brokers && k.brokers.length > 0) || k.topic;
  const hasCH = !!(h.dsn || h.database || h.table);

  const rename = async () => {
    if (typeof id !== "string") return;
    const name = prompt("New name", meta?.name || "");
    if (!name) return;
    await api.updatePipeline(id, { name });
    const m = await api.getPipeline(id);
    setMeta(m);
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
            href={`/pipelines/${id}/kafka`}
          >
            Kafka
          </Link>
          <span className="mx-2 text-slate-300">|</span>
          <Link
            className="text-indigo-600 hover:underline"
            href={`/pipelines/${id}/clickhouse`}
          >
            ClickHouse
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
            <div className="flex items-center justify-between">
              <h2>Kafka</h2>
              <Link
                className="text-indigo-600 hover:underline"
                href={`/pipelines/${id}/kafka`}
              >
                Edit
              </Link>
            </div>
            {hasKafka ? (
              <div className="rounded-md border border-slate-200 bg-white p-3 text-sm">
                <div>
                  <strong>Brokers:</strong>{" "}
                  {(k.brokers || []).join(", ") || "-"}
                </div>
                <div>
                  <strong>Topic:</strong> {k.topic || "-"}
                </div>
                <div>
                  <strong>GroupID:</strong> {k.groupID || "-"}
                </div>
                {k.securityProtocol && (
                  <div>
                    <strong>Security:</strong> {k.securityProtocol}
                  </div>
                )}
                {(k.saslUsername || k.saslMechanism) && (
                  <div>
                    <strong>SASL:</strong> {k.saslUsername || "(user unset)"}{" "}
                    {k.saslMechanism ? `(${k.saslMechanism})` : ""}
                  </div>
                )}
              </div>
            ) : (
              <p className="text-sm text-slate-600">
                No Kafka configuration yet. Go to the Kafka page to add it.
              </p>
            )}
          </div>
          <div className="space-y-2">
            <div className="flex items-center justify-between">
              <h2>ClickHouse</h2>
              <Link
                className="text-indigo-600 hover:underline"
                href={`/pipelines/${id}/clickhouse`}
              >
                Edit
              </Link>
            </div>
            {hasCH ? (
              <div className="rounded-md border border-slate-200 bg-white p-3 text-sm">
                <div>
                  <strong>DSN:</strong> {h.dsn || "-"}
                </div>
                <div>
                  <strong>Database:</strong> {h.database || "-"}
                </div>
                <div>
                  <strong>Table:</strong> {h.table || "-"}
                </div>
                <div>
                  <strong>Batch Size:</strong> {h.batchSize || 0}
                </div>
                <div>
                  <strong>Flush Interval:</strong> {h.batchFlushInterval || "-"}
                </div>
                <div>
                  <strong>Insert Rate/sec:</strong> {h.insertRatePerSec || 0}
                </div>
              </div>
            ) : (
              <p className="text-sm text-slate-600">
                No ClickHouse configuration yet. Go to the ClickHouse page to
                add it.
              </p>
            )}
          </div>
        </div>
      </div>
    </main>
  );
}
