import { useRouter } from "next/router";
import { useEffect, useState } from "react";
import Link from "next/link";
import { api } from "../../../lib/api";

export default function PipelineKafka() {
  const router = useRouter();
  const { id } = router.query;
  const [cfg, setCfg] = useState<any>({ brokers: [], topic: "", groupID: "" });
  const [status, setStatus] = useState<string>("");
  const [err, setErr] = useState<string | undefined>();
  const [notice, setNotice] = useState<string | undefined>();
  const [sample, setSample] = useState<
    { fieldPath: string; column: string; type: string }[]
  >([]);

  useEffect(() => {
    if (typeof id === "string") {
      api
        .getKafkaConfig(id)
        .then(setCfg)
        .catch(() => {});
    }
  }, [id]);

  const normalize = () => ({
    brokers: cfg.brokers || cfg.Brokers || [],
    topic: cfg.topic || cfg.Topic || "",
    groupID: cfg.groupID || cfg.groupId || cfg.GroupID || "",
    securityProtocol: cfg.securityProtocol || cfg.SecurityProtocol || "",
    saslUsername: cfg.saslUsername || cfg.SASLUsername || "",
    saslPassword: cfg.saslPassword || cfg.SASLPassword || "",
    saslMechanism: cfg.saslMechanism || cfg.SASLMechanism || "",
  });

  const save = async () => {
    if (typeof id !== "string") return;
    try {
      await api.saveKafkaConfig(id, normalize());
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
      await api.validateKafka(id);
      setStatus("Kafka connectivity OK");
    } catch (e: any) {
      setErr(String(e));
      setStatus("");
    }
  };

  const infer = async () => {
    setStatus("Sampling...");
    setErr(undefined);
    setNotice(undefined);
    try {
      const res = await api.validateKafkaSample(normalize(), 100);
      setSample(res.fields);
      if (res.notice) setNotice(res.notice);
      setStatus(res.fields.length > 0 ? "Sampled" : "No fields inferred");
    } catch (e: any) {
      setErr(String(e));
      setStatus("");
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
            href={`/pipelines/${id}/clickhouse`}
          >
            ClickHouse
          </Link>
        </p>
        <h1>Pipeline {id} - Kafka</h1>
        {err && <p className="mb-2 text-sm text-red-600">{err}</p>}
        {status && <p className="mb-2 text-sm text-green-700">{status}</p>}
        {notice && <p className="mb-2 text-sm text-slate-600">{notice}</p>}
        <div className="grid grid-cols-1 gap-3">
          <input
            placeholder="brokers (comma separated)"
            value={(cfg.brokers || cfg.Brokers || []).join(",")}
            onChange={(e) =>
              setCfg({
                ...cfg,
                brokers: e.target.value
                  .split(",")
                  .map((s) => s.trim())
                  .filter(Boolean),
              })
            }
          />
          <input
            placeholder="topic"
            value={cfg.topic || cfg.Topic || ""}
            onChange={(e) => setCfg({ ...cfg, topic: e.target.value })}
          />
          <input
            placeholder="groupID"
            value={cfg.groupID || cfg.groupId || cfg.GroupID || ""}
            onChange={(e) => setCfg({ ...cfg, groupID: e.target.value })}
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
          <button onClick={infer}>Sample & Infer</button>
          {sample.length > 0 && typeof id === "string" && (
            <button
              className="bg-indigo-600 text-white hover:bg-indigo-700"
              onClick={async () => {
                try {
                  await api.savePipelineMapping(id as string, {
                    columns: sample,
                  });
                  setStatus("Inferred mapping saved");
                } catch (e: any) {
                  setErr(String(e));
                }
              }}
            >
              Use Inferred Mapping
            </button>
          )}
        </div>
        {sample.length > 0 && (
          <div className="mt-6">
            <h2>Sample Fields</h2>
            <div className="overflow-hidden rounded-lg border border-slate-200 bg-white">
              <table className="w-full">
                <thead className="bg-slate-50">
                  <tr>
                    <th className="text-left">Field</th>
                    <th className="text-left">Column</th>
                    <th className="text-left">Type</th>
                  </tr>
                </thead>
                <tbody>
                  {sample.map((f, i) => (
                    <tr key={i}>
                      <td>
                        <code className="text-xs">{f.fieldPath}</code>
                      </td>
                      <td>{f.column}</td>
                      <td>{f.type}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        )}
      </div>
    </main>
  );
}
