import { useRouter } from "next/router";
import { useEffect, useState } from "react";
import Link from "next/link";
import { api, Status } from "../../../lib/api";

export default function PipelineRun() {
  const router = useRouter();
  const { id } = router.query;
  const [status, setStatus] = useState<Status | null>(null);
  const [err, setErr] = useState<string | undefined>();
  const refresh = () => {
    if (typeof id === "string")
      api
        .pipelineStatus(id)
        .then(setStatus)
        .catch((e) => setErr(String(e)));
  };
  useEffect(() => {
    refresh();
    const t = setInterval(refresh, 2000);
    return () => clearInterval(t);
  }, [id]);
  const start = async () => {
    if (typeof id !== "string") return;
    try {
      await api.pipelineStart(id);
      refresh();
    } catch (e: any) {
      alert(String(e));
    }
  };
  const stop = async () => {
    if (typeof id !== "string") return;
    try {
      await api.pipelineStop(id);
      refresh();
    } catch (e: any) {
      alert(String(e));
    }
  };
  return (
    <main style={{ padding: 24 }}>
      <p>
        <Link href={`/pipelines`}>← Pipelines</Link> |{" "}
        <Link href={`/pipelines/${id}/config`}>Config</Link> |{" "}
        <Link href={`/pipelines/${id}/mapping`}>Mapping</Link>
      </p>
      <h1>Pipeline {id} - Run</h1>
      {err && <p style={{ color: "red" }}>{err}</p>}
      <pre>{status ? JSON.stringify(status, null, 2) : "..."}</pre>
      <button onClick={start}>Start</button>
      <button onClick={stop}>Stop</button>
    </main>
  );
}
