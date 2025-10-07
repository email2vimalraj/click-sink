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
    <main className="min-h-screen p-6">
      <div className="mx-auto max-w-4xl">
        <p className="mb-4 text-sm text-slate-600">
          <Link className="hover:underline" href={`/pipelines`}>
            ← Pipelines
          </Link>
          <span className="mx-2 text-slate-300">|</span>
          <Link
            className="text-indigo-600 hover:underline"
            href={`/pipelines/${id}/config`}
          >
            Config
          </Link>
          <span className="mx-2 text-slate-300">|</span>
          <Link
            className="text-indigo-600 hover:underline"
            href={`/pipelines/${id}/mapping`}
          >
            Mapping
          </Link>
        </p>
        <h1>Pipeline {id} - Run</h1>
        {err && <p className="mb-4 text-sm text-red-600">{err}</p>}
        <pre className="mb-4 rounded-md border border-slate-200 bg-white p-3 text-xs">
          {status ? JSON.stringify(status, null, 2) : "..."}
        </pre>
        <div className="flex gap-2">
          <button onClick={start}>Start</button>
          <button
            className="bg-slate-200 text-slate-900 hover:bg-slate-300"
            onClick={stop}
          >
            Stop
          </button>
        </div>
      </div>
    </main>
  );
}
