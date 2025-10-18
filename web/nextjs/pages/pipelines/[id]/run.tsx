import { useRouter } from "next/router";
import { useEffect, useState } from "react";
import Link from "next/link";
import { api, Status, PipelineState, Assignment } from "../../../lib/api";

export default function PipelineRun() {
  const router = useRouter();
  const { id } = router.query;
  const [status, setStatus] = useState<Status | null>(null);
  const [err, setErr] = useState<string | undefined>();
  const [state, setState] = useState<PipelineState | null>(null);
  const [assignments, setAssignments] = useState<Assignment[] | null>(null);
  const [mode, setMode] = useState<"leases" | "no-leases" | "mixed" | null>(
    null
  );
  const refresh = () => {
    if (typeof id === "string")
      api
        .pipelineStatus(id)
        .then(setStatus)
        .catch((e) => setErr(String(e)));
    if (typeof id === "string")
      api
        .getPipelineState(id)
        .then(setState)
        .catch((e) => console.warn(e));
    if (typeof id === "string")
      api
        .getAssignments(id)
        .then((r) => setAssignments(r.assignments))
        .catch((e) => console.warn(e));
    api
      .listWorkers()
      .then(({ workers }) => {
        if (!workers || workers.length === 0) {
          setMode(null);
        } else {
          const modes = new Set(
            workers.map((w) => (w.mode as any) || "leases")
          );
          if (modes.size === 1)
            setMode((Array.from(modes)[0] as any) || "leases");
          else setMode("mixed");
        }
      })
      .catch(() => setMode(null));
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
  const desiredStart = async () => {
    if (typeof id !== "string") return;
    try {
      await api.setPipelineState(id, { desired: "started" });
      refresh();
    } catch (e: any) {
      alert(String(e));
    }
  };
  const desiredStop = async () => {
    if (typeof id !== "string") return;
    try {
      await api.setPipelineState(id, { desired: "stopped" });
      refresh();
    } catch (e: any) {
      alert(String(e));
    }
  };
  const updateReplicas = async (replicas: number) => {
    if (typeof id !== "string") return;
    try {
      await api.setPipelineState(id, {
        desired: (state?.desired as any) || "started",
        replicas,
      });
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
        <pre className="mb-4 rounded-md border border-slate-200 bg-white p-3 text-xs">
          {state ? JSON.stringify(state, null, 2) : "..."}
        </pre>
        {mode && (
          <div
            className={`mb-4 rounded border p-2 text-sm ${
              mode === "no-leases"
                ? "border-orange-300 bg-orange-50 text-orange-700"
                : mode === "mixed"
                ? "border-yellow-300 bg-yellow-50 text-yellow-700"
                : "border-emerald-300 bg-emerald-50 text-emerald-700"
            }`}
          >
            Worker mode: {mode}
            {mode === "no-leases" && (
              <span className="ml-2 text-xs">
                Replicas are ignored in no-leases mode. Each worker runs one
                instance per pipeline.
              </span>
            )}
            {mode === "mixed" && (
              <span className="ml-2 text-xs">
                Mixed modes detected. Prefer running all workers with leases or
                all with no-leases.
              </span>
            )}
          </div>
        )}
        <div className="mb-4 flex items-center gap-2">
          <label className="text-sm text-slate-700">Replicas</label>
          <input
            type="number"
            min={1}
            className="w-24 rounded border px-2 py-1"
            value={state?.replicas || 1}
            onChange={(e) =>
              updateReplicas(Math.max(1, parseInt(e.target.value || "1", 10)))
            }
            disabled={mode === "no-leases"}
          />
        </div>
        <div className="flex gap-2">
          <button onClick={start}>Start</button>
          <button
            className="bg-slate-200 text-slate-900 hover:bg-slate-300"
            onClick={stop}
          >
            Stop
          </button>
          <span className="mx-2" />
          <button className="bg-green-600 text-white" onClick={desiredStart}>
            Desired: Start
          </button>
          <button className="bg-orange-600 text-white" onClick={desiredStop}>
            Desired: Stop
          </button>
        </div>
        <h2 className="mt-8 text-lg font-semibold">Assignments</h2>
        <div className="mb-4 rounded-md border border-slate-200 bg-white p-3 text-xs">
          {assignments ? (
            assignments.length ? (
              <ul>
                {assignments.map((a) => (
                  <li key={a.slot}>
                    slot {a.slot} → {a.workerId} (leaseUntil{" "}
                    {new Date(a.leaseUntil).toLocaleTimeString()})
                  </li>
                ))}
              </ul>
            ) : (
              <span>No active assignments</span>
            )
          ) : (
            <span>...</span>
          )}
        </div>
      </div>
    </main>
  );
}
