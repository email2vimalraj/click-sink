import { useEffect, useState } from "react";
import Link from "next/link";
import { api, Status } from "../lib/api";

export default function PipelinePage() {
  const [status, setStatus] = useState<Status | null>(null);
  const [err, setErr] = useState<string | undefined>();
  const refresh = () =>
    api
      .status()
      .then(setStatus)
      .catch((e) => setErr(String(e)));
  useEffect(() => {
    refresh();
    const t = setInterval(refresh, 2000);
    return () => clearInterval(t);
  }, []);
  const start = async () => {
    try {
      await api.start();
      refresh();
    } catch (e: any) {
      alert(String(e));
    }
  };
  const stop = async () => {
    try {
      await api.stop();
      refresh();
    } catch (e: any) {
      alert(String(e));
    }
  };
  return (
    <main style={{ padding: 24 }}>
      <p>
        <Link href="/">← Home</Link>
      </p>
      <h1>Pipeline</h1>
      {err && <p style={{ color: "red" }}>{err}</p>}
      <pre>{status ? JSON.stringify(status, null, 2) : "..."}</pre>
      <button onClick={start}>Start</button>
      <button onClick={stop}>Stop</button>
    </main>
  );
}
