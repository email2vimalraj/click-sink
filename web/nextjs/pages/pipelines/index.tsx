import { useEffect, useState } from "react";
import Link from "next/link";
import { api } from "../../lib/api";

export default function PipelinesPage() {
  const [list, setList] = useState<any[]>([]);
  const [name, setName] = useState("");
  const [err, setErr] = useState<string | undefined>();
  const refresh = () =>
    api
      .listPipelines()
      .then(setList)
      .catch((e) => setErr(String(e)));
  useEffect(() => {
    refresh();
  }, []);
  const create = async () => {
    try {
      if (!name.trim()) return;
      await api.createPipeline(name.trim());
      setName("");
      refresh();
    } catch (e: any) {
      alert(String(e));
    }
  };
  const del = async (id: string) => {
    if (!confirm("Delete pipeline?")) return;
    try {
      await api.deletePipeline(id);
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
      <h1>Pipelines</h1>
      {err && <p style={{ color: "red" }}>{err}</p>}
      <div style={{ marginBottom: 12 }}>
        <input
          placeholder="pipeline name"
          value={name}
          onChange={(e) => setName(e.target.value)}
        />
        <button onClick={create}>Create</button>
      </div>
      <table border={1} cellPadding={6} cellSpacing={0}>
        <thead>
          <tr>
            <th>Name</th>
            <th>Status</th>
            <th>Total Rows</th>
            <th>Actions</th>
          </tr>
        </thead>
        <tbody>
          {list.map((p) => (
            <tr key={p.id}>
              <td>
                <Link href={`/pipelines/${p.id}/config`}>{p.name}</Link>
              </td>
              <td>{p.running ? "running" : "stopped"}</td>
              <td>{p.totalRows || 0}</td>
              <td>
                <Link href={`/pipelines/${p.id}/config`}>Config</Link>
                {" | "}
                <Link href={`/pipelines/${p.id}/mapping`}>Mapping</Link>
                {" | "}
                <Link href={`/pipelines/${p.id}/run`}>Run</Link>
                {" | "}
                <button onClick={() => del(p.id)}>Delete</button>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </main>
  );
}
