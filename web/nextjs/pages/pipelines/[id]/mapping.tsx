import { useRouter } from "next/router";
import { useEffect, useState } from "react";
import Link from "next/link";
import { api, Mapping } from "../../../lib/api";

export default function PipelineMapping() {
  const router = useRouter();
  const { id } = router.query;
  const [fields, setFields] = useState<
    { fieldPath: string; column: string; type: string }[]
  >([]);
  const [mapping, setMapping] = useState<Mapping>({ columns: [] });
  const [err, setErr] = useState<string | undefined>();
  useEffect(() => {
    if (typeof id === "string") {
      api
        .pipelineSample(id, 100)
        .then(setFields)
        .catch((e) => setErr(String(e)));
      api
        .getPipelineMapping(id)
        .then(setMapping)
        .catch(() => {});
    }
  }, [id]);
  const add = (f: { fieldPath: string; column: string; type: string }) =>
    setMapping((m) => ({
      columns: [
        ...m.columns,
        { fieldPath: f.fieldPath, column: f.column, type: f.type },
      ],
    }));
  const save = async () => {
    if (typeof id !== "string") return;
    try {
      await api.savePipelineMapping(id, mapping);
      alert("Saved");
    } catch (e: any) {
      alert(String(e));
    }
  };
  return (
    <main className="min-h-screen p-6">
      <div className="mx-auto max-w-5xl">
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
            href={`/pipelines/${id}/run`}
          >
            Run
          </Link>
        </p>
        <h1>Pipeline {id} - Mapping</h1>
        {err && <p className="mb-4 text-sm text-red-600">{err}</p>}
        <div className="overflow-hidden rounded-lg border border-slate-200 bg-white">
          <table className="w-full">
            <thead className="bg-slate-50">
              <tr>
                <th className="text-left">Field</th>
                <th className="text-left">Column</th>
                <th className="text-left">Type</th>
                <th></th>
              </tr>
            </thead>
            <tbody>
              {fields.map((f, i) => (
                <tr key={i}>
                  <td>
                    <code className="text-xs">{f.fieldPath}</code>
                  </td>
                  <td>
                    <input
                      defaultValue={f.column}
                      onChange={(e) => (f.column = e.target.value)}
                    />
                  </td>
                  <td>
                    <select
                      defaultValue={f.type}
                      onChange={(e) => (f.type = e.target.value)}
                    >
                      {[
                        "String",
                        "Int64",
                        "Float64",
                        "Bool",
                        "Nullable(String)",
                        "Nullable(Int64)",
                        "Nullable(Float64)",
                        "Nullable(Bool)",
                      ].map((t) => (
                        <option key={t}>{t}</option>
                      ))}
                    </select>
                  </td>
                  <td>
                    <button onClick={() => add(f)}>Add</button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        <h2>Current Mapping</h2>
        <pre className="rounded-md border border-slate-200 bg-white p-3 text-xs">
          {JSON.stringify(mapping, null, 2)}
        </pre>
        <button onClick={save}>Save Mapping</button>
      </div>
    </main>
  );
}
