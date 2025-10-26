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
  const [databases, setDatabases] = useState<string[]>([]);
  const [tables, setTables] = useState<string[]>([]);
  const [selectedDb, setSelectedDb] = useState<string>("");
  const [selectedTable, setSelectedTable] = useState<string>("");
  const [tableCols, setTableCols] = useState<{ name: string; type: string }[]>(
    []
  );
  const [rowEdits, setRowEdits] = useState<
    Record<string, { column: string; type: string }>
  >({});
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
      // Load ClickHouse browsing info
      api
        .listDatabases(id)
        .then((r) =>
          setDatabases(Array.isArray(r.databases) ? r.databases : [])
        )
        .catch(() => setDatabases([]));
      api
        .getClickHouseConfig(id)
        .then((cfg) => {
          if (cfg?.database) setSelectedDb(cfg.database);
          if (cfg?.table) setSelectedTable(cfg.table);
        })
        .catch(() => {});
    }
  }, [id]);

  useEffect(() => {
    if (typeof id !== "string") return;
    if (!selectedDb) {
      setTables([]);
      setSelectedTable("");
      setTableCols([]);
      setRowEdits({});
      return;
    }
    api
      .listTables(id, selectedDb)
      .then((r) => setTables(Array.isArray(r.tables) ? r.tables : []))
      .catch(() => setTables([]));
  }, [id, selectedDb]);

  useEffect(() => {
    if (typeof id !== "string") return;
    if (!selectedDb || !selectedTable) {
      setTableCols([]);
      setRowEdits({});
      return;
    }
    api
      .getTableSchema(id, selectedDb, selectedTable)
      .then((r) => setTableCols(Array.isArray(r.columns) ? r.columns : []))
      .catch(() => setTableCols([]));
  }, [id, selectedDb, selectedTable]);
  const add = (f: { fieldPath: string }) => {
    const edit = rowEdits[f.fieldPath];
    if (!edit || !edit.column) return;
    const colType =
      edit.type ||
      tableCols.find((c) => c.name === edit.column)?.type ||
      "String";
    setMapping((m) => {
      const idx = m.columns.findIndex((c) => c.fieldPath === f.fieldPath);
      if (idx >= 0) {
        const next = [...m.columns];
        next[idx] = { ...next[idx], column: edit.column, type: colType };
        return { columns: next };
      }
      return {
        columns: [
          ...m.columns,
          { fieldPath: f.fieldPath, column: edit.column, type: colType },
        ],
      };
    });
  };
  const addAll = () => {
    const selected = fields
      .map((f) => ({ f, edit: rowEdits[f.fieldPath] }))
      .filter((r) => r.edit && r.edit.column);
    setMapping((m) => {
      const next = [...m.columns];
      for (const r of selected) {
        const colType =
          r.edit!.type ||
          tableCols.find((c) => c.name === r.edit!.column)?.type ||
          "String";
        const idx = next.findIndex((c) => c.fieldPath === r.f.fieldPath);
        if (idx >= 0) {
          next[idx] = { ...next[idx], column: r.edit!.column, type: colType };
        } else {
          next.push({
            fieldPath: r.f.fieldPath,
            column: r.edit!.column!,
            type: colType,
          });
        }
      }
      return { columns: next };
    });
  };
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
        <div className="mb-4 flex flex-col gap-2 md:flex-row md:items-center md:justify-between">
          <div className="flex flex-wrap items-center gap-2">
            <label className="text-sm text-slate-700">Database</label>
            <select
              value={selectedDb}
              onChange={(e) => setSelectedDb(e.target.value)}
            >
              <option value="">Select database</option>
              {(databases ?? []).map((d) => (
                <option key={d} value={d}>
                  {d}
                </option>
              ))}
            </select>
            <label className="ml-4 text-sm text-slate-700">Table</label>
            <select
              value={selectedTable}
              onChange={(e) => setSelectedTable(e.target.value)}
              disabled={!selectedDb}
            >
              <option value="">Select table</option>
              {(tables ?? []).map((t) => (
                <option key={t} value={t}>
                  {t}
                </option>
              ))}
            </select>
            {selectedDb && selectedTable && (
              <span className="text-xs text-slate-500">
                Schema columns: {tableCols.length}
              </span>
            )}
          </div>
          <h2 className="m-0">Sample Fields</h2>
          <button
            className="bg-slate-200 text-slate-900 hover:bg-slate-300"
            onClick={addAll}
          >
            Add All
          </button>
        </div>
        <div className="overflow-hidden rounded-lg border border-slate-200 bg-white">
          <table className="w-full">
            <thead className="bg-slate-50">
              <tr>
                <th className="text-left">Field</th>
                <th className="text-left">ClickHouse Column</th>
                <th className="text-left">Target Type</th>
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
                    <label htmlFor={`map-col-${i}`} className="sr-only">
                      ClickHouse Column
                    </label>
                    <select
                      id={`map-col-${i}`}
                      value={rowEdits[f.fieldPath]?.column || ""}
                      onChange={(e) => {
                        const col = e.target.value;
                        const colType =
                          tableCols.find((c) => c.name === col)?.type || "";
                        setRowEdits((r) => ({
                          ...r,
                          [f.fieldPath]: {
                            column: col,
                            type: r[f.fieldPath]?.type || colType,
                          },
                        }));
                      }}
                      disabled={!selectedTable}
                    >
                      <option value="">Select column</option>
                      {(tableCols ?? []).map((c) => (
                        <option key={c.name} value={c.name}>
                          {c.name}
                        </option>
                      ))}
                    </select>
                  </td>
                  <td>
                    <label htmlFor={`map-type-${i}`} className="sr-only">
                      Target Type
                    </label>
                    <select
                      id={`map-type-${i}`}
                      value={
                        rowEdits[f.fieldPath]?.type ||
                        tableCols.find(
                          (c) => c.name === rowEdits[f.fieldPath]?.column
                        )?.type ||
                        f.type
                      }
                      onChange={(e) =>
                        setRowEdits((r) => ({
                          ...r,
                          [f.fieldPath]: {
                            column: r[f.fieldPath]?.column || "",
                            type: e.target.value,
                          },
                        }))
                      }
                      disabled={!rowEdits[f.fieldPath]?.column}
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
                        <option key={t} value={t}>
                          {t}
                        </option>
                      ))}
                    </select>
                  </td>
                  <td>
                    <button
                      disabled={!rowEdits[f.fieldPath]?.column}
                      onClick={() => add(f)}
                    >
                      {mapping.columns.some((c) => c.fieldPath === f.fieldPath)
                        ? "Update"
                        : rowEdits[f.fieldPath]?.column
                        ? "Add"
                        : "Select column"}
                    </button>
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
