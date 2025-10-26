import { useRouter } from "next/router";
import { useEffect, useState } from "react";
import Link from "next/link";
import Head from "next/head";
import { api } from "../../../lib/api";

export default function PipelineFilters() {
  const router = useRouter();
  const { id } = router.query;
  const [enabled, setEnabled] = useState<boolean>(false);
  const [language, setLanguage] = useState<string>("CEL");
  const [expression, setExpression] = useState<string>("");
  const [status, setStatus] = useState<string>("");
  const [err, setErr] = useState<string | undefined>();

  useEffect(() => {
    if (typeof id === "string") {
      api
        .getFilterConfig(id)
        .then((cfg) => {
          setEnabled(!!cfg.enabled);
          setLanguage(cfg.language || "CEL");
          setExpression(cfg.expression || "");
        })
        .catch(() => {});
    }
  }, [id]);

  const save = async () => {
    if (typeof id !== "string") return;
    setStatus("Saving...");
    setErr(undefined);
    try {
      await api.saveFilterConfig(id, { enabled, language, expression });
      setStatus("Saved");
    } catch (e: any) {
      setErr(String(e));
      setStatus("");
    }
  };

  return (
    <main className="min-h-screen p-6">
      <Head>
        <title>
          {typeof id === "string"
            ? `Filters - Pipeline ${id} - Click Sink`
            : "Filters - Click Sink"}
        </title>
      </Head>
      <div className="mx-auto max-w-4xl">
        <p className="mb-4 text-sm text-slate-600">
          <Link className="hover:underline" href={`/pipelines`}>
            ← Pipelines
          </Link>
          <span className="mx-2 text-slate-300">|</span>
          <Link className="hover:underline" href={`/pipelines/${id}/kafka`}>
            Kafka
          </Link>
          <span className="mx-2 text-slate-300">|</span>
          <Link className="hover:underline" href={`/pipelines/${id}/mapping`}>
            Mapping
          </Link>
          <span className="mx-2 text-slate-300">|</span>
          <Link className="hover:underline" href={`/pipelines/${id}/run`}>
            Run
          </Link>
        </p>
        <h1>Pipeline {id} - Filters</h1>
        {err && <p className="mb-2 text-sm text-red-600">{err}</p>}
        {status && <p className="mb-2 text-sm text-green-700">{status}</p>}
        <div className="mt-2 flex items-center gap-2">
          <input
            id="filter-enabled"
            type="checkbox"
            checked={enabled}
            onChange={(e) => setEnabled(e.target.checked)}
          />
          <label htmlFor="filter-enabled" className="text-sm text-slate-700">
            Enable filtering
          </label>
        </div>
        <div className="mt-4 grid grid-cols-1 gap-3">
          <div className="flex flex-col gap-1">
            <label htmlFor="filter-language" className="text-sm text-slate-700">
              Language
            </label>
            <select
              id="filter-language"
              value={language}
              onChange={(e) => setLanguage(e.target.value)}
            >
              <option value="CEL">CEL</option>
            </select>
          </div>
          <div className="flex flex-col gap-1">
            <label
              htmlFor="filter-expression"
              className="text-sm text-slate-700"
            >
              Expression
            </label>
            <textarea
              id="filter-expression"
              rows={8}
              placeholder={
                'Example: flat["event.type"] == "purchase" && string(flat["user.email"]).matches("@example.com$")'
              }
              value={expression}
              onChange={(e) => setExpression(e.target.value)}
            />
            <p className="text-xs text-slate-500">
              Context: flat is a map of flattened JSON fields, e.g.,
              flat["user.id"], flat["event.value"]. Use matches() for regex and
              string()/int()/double()/bool() casts as needed.
            </p>
          </div>
        </div>
        <div className="mt-4 flex gap-2">
          <button onClick={save}>Save</button>
        </div>
      </div>
    </main>
  );
}
