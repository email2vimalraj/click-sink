import { useRouter } from "next/router";
import { useEffect, useMemo, useState } from "react";
import Link from "next/link";
import Head from "next/head";
import { api } from "../../../lib/api";
import { RuleBuilder } from "../../../components/RuleBuilder";
import { RuleGroup, newGroup, toCEL } from "../../../components/rules";

export default function PipelineFilters() {
  const router = useRouter();
  const { id } = router.query;
  const [enabled, setEnabled] = useState<boolean>(false);
  const [language, setLanguage] = useState<string>("CEL");
  const [expression, setExpression] = useState<string>("");
  const [status, setStatus] = useState<string>("");
  const [err, setErr] = useState<string | undefined>();
  const [mode, setMode] = useState<"visual" | "cel">("visual");
  const [root, setRoot] = useState<RuleGroup>(() => newGroup());
  const generatedCEL = useMemo(() => toCEL(root), [root]);
  const [sample, setSample] = useState<string>(
    '{\n  "event": { "type": "purchase" },\n  "user": { "email": "a@example.com" },\n  "value": 123\n}'
  );
  const [testResult, setTestResult] = useState<string>("");
  const [testing, setTesting] = useState<boolean>(false);

  useEffect(() => {
    if (typeof id === "string") {
      api
        .getFilterConfig(id)
        .then((cfg) => {
          setEnabled(!!cfg.enabled);
          setLanguage(cfg.language || "CEL");
          setExpression(cfg.expression || "");
          // Choose mode based on presence of saved expression
          if (
            cfg &&
            typeof cfg.expression === "string" &&
            cfg.expression.trim() !== ""
          ) {
            setMode("cel");
          } else {
            setMode("visual");
          }
          // Initialize a fresh visual builder tree (we do not parse CEL back into rules yet)
          setRoot(newGroup());
        })
        .catch(() => {});
    }
  }, [id]);

  const save = async () => {
    if (typeof id !== "string") return;
    setStatus("Saving...");
    setErr(undefined);
    try {
      const expr = mode === "visual" ? generatedCEL : expression;
      await api.saveFilterConfig(id, { enabled, language, expression: expr });
      setStatus("Saved");
    } catch (e: any) {
      setErr(String(e));
      setStatus("");
    }
  };

  const test = async () => {
    if (typeof id !== "string") return;
    setTesting(true);
    setTestResult("");
    try {
      const expr = mode === "visual" ? generatedCEL : expression;
      let payload: any;
      try {
        payload = JSON.parse(sample);
      } catch (e) {
        setTestResult("Invalid JSON sample");
        setTesting(false);
        return;
      }
      const res = await api.testFilter(id, expr, payload);
      if (res.error) {
        setTestResult(`Error: ${res.error}`);
      } else {
        setTestResult(
          res.result
            ? "Would KEEP (passes filter)"
            : "Would DROP (filtered out)"
        );
      }
    } catch (e: any) {
      setTestResult(String(e));
    } finally {
      setTesting(false);
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
          <div className="flex items-center gap-3">
            <label className="text-sm text-slate-700">Mode</label>
            <select
              value={mode}
              onChange={(e) => {
                const next = e.target.value as "visual" | "cel";
                setMode(next);
                if (next === "cel") {
                  setExpression(generatedCEL);
                }
              }}
            >
              <option value="visual">Visual Builder</option>
              <option value="cel">CEL Code</option>
            </select>
            {mode === "visual" && expression && (
              <span className="text-xs text-amber-600">
                Saving will overwrite the existing CEL with the generated
                expression.
              </span>
            )}
          </div>
          {mode === "visual" ? (
            <div className="flex flex-col gap-1">
              <RuleBuilder value={root} onChange={setRoot} />
              <p className="text-xs text-slate-500">
                Build conditions on flattened fields. For string match, use
                equals; for regex use matches regex. Nested groups support
                AND/OR/NOT. Generated CEL guards for missing keys using "key in
                flat".
              </p>
            </div>
          ) : (
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
                  'Example: ("event.type" in flat) && string(flat["event.type"]) == "purchase" && ("user.email" in flat) && string(flat["user.email"]).matches("@example.com$")'
                }
                value={expression}
                onChange={(e) => setExpression(e.target.value)}
              />
              <p className="text-xs text-slate-500">
                Context: flat is a map of flattened JSON fields, e.g.,
                flat["user.id"], flat["event.value"]. Use matches() for regex
                and string()/int()/double()/bool() casts as needed.
              </p>
            </div>
          )}
        </div>
        <div className="mt-4 flex gap-2">
          <button onClick={save}>Save</button>
        </div>
        <div className="mt-8 rounded-md border border-slate-200 p-3">
          <h2 className="mb-2 text-base font-semibold">
            Test against sample JSON
          </h2>
          <p className="mb-2 text-xs text-slate-500">
            Paste a representative JSON payload to evaluate the current
            expression. This simulates the pipeline's pre-mapping filter
            evaluation using the flattened fields.
          </p>
          <textarea
            rows={8}
            value={sample}
            onChange={(e) => setSample(e.target.value)}
          />
          <div className="mt-2 flex items-center gap-2">
            <button onClick={test} disabled={testing}>
              {testing ? "Testing..." : "Test Expression"}
            </button>
            {testResult && (
              <span
                className={`text-sm ${
                  testResult.startsWith("Would KEEP")
                    ? "text-green-700"
                    : testResult.startsWith("Would DROP")
                    ? "text-red-700"
                    : "text-amber-700"
                }`}
              >
                {testResult}
              </span>
            )}
          </div>
        </div>
      </div>
    </main>
  );
}
