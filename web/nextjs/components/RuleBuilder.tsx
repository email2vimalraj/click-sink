import { useMemo } from "react";
import {
  BoolOp,
  Rule,
  RuleGroup,
  RuleNode,
  newGroup,
  newRule,
  toCEL,
} from "./rules";

export type RuleBuilderProps = {
  value: RuleGroup;
  onChange: (v: RuleGroup) => void;
};

export function RuleBuilder({ value, onChange }: RuleBuilderProps) {
  const cel = useMemo(() => toCEL(value), [value]);

  return (
    <div className="flex flex-col gap-3">
      <GroupEditor node={value} onChange={onChange} />
      <div className="flex flex-col gap-1">
        <label className="text-sm text-slate-700">Generated CEL</label>
        <textarea readOnly rows={5} value={cel} />
        <p className="text-xs text-slate-500">
          This is generated from the visual rules. Saving will persist this CEL
          expression.
        </p>
      </div>
    </div>
  );
}

function GroupEditor({
  node,
  onChange,
}: {
  node: RuleGroup;
  onChange: (v: RuleGroup) => void;
}) {
  const setOp = (op: BoolOp) => onChange({ ...node, op });
  const setNot = (not: boolean) => onChange({ ...node, not });

  const updateChild = (idx: number, child: RuleNode) => {
    const children = node.children.slice();
    children[idx] = child;
    onChange({ ...node, children });
  };

  const removeChild = (idx: number) => {
    const children = node.children.slice();
    children.splice(idx, 1);
    onChange({ ...node, children });
  };

  const addRule = () =>
    onChange({ ...node, children: [...node.children, newRule()] });
  const addGroup = () =>
    onChange({ ...node, children: [...node.children, newGroup([newRule()])] });

  return (
    <div className="rounded-md border border-slate-200 p-3">
      <div className="mb-3 flex items-center gap-3">
        <span className="text-sm font-medium text-slate-700">Group</span>
        <select
          value={node.op}
          onChange={(e) => setOp(e.target.value as BoolOp)}
        >
          <option value="AND">AND</option>
          <option value="OR">OR</option>
        </select>
        <label className="ml-2 inline-flex items-center gap-2 text-sm text-slate-700">
          <input
            type="checkbox"
            checked={!!node.not}
            onChange={(e) => setNot(e.target.checked)}
          />{" "}
          NOT
        </label>
        <div className="ml-auto flex gap-2">
          <button className="btn" onClick={addRule}>
            + Condition
          </button>
          <button className="btn" onClick={addGroup}>
            + Group
          </button>
        </div>
      </div>
      <div className="flex flex-col gap-2">
        {node.children.length === 0 && (
          <p className="text-xs text-slate-500">
            No rules yet. Add a condition or nested group.
          </p>
        )}
        {node.children.map((c, idx) => (
          <div
            key={(c as any).id}
            className="flex flex-col gap-2 rounded-md bg-slate-50 p-2"
          >
            <div className="flex items-center justify-between">
              <span className="text-xs uppercase tracking-wide text-slate-500">
                {c.kind === "group" ? "Group" : "Condition"}
              </span>
              <button
                className="btn btn-danger"
                onClick={() => removeChild(idx)}
              >
                Remove
              </button>
            </div>
            {c.kind === "group" ? (
              <GroupEditor node={c} onChange={(v) => updateChild(idx, v)} />
            ) : (
              <RuleEditor node={c} onChange={(v) => updateChild(idx, v)} />
            )}
          </div>
        ))}
      </div>
    </div>
  );
}

function RuleEditor({
  node,
  onChange,
}: {
  node: Rule;
  onChange: (v: Rule) => void;
}) {
  const setField = (field: string) => onChange({ ...node, field });
  const setOp = (operator: Rule["operator"]) => onChange({ ...node, operator });
  const setVal = (value: string) => onChange({ ...node, value });
  const setNot = (not: boolean) => onChange({ ...node, not });

  return (
    <div className="grid grid-cols-1 gap-2 md:grid-cols-12 md:items-center">
      <div className="md:col-span-5">
        <label className="text-xs text-slate-600">Field (flattened path)</label>
        <input
          placeholder="e.g. user.email or event.type"
          value={node.field}
          onChange={(e) => setField(e.target.value)}
        />
      </div>
      <div className="md:col-span-3">
        <label className="text-xs text-slate-600">Operator</label>
        <select
          value={node.operator}
          onChange={(e) => setOp(e.target.value as any)}
        >
          <option value="equals">equals</option>
          <option value="contains">contains</option>
          <option value="regex">matches regex</option>
          <option value="exists">exists</option>
          <option value="gt">numeric &gt;</option>
          <option value="gte">numeric &ge;</option>
          <option value="lt">numeric &lt;</option>
          <option value="lte">numeric &le;</option>
        </select>
      </div>
      {node.operator !== "exists" && (
        <div className="md:col-span-3">
          <label className="text-xs text-slate-600">Value</label>
          <input
            placeholder={
              node.operator === "regex"
                ? "e.g. \\.(com|org)$"
                : node.operator === "contains"
                ? "e.g. @example.com"
                : node.operator === "gt" ||
                  node.operator === "gte" ||
                  node.operator === "lt" ||
                  node.operator === "lte"
                ? "e.g. 100"
                : "e.g. purchase"
            }
            value={node.value}
            onChange={(e) => setVal(e.target.value)}
          />
        </div>
      )}
      <div className="md:col-span-1 mt-4 md:mt-6">
        <label className="inline-flex items-center gap-2 text-xs text-slate-600">
          <input
            type="checkbox"
            checked={!!node.not}
            onChange={(e) => setNot(e.target.checked)}
          />{" "}
          NOT
        </label>
      </div>
    </div>
  );
}
