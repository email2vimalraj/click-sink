export type BoolOp = "AND" | "OR";

export type RuleOperator = "equals" | "regex";

export type RuleNode = RuleGroup | Rule;

export type RuleGroup = {
  id: string;
  kind: "group";
  op: BoolOp;
  not?: boolean;
  children: RuleNode[];
};

export type Rule = {
  id: string;
  kind: "rule";
  not?: boolean;
  field: string; // flattened path, e.g., user.email
  operator: RuleOperator;
  value: string; // string for equals/regex
};

export function newRule(): Rule {
  return {
    id: cryptoId(),
    kind: "rule",
    field: "",
    operator: "equals",
    value: "",
  };
}

export function newGroup(children: RuleNode[] = []): RuleGroup {
  return {
    id: cryptoId(),
    kind: "group",
    op: "AND",
    children,
  };
}

export function cryptoId(): string {
  if (typeof crypto !== "undefined" && "randomUUID" in crypto) {
    // @ts-ignore
    return crypto.randomUUID();
  }
  return Math.random().toString(36).slice(2);
}

// Convert rule tree to CEL expression string using flat["path"] semantics
export function toCEL(node: RuleNode): string {
  if (node.kind === "rule") return ruleToCEL(node);
  const inner = node.children
    .map((c) => wrapIfNeeded(c))
    .join(` ${node.op === "AND" ? "&&" : "||"} `);
  const expr = inner || "true"; // empty group yields true
  return node.not ? `!(${expr})` : `${expr}`;
}

function wrapIfNeeded(n: RuleNode): string {
  const s = toCEL(n);
  if (n.kind === "rule") return s; // rule already atomic
  return `(${s})`;
}

function escapeString(s: string): string {
  return s.replace(/\\/g, "\\\\").replace(/"/g, '\\"');
}

function ruleToCEL(r: Rule): string {
  const key = escapeString(r.field.trim());
  const val = escapeString(r.value);
  let base = "true";
  if (!key) {
    base = "true";
  } else if (r.operator === "equals") {
    base = `("${key}" in flat) && string(flat["${key}"]) == "${val}"`;
  } else if (r.operator === "regex") {
    base = `("${key}" in flat) && string(flat["${key}"]).matches("${val}")`;
  }
  return r.not ? `!(${base})` : base;
}

// Attempt a very naive parse for simple expressions produced by this builder.
// For now we do not support parsing arbitrary CEL back to rules.
export function canParseBackToRules(expr: string): boolean {
  // Recognize patterns we generate only; otherwise return false
  return /flat\[".+"\]/.test(expr) && /&&|\|\|/.test(expr);
}
