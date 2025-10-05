import Link from "next/link";

export default function Home() {
  return (
    <main style={{ padding: 24, fontFamily: "system-ui, sans-serif" }}>
      <h1>click-sink UI</h1>
      <ul>
        <li>
          <Link href="/config">Config</Link>
        </li>
        <li>
          <Link href="/mapping">Mapping</Link>
        </li>
        <li>
          <Link href="/pipeline">Pipeline</Link>
        </li>
      </ul>
    </main>
  );
}
