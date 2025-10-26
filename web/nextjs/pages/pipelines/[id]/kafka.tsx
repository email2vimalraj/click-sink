import { useRouter } from "next/router";
import { useEffect, useState } from "react";
import Link from "next/link";
import Head from "next/head";
import { api } from "../../../lib/api";

export default function PipelineKafka() {
  const router = useRouter();
  const { id } = router.query;
  const [cfg, setCfg] = useState<any>({
    brokers: [],
    topic: "",
    groupID: "",
    securityProtocol: "",
    saslMechanism: "",
    saslUsername: "",
    saslPassword: "",
    tlsCA: "",
    tlsCert: "",
    tlsKey: "",
    tlsInsecureSkipVerify: false,
    tlsServerName: "",
    gssapiRealm: "",
    gssapiServiceName: "",
    gssapiAuthType: "",
    gssapiUsername: "",
    gssapiPassword: "",
    gssapiKeytabPath: "",
    gssapiKerberosConfigPath: "",
    gssapiDisablePAFXFAST: false,
  });
  const [status, setStatus] = useState<string>("");
  const [err, setErr] = useState<string | undefined>();
  const [notice, setNotice] = useState<string | undefined>();
  const [sample, setSample] = useState<
    { fieldPath: string; column: string; type: string }[]
  >([]);

  useEffect(() => {
    if (typeof id === "string") {
      api
        .getKafkaConfig(id)
        .then(setCfg)
        .catch(() => {});
    }
  }, [id]);

  const normalize = () => ({
    brokers: cfg.brokers || cfg.Brokers || [],
    topic: cfg.topic || cfg.Topic || "",
    groupID: cfg.groupID || cfg.groupId || cfg.GroupID || "",
    securityProtocol: cfg.securityProtocol || cfg.SecurityProtocol || "",
    saslUsername: cfg.saslUsername || cfg.SASLUsername || "",
    saslPassword: cfg.saslPassword || cfg.SASLPassword || "",
    saslMechanism: cfg.saslMechanism || cfg.SASLMechanism || "",
    tlsCA: cfg.tlsCA || cfg.TLSCA || "",
    tlsCert: cfg.tlsCert || cfg.TLSCert || "",
    tlsKey: cfg.tlsKey || cfg.TLSKey || "",
    tlsInsecureSkipVerify:
      cfg.tlsInsecureSkipVerify ?? cfg.TLSInsecureSkipVerify ?? false,
    tlsServerName: cfg.tlsServerName || cfg.TLSServerName || "",
    gssapiRealm: cfg.gssapiRealm || cfg.GSSAPIRealm || "",
    gssapiServiceName: cfg.gssapiServiceName || cfg.GSSAPIServiceName || "",
    gssapiAuthType: cfg.gssapiAuthType || cfg.GSSAPIAuthType || "",
    gssapiUsername: cfg.gssapiUsername || cfg.GSSAPIUsername || "",
    gssapiPassword: cfg.gssapiPassword || cfg.GSSAPIPassword || "",
    gssapiKeytabPath: cfg.gssapiKeytabPath || cfg.GSSAPIKeytabPath || "",
    gssapiKerberosConfigPath:
      cfg.gssapiKerberosConfigPath || cfg.GSSAPIKerberosConfigPath || "",
    gssapiDisablePAFXFAST:
      cfg.gssapiDisablePAFXFAST ?? cfg.GSSAPIDisablePAFXFAST ?? false,
  });

  const save = async () => {
    if (typeof id !== "string") return;
    try {
      await api.saveKafkaConfig(id, normalize());
      setStatus("Saved");
    } catch (e: any) {
      setErr(String(e));
    }
  };

  const validate = async () => {
    if (typeof id !== "string") return;
    setStatus("Validating...");
    setErr(undefined);
    try {
      await api.validateKafka(id);
      setStatus("Kafka connectivity OK");
    } catch (e: any) {
      setErr(String(e));
      setStatus("");
    }
  };

  const infer = async () => {
    setStatus("Sampling...");
    setErr(undefined);
    setNotice(undefined);
    try {
      const res = await api.validateKafkaSample(normalize(), 100);
      setSample(res.fields);
      if (res.notice) setNotice(res.notice);
      setStatus(res.fields.length > 0 ? "Sampled" : "No fields inferred");
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
            ? `Kafka - Pipeline ${id} - Click Sink`
            : "Kafka - Click Sink"}
        </title>
      </Head>
      <div className="mx-auto max-w-4xl">
        <p className="mb-4 text-sm text-slate-600">
          <Link className="hover:underline" href={`/pipelines`}>
            ← Pipelines
          </Link>
          <span className="mx-2 text-slate-300">|</span>
          <Link
            className="text-indigo-600 hover:underline"
            href={`/pipelines/${id}/clickhouse`}
          >
            ClickHouse
          </Link>
          <span className="mx-2 text-slate-300">|</span>
          <Link className="hover:underline" href={`/pipelines/${id}/filters`}>
            Filters
          </Link>
        </p>
        <h1>Pipeline {id} - Kafka</h1>
        {err && <p className="mb-2 text-sm text-red-600">{err}</p>}
        {status && <p className="mb-2 text-sm text-green-700">{status}</p>}
        {notice && <p className="mb-2 text-sm text-slate-600">{notice}</p>}
        <div className="grid grid-cols-1 gap-3">
          <div className="flex flex-col gap-1">
            <label htmlFor="kafka-brokers" className="text-sm text-slate-700">
              Brokers (comma separated)
            </label>
            <input
              id="kafka-brokers"
              placeholder="kafka:9092"
              value={(cfg.brokers || cfg.Brokers || []).join(",")}
              onChange={(e) =>
                setCfg({
                  ...cfg,
                  brokers: e.target.value
                    .split(",")
                    .map((s) => s.trim())
                    .filter(Boolean),
                })
              }
            />
          </div>
          <div className="flex flex-col gap-1">
            <label htmlFor="kafka-topic" className="text-sm text-slate-700">
              Topic
            </label>
            <input
              id="kafka-topic"
              placeholder="topic"
              value={cfg.topic || cfg.Topic || ""}
              onChange={(e) => setCfg({ ...cfg, topic: e.target.value })}
            />
          </div>
          <div className="flex flex-col gap-1">
            <label htmlFor="kafka-group" className="text-sm text-slate-700">
              Consumer Group ID
            </label>
            <input
              id="kafka-group"
              placeholder="groupID"
              value={cfg.groupID || cfg.groupId || cfg.GroupID || ""}
              onChange={(e) => setCfg({ ...cfg, groupID: e.target.value })}
            />
          </div>
          <div className="flex flex-col gap-1">
            <label htmlFor="kafka-security" className="text-sm text-slate-700">
              Security Protocol
            </label>
            <select
              id="kafka-security"
              value={cfg.securityProtocol || cfg.SecurityProtocol || ""}
              onChange={(e) =>
                setCfg({ ...cfg, securityProtocol: e.target.value })
              }
            >
              <option value="">(default)</option>
              <option value="PLAINTEXT">PLAINTEXT</option>
              <option value="SSL">SSL</option>
              <option value="SASL_PLAINTEXT">SASL_PLAINTEXT</option>
              <option value="SASL_SSL">SASL_SSL</option>
            </select>
          </div>
          {(cfg.securityProtocol || cfg.SecurityProtocol || "").startsWith(
            "SASL_"
          ) && (
            <div className="flex flex-col gap-1">
              <label
                htmlFor="kafka-sasl-mech"
                className="text-sm text-slate-700"
              >
                SASL Mechanism
              </label>
              <select
                id="kafka-sasl-mech"
                value={cfg.saslMechanism || cfg.SASLMechanism || ""}
                onChange={(e) =>
                  setCfg({ ...cfg, saslMechanism: e.target.value })
                }
              >
                <option value="PLAIN">PLAIN</option>
                <option value="SCRAM-SHA-256">SCRAM-SHA-256</option>
                <option value="SCRAM-SHA-512">SCRAM-SHA-512</option>
                <option value="GSSAPI">GSSAPI (Kerberos)</option>
              </select>
            </div>
          )}
          {(cfg.securityProtocol || cfg.SecurityProtocol || "").includes(
            "SSL"
          ) && (
            <div className="grid grid-cols-1 gap-3">
              <div className="flex flex-col gap-1">
                <label
                  htmlFor="tls-servername"
                  className="text-sm text-slate-700"
                >
                  TLS Server Name (SNI)
                </label>
                <input
                  id="tls-servername"
                  placeholder="broker.example.com"
                  value={cfg.tlsServerName || cfg.TLSServerName || ""}
                  onChange={(e) =>
                    setCfg({ ...cfg, tlsServerName: e.target.value })
                  }
                />
              </div>
              <div className="flex items-center gap-2">
                <input
                  id="tls-insecure"
                  type="checkbox"
                  checked={
                    !!(cfg.tlsInsecureSkipVerify ?? cfg.TLSInsecureSkipVerify)
                  }
                  onChange={(e) =>
                    setCfg({ ...cfg, tlsInsecureSkipVerify: e.target.checked })
                  }
                />
                <label
                  htmlFor="tls-insecure"
                  className="text-sm text-slate-700"
                >
                  InsecureSkipVerify (not recommended)
                </label>
              </div>
              <div className="flex flex-col gap-1">
                <label htmlFor="tls-ca" className="text-sm text-slate-700">
                  CA Certificate (PEM)
                </label>
                <textarea
                  id="tls-ca"
                  rows={4}
                  placeholder="-----BEGIN CERTIFICATE-----\n..."
                  value={cfg.tlsCA || cfg.TLSCA || ""}
                  onChange={(e) => setCfg({ ...cfg, tlsCA: e.target.value })}
                />
              </div>
              <div className="flex flex-col gap-1">
                <label htmlFor="tls-cert" className="text-sm text-slate-700">
                  Client Certificate (PEM)
                </label>
                <textarea
                  id="tls-cert"
                  rows={4}
                  placeholder="-----BEGIN CERTIFICATE-----\n..."
                  value={cfg.tlsCert || cfg.TLSCert || ""}
                  onChange={(e) => setCfg({ ...cfg, tlsCert: e.target.value })}
                />
              </div>
              <div className="flex flex-col gap-1">
                <label htmlFor="tls-key" className="text-sm text-slate-700">
                  Client Private Key (PEM)
                </label>
                <textarea
                  id="tls-key"
                  rows={4}
                  placeholder="-----BEGIN PRIVATE KEY-----\n..."
                  value={cfg.tlsKey || cfg.TLSKey || ""}
                  onChange={(e) => setCfg({ ...cfg, tlsKey: e.target.value })}
                />
              </div>
            </div>
          )}
          {(cfg.securityProtocol || cfg.SecurityProtocol || "").startsWith(
            "SASL_"
          ) &&
            (cfg.saslMechanism || cfg.SASLMechanism || "PLAIN").startsWith(
              "SCRAM-"
            ) && (
              <div className="grid grid-cols-1 gap-3">
                <div className="flex flex-col gap-1">
                  <label htmlFor="sasl-user" className="text-sm text-slate-700">
                    SASL Username
                  </label>
                  <input
                    id="sasl-user"
                    value={cfg.saslUsername || cfg.SASLUsername || ""}
                    onChange={(e) =>
                      setCfg({ ...cfg, saslUsername: e.target.value })
                    }
                  />
                </div>
                <div className="flex flex-col gap-1">
                  <label htmlFor="sasl-pass" className="text-sm text-slate-700">
                    SASL Password
                  </label>
                  <input
                    id="sasl-pass"
                    type="password"
                    value={cfg.saslPassword || cfg.SASLPassword || ""}
                    onChange={(e) =>
                      setCfg({ ...cfg, saslPassword: e.target.value })
                    }
                  />
                </div>
              </div>
            )}
          {(cfg.securityProtocol || cfg.SecurityProtocol || "").startsWith(
            "SASL_"
          ) &&
            (cfg.saslMechanism || cfg.SASLMechanism || "PLAIN") === "PLAIN" && (
              <div className="grid grid-cols-1 gap-3">
                <div className="flex flex-col gap-1">
                  <label htmlFor="sasl-user" className="text-sm text-slate-700">
                    SASL Username
                  </label>
                  <input
                    id="plain-user"
                    value={cfg.saslUsername || cfg.SASLUsername || ""}
                    onChange={(e) =>
                      setCfg({ ...cfg, saslUsername: e.target.value })
                    }
                  />
                </div>
                <div className="flex flex-col gap-1">
                  <label htmlFor="sasl-pass" className="text-sm text-slate-700">
                    SASL Password
                  </label>
                  <input
                    id="plain-pass"
                    type="password"
                    value={cfg.saslPassword || cfg.SASLPassword || ""}
                    onChange={(e) =>
                      setCfg({ ...cfg, saslPassword: e.target.value })
                    }
                  />
                </div>
              </div>
            )}
          {(cfg.securityProtocol || cfg.SecurityProtocol || "").startsWith(
            "SASL_"
          ) &&
            (cfg.saslMechanism || cfg.SASLMechanism || "") === "GSSAPI" && (
              <div className="grid grid-cols-1 gap-3">
                <div className="flex flex-col gap-1">
                  <label htmlFor="gss-realm" className="text-sm text-slate-700">
                    Realm
                  </label>
                  <input
                    id="gss-realm"
                    value={cfg.gssapiRealm || cfg.GSSAPIRealm || ""}
                    onChange={(e) =>
                      setCfg({ ...cfg, gssapiRealm: e.target.value })
                    }
                  />
                </div>
                <div className="flex flex-col gap-1">
                  <label htmlFor="gss-svc" className="text-sm text-slate-700">
                    Service Name
                  </label>
                  <input
                    id="gss-svc"
                    placeholder="kafka"
                    value={cfg.gssapiServiceName || cfg.GSSAPIServiceName || ""}
                    onChange={(e) =>
                      setCfg({ ...cfg, gssapiServiceName: e.target.value })
                    }
                  />
                </div>
                <div className="flex flex-col gap-1">
                  <label htmlFor="gss-auth" className="text-sm text-slate-700">
                    Auth Type
                  </label>
                  <select
                    id="gss-auth"
                    value={cfg.gssapiAuthType || cfg.GSSAPIAuthType || "USER"}
                    onChange={(e) =>
                      setCfg({ ...cfg, gssapiAuthType: e.target.value })
                    }
                  >
                    <option value="USER">USER</option>
                    <option value="KEYTAB">KEYTAB</option>
                  </select>
                </div>
                {(cfg.gssapiAuthType || cfg.GSSAPIAuthType || "USER") ===
                  "USER" && (
                  <>
                    <div className="flex flex-col gap-1">
                      <label
                        htmlFor="gss-user"
                        className="text-sm text-slate-700"
                      >
                        Username
                      </label>
                      <input
                        id="gss-user"
                        value={cfg.gssapiUsername || cfg.GSSAPIUsername || ""}
                        onChange={(e) =>
                          setCfg({ ...cfg, gssapiUsername: e.target.value })
                        }
                      />
                    </div>
                    <div className="flex flex-col gap-1">
                      <label
                        htmlFor="gss-pass"
                        className="text-sm text-slate-700"
                      >
                        Password
                      </label>
                      <input
                        id="gss-pass"
                        type="password"
                        value={cfg.gssapiPassword || cfg.GSSAPIPassword || ""}
                        onChange={(e) =>
                          setCfg({ ...cfg, gssapiPassword: e.target.value })
                        }
                      />
                    </div>
                  </>
                )}
                {(cfg.gssapiAuthType || cfg.GSSAPIAuthType || "USER") ===
                  "KEYTAB" && (
                  <div className="flex flex-col gap-1">
                    <label
                      htmlFor="gss-keytab"
                      className="text-sm text-slate-700"
                    >
                      Keytab Path (inside container)
                    </label>
                    <input
                      id="gss-keytab"
                      placeholder="/etc/keytabs/app.keytab"
                      value={cfg.gssapiKeytabPath || cfg.GSSAPIKeytabPath || ""}
                      onChange={(e) =>
                        setCfg({ ...cfg, gssapiKeytabPath: e.target.value })
                      }
                    />
                  </div>
                )}
                <div className="flex flex-col gap-1">
                  <label htmlFor="gss-krb5" className="text-sm text-slate-700">
                    krb5.conf Path (inside container)
                  </label>
                  <input
                    id="gss-krb5"
                    placeholder="/etc/krb5.conf"
                    value={
                      cfg.gssapiKerberosConfigPath ||
                      cfg.GSSAPIKerberosConfigPath ||
                      ""
                    }
                    onChange={(e) =>
                      setCfg({
                        ...cfg,
                        gssapiKerberosConfigPath: e.target.value,
                      })
                    }
                  />
                </div>
                <div className="flex items-center gap-2">
                  <input
                    id="gss-pafxfast"
                    type="checkbox"
                    checked={
                      !!(cfg.gssapiDisablePAFXFAST ?? cfg.GSSAPIDisablePAFXFAST)
                    }
                    onChange={(e) =>
                      setCfg({
                        ...cfg,
                        gssapiDisablePAFXFAST: e.target.checked,
                      })
                    }
                  />
                  <label
                    htmlFor="gss-pafxfast"
                    className="text-sm text-slate-700"
                  >
                    Disable PAFXFAST
                  </label>
                </div>
              </div>
            )}
        </div>
        <div className="mt-4 flex gap-2">
          <button onClick={save}>Save</button>
          <button
            className="bg-slate-200 text-slate-900 hover:bg-slate-300"
            onClick={validate}
          >
            Validate
          </button>
          <button onClick={infer}>Sample & Infer</button>
          {sample.length > 0 && typeof id === "string" && (
            <button
              className="bg-indigo-600 text-white hover:bg-indigo-700"
              onClick={async () => {
                try {
                  await api.savePipelineMapping(id as string, {
                    columns: sample,
                  });
                  setStatus("Inferred mapping saved");
                } catch (e: any) {
                  setErr(String(e));
                }
              }}
            >
              Use Inferred Mapping
            </button>
          )}
        </div>
        {sample.length > 0 && (
          <div className="mt-6">
            <h2>Sample Fields</h2>
            <div className="overflow-hidden rounded-lg border border-slate-200 bg-white">
              <table className="w-full">
                <thead className="bg-slate-50">
                  <tr>
                    <th className="text-left">Field</th>
                    <th className="text-left">Column</th>
                    <th className="text-left">Type</th>
                  </tr>
                </thead>
                <tbody>
                  {sample.map((f, i) => (
                    <tr key={i}>
                      <td>
                        <code className="text-xs">{f.fieldPath}</code>
                      </td>
                      <td>{f.column}</td>
                      <td>{f.type}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        )}
      </div>
    </main>
  );
}
