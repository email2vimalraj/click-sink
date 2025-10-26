import React, { useCallback, useEffect, useState } from "react";
import Link from "next/link";
import { api } from "../../lib/api";
import * as Dialog from "@radix-ui/react-dialog";
import * as AlertDialog from "@radix-ui/react-alert-dialog";

export default function PipelinesPage() {
  const [list, setList] = useState<any[]>([]);
  const [name, setName] = useState("");
  const [description, setDescription] = useState("");
  const [err, setErr] = useState<string | undefined>();

  const refresh = useCallback(() => {
    api
      .listPipelines()
      .then(setList)
      .catch((e) => setErr(String(e)));
  }, []);

  useEffect(() => {
    refresh();
  }, [refresh]);

  const create = async () => {
    try {
      if (!name.trim()) return;
      await api.createPipeline(name.trim(), description);
      setName("");
      setDescription("");
      refresh();
    } catch (e: any) {
      alert(String(e));
    }
  };

  const del = async (id: string) => {
    try {
      await api.deletePipeline(id);
      refresh();
    } catch (e: any) {
      alert(String(e));
    }
  };

  return (
    <main className="min-h-screen p-6">
      <div className="mx-auto max-w-5xl">
        <div className="mb-6 flex items-center justify-between">
          <div className="space-y-1">
            <p className="text-sm text-slate-600">
              <Link className="hover:underline" href="/">
                ← Home
              </Link>
            </p>
            <h1>Pipelines</h1>
          </div>
          <Dialog.Root>
            <Dialog.Trigger asChild>
              <button>Create pipeline</button>
            </Dialog.Trigger>
            <Dialog.Portal>
              <Dialog.Overlay className="fixed inset-0 z-40 bg-black/30" />
              <Dialog.Content className="fixed left-1/2 top-1/2 z-50 w-[92vw] max-w-md -translate-x-1/2 -translate-y-1/2 rounded-lg bg-white p-5 shadow-xl focus:outline-none">
                <Dialog.Title className="text-lg font-medium">
                  Create pipeline
                </Dialog.Title>
                <div className="mt-4 space-y-3">
                  <div className="flex flex-col gap-1">
                    <label htmlFor="pl-name" className="text-sm text-slate-700">
                      Name
                    </label>
                    <input
                      id="pl-name"
                      placeholder="name"
                      value={name}
                      onChange={(e) => setName(e.target.value)}
                    />
                  </div>
                  <div className="flex flex-col gap-1">
                    <label htmlFor="pl-desc" className="text-sm text-slate-700">
                      Description
                    </label>
                    <input
                      id="pl-desc"
                      placeholder="description"
                      value={description}
                      onChange={(e) => setDescription(e.target.value)}
                    />
                  </div>
                </div>
                <div className="mt-5 flex justify-end gap-2">
                  <Dialog.Close asChild>
                    <button onClick={create}>Create</button>
                  </Dialog.Close>
                  <Dialog.Close asChild>
                    <button className="bg-slate-200 text-slate-900 hover:bg-slate-300">
                      Cancel
                    </button>
                  </Dialog.Close>
                </div>
              </Dialog.Content>
            </Dialog.Portal>
          </Dialog.Root>
        </div>

        {err && <p className="mb-4 text-sm text-red-600">{err}</p>}

        <div className="overflow-hidden rounded-lg border border-slate-200 bg-white">
          <table className="w-full">
            <thead className="bg-slate-50">
              <tr>
                <th className="text-left">Name</th>
                <th className="text-left">Description</th>
                <th className="text-left">Status</th>
                <th className="text-left">Total Rows</th>
                <th className="text-left">Actions</th>
              </tr>
            </thead>
            <tbody>
              {list.map((p) => (
                <tr key={p.id}>
                  <td>
                    <Link
                      className="text-indigo-600 hover:underline"
                      href={`/pipelines/${p.id}/config`}
                    >
                      {p.name}
                    </Link>
                  </td>
                  <td>{p.description || ""}</td>
                  <td>
                    <span
                      className={
                        "inline-flex items-center rounded-full px-2 py-0.5 text-xs font-medium " +
                        (p.running
                          ? "bg-green-100 text-green-700"
                          : "bg-slate-100 text-slate-700")
                      }
                    >
                      {p.running ? "running" : "stopped"}
                    </span>
                  </td>
                  <td>{p.totalRows || 0}</td>
                  <td className="space-x-2">
                    <Link
                      className="text-indigo-600 hover:underline"
                      href={`/pipelines/${p.id}/config`}
                    >
                      Config
                    </Link>
                    <span className="text-slate-300">|</span>
                    <Link
                      className="text-indigo-600 hover:underline"
                      href={`/pipelines/${p.id}/mapping`}
                    >
                      Mapping
                    </Link>
                    <span className="text-slate-300">|</span>
                    <Link
                      className="text-indigo-600 hover:underline"
                      href={`/pipelines/${p.id}/run`}
                    >
                      Run
                    </Link>
                    <span className="text-slate-300">|</span>
                    <AlertDialog.Root>
                      <AlertDialog.Trigger asChild>
                        <button className="bg-red-600 hover:bg-red-500">
                          Delete
                        </button>
                      </AlertDialog.Trigger>
                      <AlertDialog.Portal>
                        <AlertDialog.Overlay className="fixed inset-0 z-40 bg-black/30" />
                        <AlertDialog.Content className="fixed left-1/2 top-1/2 z-50 w-[92vw] max-w-md -translate-x-1/2 -translate-y-1/2 rounded-lg bg-white p-5 shadow-xl focus:outline-none">
                          <AlertDialog.Title className="text-lg font-medium">
                            Delete pipeline?
                          </AlertDialog.Title>
                          <AlertDialog.Description className="mt-2 text-sm text-slate-600">
                            Are you sure you want to delete this pipeline? This
                            action cannot be undone.
                          </AlertDialog.Description>
                          <div className="mt-5 flex justify-end gap-2">
                            <AlertDialog.Cancel asChild>
                              <button className="bg-slate-200 text-slate-900 hover:bg-slate-300">
                                Cancel
                              </button>
                            </AlertDialog.Cancel>
                            <AlertDialog.Action asChild>
                              <button
                                className="bg-red-600 hover:bg-red-500"
                                onClick={() => del(p.id)}
                              >
                                Delete
                              </button>
                            </AlertDialog.Action>
                          </div>
                        </AlertDialog.Content>
                      </AlertDialog.Portal>
                    </AlertDialog.Root>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </main>
  );
}
