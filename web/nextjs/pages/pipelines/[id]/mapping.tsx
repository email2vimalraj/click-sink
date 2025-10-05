import { useRouter } from 'next/router'
import { useEffect, useState } from 'react'
import Link from 'next/link'
import { api, Mapping } from '../../../lib/api'

export default function PipelineMapping(){
  const router = useRouter(); const { id } = router.query
  const [fields, setFields] = useState<{fieldPath:string;column:string;type:string}[]>([])
  const [mapping, setMapping] = useState<Mapping>({ columns: [] })
  const [err, setErr] = useState<string|undefined>()
  useEffect(()=>{ if (typeof id==='string'){ api.pipelineSample(id, 100).then(setFields).catch(e=>setErr(String(e))); api.getPipelineMapping(id).then(setMapping).catch(()=>{}) } }, [id])
  const add = (f:{fieldPath:string;column:string;type:string}) => setMapping(m=>({ columns:[...m.columns, { fieldPath:f.fieldPath, column:f.column, type:f.type }] }))
  const save = async () => { if (typeof id!=='string') return; try { await api.savePipelineMapping(id, mapping); alert('Saved') } catch(e:any){ alert(String(e)) } }
  return (
    <main style={{ padding: 24 }}>
      <p><Link href={`/pipelines`}>← Pipelines</Link> | <Link href={`/pipelines/${id}/config`}>Config</Link> | <Link href={`/pipelines/${id}/run`}>Run</Link></p>
      <h1>Pipeline {id} - Mapping</h1>
      {err && <p style={{color:'red'}}>{err}</p>}
      <table border={1} cellPadding={6} cellSpacing={0}>
        <thead><tr><th>Field</th><th>Column</th><th>Type</th><th></th></tr></thead>
        <tbody>
        {fields.map((f,i)=> (
          <tr key={i}>
            <td><code>{f.fieldPath}</code></td>
            <td><input defaultValue={f.column} onChange={e=>f.column=e.target.value} /></td>
            <td>
              <select defaultValue={f.type} onChange={e=>f.type=e.target.value}>
                {['String','Int64','Float64','Bool','Nullable(String)','Nullable(Int64)','Nullable(Float64)','Nullable(Bool)'].map(t=> <option key={t}>{t}</option>)}
              </select>
            </td>
            <td><button onClick={()=>add(f)}>Add</button></td>
          </tr>
        ))}
        </tbody>
      </table>
      <h2>Current Mapping</h2>
      <pre>{JSON.stringify(mapping, null, 2)}</pre>
      <button onClick={save}>Save Mapping</button>
    </main>
  )
}
