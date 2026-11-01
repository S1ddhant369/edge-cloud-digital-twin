import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import {
  Activity, AlertTriangle, ArrowDownToLine, ArrowUpRight, CheckCircle2,
  Cloud, Cpu, Database, Gauge, Layers3, Network, Radio, RefreshCw,
  Send, Server, Wifi, WifiOff, Zap
} from 'lucide-react'
import {
  Area, AreaChart, CartesianGrid, ResponsiveContainer, Tooltip, XAxis, YAxis
} from 'recharts'

const POLL_MS = 2000
const fmt = (n, digits = 0) => Number.isFinite(Number(n)) ? Number(n).toFixed(digits) : '—'
const timeLabel = (ts) => {
  const d = new Date(Number(ts) * 1000)
  return Number.isNaN(d.getTime()) ? '—' : d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' })
}

async function getJson(url) {
  const response = await fetch(url, { cache: 'no-store' })
  if (!response.ok) throw new Error(`${response.status} ${response.statusText}`)
  return response.json()
}
async function postJson(url, body) {
  const response = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  })
  const data = await response.json().catch(() => ({}))
  if (!response.ok) throw new Error(data.error || `${response.status} ${response.statusText}`)
  return data
}

function StatusPill({ online, label }) {
  return <span className={`status-pill ${online ? 'good' : 'bad'}`}>
    <span className="status-dot" />{label || (online ? 'Online' : 'Offline')}
  </span>
}

function MetricCard({ icon: Icon, label, value, detail, tone = 'blue' }) {
  return <div className="metric-card">
    <div className={`metric-icon ${tone}`}><Icon size={19} /></div>
    <div className="metric-copy">
      <div className="metric-label">{label}</div>
      <div className="metric-value">{value}</div>
      <div className="metric-detail">{detail}</div>
    </div>
  </div>
}

function SectionTitle({ icon: Icon, title, subtitle, right }) {
  return <div className="section-title">
    <div className="section-title-left">
      <span className="section-icon"><Icon size={17} /></span>
      <div><h2>{title}</h2>{subtitle && <p>{subtitle}</p>}</div>
    </div>
    {right}
  </div>
}

export default function App() {
  const [edgeHealth, setEdgeHealth] = useState(false)
  const [cloudHealth, setCloudHealth] = useState(false)
  const [edge, setEdge] = useState(null)
  const [cloudMetrics, setCloudMetrics] = useState(null)
  const [cloudState, setCloudState] = useState([])
  const [history, setHistory] = useState([])
  const [lastUpdated, setLastUpdated] = useState(null)
  const [loading, setLoading] = useState(true)
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState('')
  const [notice, setNotice] = useState('')
  const [sensorId, setSensorId] = useState('machine-01')
  const [sensorValue, setSensorValue] = useState('25.5')
  const seenEvents = useRef(new Set())

  const refresh = useCallback(async (quiet = false) => {
    if (!quiet) setLoading(true)
    try {
      const results = await Promise.allSettled([
        getJson('/api/edge/health'),
        getJson('/api/edge/metrics'),
        getJson('/api/cloud/health'),
        getJson('/api/cloud/metrics'),
        getJson('/api/cloud/state'),
      ])
      const [eh, em, ch, cm, cs] = results
      setEdgeHealth(eh.status === 'fulfilled' && eh.value.status === 'ok')
      setCloudHealth(ch.status === 'fulfilled' && ch.value.status === 'ok')
      if (em.status === 'fulfilled') setEdge(em.value)
      if (cm.status === 'fulfilled') setCloudMetrics(cm.value)
      if (cs.status === 'fulfilled') {
        const rows = Array.isArray(cs.value.state) ? cs.value.state : []
        setCloudState(rows)
        const fresh = rows
          .filter(row => row.event_id && !seenEvents.current.has(row.event_id))
          .map(row => {
            seenEvents.current.add(row.event_id)
            return { id: row.event_id, twin: row.twin_id, value: Number(row.value), ts: Number(row.event_ts) }
          })
        if (fresh.length) {
          setHistory(prev => [...prev, ...fresh].sort((a, b) => a.ts - b.ts).slice(-40))
        }
      }
      const failures = results.filter(r => r.status === 'rejected')
      if (failures.length === results.length) setError('Cannot reach the APIs. Check that Docker services are running.')
      else if (failures.length) setError('Some API endpoints are unavailable. Check the Edge and Cloud containers.')
      else setError('')
      setLastUpdated(new Date())
    } catch (e) {
      setError(e.message || 'Unable to refresh dashboard.')
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => {
    refresh()
    const id = setInterval(() => refresh(true), POLL_MS)
    return () => clearInterval(id)
  }, [refresh])

  const setNetwork = async (online) => {
    setBusy(true); setNotice(''); setError('')
    try {
      await postJson('/api/edge/network', { online })
      setNotice(`Network marked ${online ? 'online' : 'offline'} at the Edge.`)
      await refresh(true)
    } catch (e) { setError(`Network control failed: ${e.message}`) }
    finally { setBusy(false) }
  }

  const publishReading = async (e) => {
    e.preventDefault()
    const value = Number(sensorValue)
    if (!Number.isFinite(value)) { setError('Enter a valid numeric sensor value.'); return }
    setBusy(true); setNotice(''); setError('')
    try {
      const result = await postJson('/api/edge/publish', { twin_id: sensorId.trim() || 'machine-01', value, source: 'edge' })
      setNotice(`Sensor event accepted: ${result.event_id || 'event queued'}`)
      await refresh(true)
    } catch (err) { setError(`Could not publish sensor reading: ${err.message}`) }
    finally { setBusy(false) }
  }

  const pending = Number(edge?.pending_rows ?? 0)
  const maxRows = Number(edge?.buffer_max_rows ?? 500)
  const bufferPct = maxRows > 0 ? Math.min(100, (pending / maxRows) * 100) : 0
  const networkOnline = Boolean(edge?.network_online)
  const chartData = useMemo(() => history.map(item => ({
    ...item, label: timeLabel(item.ts), value: item.value,
  })), [history])
  const counts = cloudMetrics?.counts || {}
  const edgeStats = edge?.stats || {}

  return <div className="app-shell">
    <aside className="sidebar">
      <div className="brand"><div className="brand-mark"><Layers3 size={21} /></div><div><strong>TWIN<span>SYNC</span></strong><small>EDGE–CLOUD PLATFORM</small></div></div>
      <div className="side-label">MONITORING</div>
      <a className="nav-item active"><Gauge size={17} />Overview</a>
      <a className="nav-item"><Cpu size={17} />Digital twins</a>
      <a className="nav-item"><Network size={17} />Connectivity</a>
      <a className="nav-item"><Database size={17} />Data pipeline</a>
      <div className="sidebar-bottom">
        <div className="side-label">SYSTEM</div>
        <div className="infra-item"><span className="infra-dot" />Edge API <small>:8080</small></div>
        <div className="infra-item"><span className="infra-dot" />Cloud API <small>:8081</small></div>
        <div className="infra-item"><span className="infra-dot muted" />Kafka <small>:9092</small></div>
        <div className="infra-item"><span className="infra-dot muted" />Mosquitto <small>:1883</small></div>
        <div className="sidebar-foot">Digital Twin Monitor <span>v1.0</span></div>
      </div>
    </aside>

    <main className="main-content">
      <header className="topbar">
        <div><div className="eyebrow">EDGE–CLOUD INFRASTRUCTURE</div><h1>System overview</h1></div>
        <div className="top-actions">
          <span className="live-indicator"><span /> LIVE MONITORING</span>
          <button className="icon-button" onClick={() => refresh()} title="Refresh now"><RefreshCw size={17} /></button>
        </div>
      </header>

      {error && <div className="alert error"><AlertTriangle size={17} /><span>{error}</span></div>}
      {notice && <div className="alert success"><CheckCircle2 size={17} /><span>{notice}</span><button onClick={() => setNotice('')}>Dismiss</button></div>}

      <section className="status-grid">
        <div className="service-card">
          <div className="service-heading"><div className="service-symbol edge-symbol"><Cpu size={20} /></div><span>EDGE NODE</span><StatusPill online={edgeHealth} /></div>
          <div className="service-name">Edge computing layer</div>
          <div className="service-foot"><span>HTTP API</span><strong>localhost:8080</strong></div>
        </div>
        <div className="service-card">
          <div className="service-heading"><div className="service-symbol cloud-symbol"><Cloud size={20} /></div><span>CLOUD NODE</span><StatusPill online={cloudHealth} /></div>
          <div className="service-name">Cloud digital twin</div>
          <div className="service-foot"><span>HTTP API</span><strong>localhost:8081</strong></div>
        </div>
        <div className="service-card network-card">
          <div className="service-heading"><div className="service-symbol network-symbol"><Network size={20} /></div><span>EDGE NETWORK</span><StatusPill online={edgeHealth && networkOnline} label={!edgeHealth ? 'Unknown' : networkOnline ? 'Connected' : 'Disconnected'} /></div>
          <div className="service-name">Edge-to-cloud link</div>
          <div className="service-foot"><span>SYNC INTERVAL</span><strong>1 second</strong></div>
        </div>
      </section>

      <section className="metric-grid">
        <MetricCard icon={Database} label="BUFFERED EVENTS" value={`${edge?.buffer_total_rows ?? '—'}`} detail={`${pending} pending · max ${maxRows || '—'}`} tone="blue" />
        <MetricCard icon={ArrowUpRight} label="SENT TO KAFKA" value={edgeStats.sent_to_kafka ?? '—'} detail={`${edgeStats.sync_errors ?? 0} sync errors`} tone="green" />
        <MetricCard icon={Cloud} label="CLOUD STATE ROWS" value={cloudMetrics?.state_rows ?? '—'} detail={`${counts.applied ?? 0} events applied`} tone="violet" />
        <MetricCard icon={Zap} label="EVENTS RECEIVED" value={counts.received ?? '—'} detail={`${counts.duplicates ?? 0} duplicates detected`} tone="amber" />
      </section>

      <section className="content-grid">
        <div className="panel chart-panel">
          <SectionTitle icon={Activity} title="Sensor activity" subtitle="Latest cloud state updates observed by this dashboard" right={<span className="subtle-tag">LAST 40 UPDATES</span>} />
          {chartData.length ? <div className="chart-wrap">
            <ResponsiveContainer width="100%" height="100%">
              <AreaChart data={chartData} margin={{ top: 12, right: 12, left: -18, bottom: 0 }}>
                <defs><linearGradient id="sensorFill" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stopColor="#5b9cff" stopOpacity={0.28}/><stop offset="100%" stopColor="#5b9cff" stopOpacity={0.01}/></linearGradient></defs>
                <CartesianGrid stroke="#202d40" strokeDasharray="3 5" vertical={false} />
                <XAxis dataKey="label" tick={{ fill: '#8190a5', fontSize: 10 }} axisLine={false} tickLine={false} minTickGap={28} />
                <YAxis tick={{ fill: '#8190a5', fontSize: 10 }} axisLine={false} tickLine={false} />
                <Tooltip contentStyle={{ background: '#111c2c', border: '1px solid #2a3a50', borderRadius: 10, color: '#e8eef8' }} labelStyle={{ color: '#93a4ba' }} />
                <Area type="monotone" dataKey="value" name="Sensor value" stroke="#68a7ff" strokeWidth={2.2} fill="url(#sensorFill)" connectNulls />
              </AreaChart>
            </ResponsiveContainer>
          </div> : <div className="empty-chart"><Activity size={27}/><strong>Waiting for cloud events</strong><span>New readings will appear here after they reach the Cloud service.</span></div>}
          <div className="chart-legend"><span><i /> Cloud-applied sensor readings</span><span className="chart-note">Chart updates when a new event ID appears in /state</span></div>
        </div>

        <div className="panel buffer-panel">
          <SectionTitle icon={Database} title="Edge buffer" subtitle="Local event queue" />
          <div className="buffer-number"><strong>{pending}</strong><span>/ {maxRows || '—'} pending</span></div>
          <div className="progress-track"><div className={`progress-fill ${bufferPct > 85 ? 'warning' : ''}`} style={{ width: `${bufferPct}%` }} /></div>
          <div className="buffer-meta"><span>Pending utilization</span><strong>{fmt(bufferPct, 1)}%</strong></div>
          <div className="buffer-divider" />
          <div className="buffer-stat"><span><ArrowDownToLine size={15}/> Events received</span><strong>{edgeStats.received ?? '—'}</strong></div>
          <div className="buffer-stat"><span><ArrowUpRight size={15}/> Events evicted</span><strong>{edgeStats.evicted ?? '—'}</strong></div>
          <div className="buffer-stat"><span><RefreshCw size={15}/> Oldest pending age</span><strong>{fmt(edge?.oldest_pending_age_seconds, 1)}s</strong></div>
        </div>
      </section>

      <section className="content-grid lower-grid">
        <div className="panel table-panel">
          <SectionTitle icon={Layers3} title="Digital twin registry" subtitle={`${cloudState.length} twin records currently stored in Cloud`} right={<span className="subtle-tag">LIVE API DATA</span>} />
          <div className="table-scroll"><table><thead><tr><th>TWIN ID</th><th>VALUE</th><th>SOURCE</th><th>LAST EVENT</th><th>EVENT ID</th></tr></thead>
            <tbody>{cloudState.map(row => <tr key={row.twin_id}><td className="twin-name"><span className="machine-icon"><Cpu size={14}/></span>{row.twin_id}</td><td className="value-cell">{fmt(row.value, 2)}</td><td><span className="source-pill">{row.source}</span></td><td>{timeLabel(row.event_ts)}</td><td className="event-id">{row.event_id}</td></tr>)}
              {!cloudState.length && <tr><td colSpan="5" className="empty-row">{loading ? 'Loading digital twins…' : 'No digital twin records returned by Cloud API yet.'}</td></tr>}
            </tbody></table></div>
        </div>

        <div className="panel controls-panel">
          <SectionTitle icon={Radio} title="Experiment controls" subtitle="Send commands to the Edge API" />
          <div className="control-block">
            <div className="control-heading"><div><strong>Network simulation</strong><p>Toggle the Edge network flag</p></div><StatusPill online={networkOnline} label={networkOnline ? 'Online' : 'Offline'} /></div>
            <div className="button-row">
              <button className="action-button danger" disabled={busy || !edgeHealth || !networkOnline} onClick={() => setNetwork(false)}><WifiOff size={15}/> Disconnect</button>
              <button className="action-button primary" disabled={busy || !edgeHealth || networkOnline} onClick={() => setNetwork(true)}><Wifi size={15}/> Reconnect</button>
            </div>
            <p className="control-hint">Disconnecting pauses Kafka synchronization. Incoming MQTT readings remain buffered at the Edge until reconnection.</p>
          </div>
          <div className="control-separator" />
          <form className="control-block" onSubmit={publishReading}>
            <div className="control-heading"><div><strong>Publish sensor reading</strong><p>Send a test event through MQTT</p></div><span className="subtle-tag">POST /publish</span></div>
            <label className="field-label">Twin ID</label>
            <input value={sensorId} onChange={e => setSensorId(e.target.value)} placeholder="machine-01" />
            <label className="field-label">Sensor value</label>
            <div className="publish-row"><input type="number" step="any" value={sensorValue} onChange={e => setSensorValue(e.target.value)} /><button className="action-button primary" type="submit" disabled={busy || !edgeHealth}><Send size={15}/> Publish</button></div>
            <p className="control-hint">The event is published to MQTT topic twin/machine-01/reading and then buffered by the Edge subscriber.</p>
          </form>
        </div>
      </section>

      <footer className="footer"><span><span className="footer-dot" /> Auto-refresh every 2 seconds</span><span>{lastUpdated ? `Last updated ${lastUpdated.toLocaleTimeString()}` : 'Waiting for API connection'}</span><span>Edge-Cloud Digital Twin · React dashboard</span></footer>
    </main>
  </div>
}
