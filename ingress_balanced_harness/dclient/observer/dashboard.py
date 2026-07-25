"""Single-file dashboard for the distributed observer, served at ``/``.

Same vanilla-JS/EventSource design as the single-node harness, extended for the
cluster: a per-broker panel (from summed OTel scrapes) sits alongside the
aggregate throughput, fleet, DLQ and history views. The observer aggregates
every broker's ``>`` firehose + ``_stats.>`` fleet plane and the shared Postgres
durable layer, so these numbers are cluster-wide.
"""

DASHBOARD_HTML = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>pubsub.py distributed</title>
<style>
  :root { color-scheme: dark; }
  * { box-sizing: border-box; }
  body { margin: 0; font: 14px/1.4 ui-monospace, SFMono-Regular, Menlo, monospace;
         background: #0e1116; color: #e6edf3; padding: 20px; }
  h1 { font-size: 18px; margin: 0 0 4px; }
  .sub { color: #7d8590; font-size: 12px; margin-bottom: 16px; }
  .grid { display: grid; gap: 12px; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); }
  .tile { background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 14px; }
  .tile .k { color: #7d8590; font-size: 11px; text-transform: uppercase; letter-spacing: .5px; }
  .tile .v { font-size: 26px; font-weight: 600; margin-top: 6px; }
  .tile .u { color: #7d8590; font-size: 12px; font-weight: 400; }
  .big .v { font-size: 34px; color: #3fb950; }
  section { margin-top: 20px; }
  section h2 { font-size: 13px; color: #7d8590; text-transform: uppercase;
               letter-spacing: .5px; margin: 0 0 8px; }
  table { width: 100%; border-collapse: collapse; }
  th, td { text-align: left; padding: 6px 8px; border-bottom: 1px solid #21262d; }
  th { color: #7d8590; font-weight: 500; font-size: 11px; text-transform: uppercase; }
  td.n, th.n { text-align: right; font-variant-numeric: tabular-nums; }
  .bar { height: 6px; background: #1f6feb; border-radius: 3px; }
  canvas { width: 100%; height: 80px; display: block; background: #0b0e13;
           border: 1px solid #30363d; border-radius: 8px; }
  .warn { background: #3d1e1e; border: 1px solid #f85149; color: #ffa198;
          padding: 8px 12px; border-radius: 8px; margin-bottom: 12px; display: none; }
  .ok { color: #3fb950; } .muted { color: #7d8590; } .down { color: #f85149; }
  .cols { display: grid; gap: 20px; grid-template-columns: 1fr 1fr; }
  @media (max-width: 720px){ .cols { grid-template-columns: 1fr; } }
  .pill { display:inline-block; padding:1px 7px; border-radius:10px; font-size:11px;
          background:#161b22; border:1px solid #30363d; margin-right:6px; }
</style>
</head>
<body>
  <h1>pubsub.py — distributed cluster</h1>
  <div class="sub" id="meta">connecting…</div>

  <div class="warn" id="satWarn"></div>
  <div class="warn" id="evictWarn"></div>

  <div class="grid">
    <div class="tile big"><div class="k">delivered / sec</div><div class="v"><span id="rateCur">0</span> <span class="u">now</span></div></div>
    <div class="tile"><div class="k">sustained (30s)</div><div class="v" id="rateSus">0<span class="u"> /s</span></div></div>
    <div class="tile"><div class="k">peak</div><div class="v" id="ratePeak">0<span class="u"> /s</span></div></div>
    <div class="tile"><div class="k">total delivered</div><div class="v" id="total">0</div></div>
    <div class="tile"><div class="k">latency p50 / p95 / p99</div><div class="v" id="lat">0 / 0 / 0<span class="u"> ms</span></div></div>
    <div class="tile"><div class="k">lifetime avg</div><div class="v" id="lifeAvg">0<span class="u"> /s</span></div></div>
    <div class="tile"><div class="k">dead-lettered</div><div class="v" id="dlqCount">0</div></div>
    <div class="tile"><div class="k">retained history</div><div class="v" id="hist">0</div></div>
  </div>

  <section>
    <h2>delivered / sec — last 60s (all brokers)</h2>
    <canvas id="spark" width="900" height="80"></canvas>
  </section>

  <section>
    <h2>brokers — per-broker OTel <span class="muted" id="brokerNote"></span></h2>
    <table><thead><tr><th>broker</th><th class="n">publish/s</th><th class="n">deliver/s</th><th class="n">ack/s</th><th class="n">nack/s</th><th class="n">rejected</th><th class="n">retry-exhausted</th></tr></thead>
    <tbody id="brokerRows"></tbody></table>
  </section>

  <section>
    <h2>fleet — live producers &amp; consumers (cluster-wide)</h2>
    <div class="grid">
      <div class="tile"><div class="k">producers</div><div class="v" id="nProd">0</div></div>
      <div class="tile"><div class="k">consumers</div><div class="v" id="nCons">0</div></div>
      <div class="tile"><div class="k">fleet publish / sec</div><div class="v" id="fPub">0<span class="u"> /s</span></div></div>
      <div class="tile"><div class="k">fleet ack / sec</div><div class="v" id="fAck">0<span class="u"> /s</span></div></div>
    </div>
    <div class="cols" style="margin-top:12px">
      <section>
        <h2>producers</h2>
        <table><thead><tr><th>id</th><th class="n">pub/s</th><th class="n">published</th><th class="n">err</th><th class="n">age</th></tr></thead>
        <tbody id="prodRows"></tbody></table>
      </section>
      <section>
        <h2>consumers (fan-in)</h2>
        <table><thead><tr><th>id</th><th class="n">ack/s</th><th class="n">nack/s</th><th class="n">att</th><th class="n">evict</th><th class="n">age</th></tr></thead>
        <tbody id="consRows"></tbody></table>
      </section>
    </div>
  </section>

  <section>
    <h2>cluster — aggregate OpenTelemetry <span class="muted" id="otelNote"></span></h2>
    <div class="grid">
      <div class="tile"><div class="k">publish accepted</div><div class="v" id="oPubOk">0<span class="u"> /s</span></div></div>
      <div class="tile"><div class="k">publish rejected</div><div class="v" id="oPubRej">0</div></div>
      <div class="tile"><div class="k">deliveries</div><div class="v" id="oDeliv">0<span class="u"> /s</span></div></div>
      <div class="tile"><div class="k">acks / nacks</div><div class="v" id="oAckNack">0 / 0<span class="u"> /s</span></div></div>
      <div class="tile"><div class="k">dead-lettered (retry exhausted)</div><div class="v" id="oDlq">0</div></div>
      <div class="tile"><div class="k">total publishes</div><div class="v" id="oPubTot">0</div></div>
    </div>
  </section>

  <div class="cols">
    <section>
      <h2>per-topic throughput</h2>
      <table><thead><tr><th>topic</th><th class="n">delivered</th><th style="width:40%">share</th></tr></thead>
      <tbody id="topics"></tbody></table>
    </section>
    <section>
      <h2>dead-letter queue (shared Postgres) <span class="muted" id="dlqNote"></span></h2>
      <table><thead><tr><th>topic</th><th class="n">attempts</th><th class="n">bytes</th></tr></thead>
      <tbody id="dlq"></tbody></table>
    </section>
  </div>

  <section>
    <h2>audit trail — rows persisted per broker <span class="muted">(shared Postgres; which machine handled it)</span></h2>
    <table><thead><tr><th>broker_id</th><th class="n">persisted</th><th class="n">dead-lettered</th></tr></thead>
    <tbody id="auditRows"></tbody></table>
  </section>

  <section>
    <h2>replayable topics</h2>
    <div id="reg" class="muted">—</div>
  </section>

<script>
const $ = id => document.getElementById(id);
function fmt(n){ return (n>=1000)? (n/1000).toFixed(n>=10000?0:1)+'k' : n; }

function drawSpark(data){
  const c = $('spark'), ctx = c.getContext('2d');
  const W = c.width, H = c.height; ctx.clearRect(0,0,W,H);
  if(!data || !data.length) return;
  const max = Math.max(1, ...data);
  const bw = W / data.length;
  for(let i=0;i<data.length;i++){
    const h = (data[i]/max) * (H-4);
    ctx.fillStyle = '#1f6feb';
    ctx.fillRect(i*bw+1, H-h, Math.max(1,bw-2), h);
  }
}

function render(s){
  const nb = s.broker.port;  // observer packs broker count into .port
  $('meta').textContent = `cluster of ${nb} broker(s) · up ${s.elapsed_s}s · `
    + `lifetime ${s.rate_lifetime}/s · seq ${s.highest_seq} · samples ${s.latency_ms.samples}`;
  $('rateCur').textContent = fmt(s.rate_current);
  $('rateSus').innerHTML = fmt(s.rate_sustained) + '<span class="u"> /s</span>';
  $('ratePeak').innerHTML = fmt(s.rate_peak) + '<span class="u"> /s</span>';
  $('total').textContent = fmt(s.total);
  const l = s.latency_ms;
  $('lat').innerHTML = `${l.p50} / ${l.p95} / ${l.p99}<span class="u"> ms</span>`;
  $('lifeAvg').innerHTML = fmt(s.rate_lifetime) + '<span class="u"> /s</span>';
  $('dlqCount').textContent = s.durable.dlq_count;
  $('hist').textContent = fmt(s.durable.history_count);

  const w = $('evictWarn');
  const parts = [];
  if(!s.sidecar.subscribed) parts.push('a broker firehose sub is down (re-subscribing)');
  if(s.sidecar.evictions>0) parts.push(`evicted ${s.sidecar.evictions}× — delivery rate under-reads true throughput at the ceiling; trust producers' reported rate`);
  if(parts.length){ w.style.display='block'; w.textContent='⚠ '+parts.join(' · '); }
  else w.style.display='none';

  drawSpark(s.sparkline);

  const h = s.health || {saturated:false};
  const sw = $('satWarn');
  if(h.saturated){ sw.style.display='block'; sw.textContent='⚠ '+h.msg; }
  else sw.style.display='none';

  // per-broker table
  const bs = s.brokers || [];
  $('brokerNote').textContent = bs.length ? `${bs.filter(b=>b.ok).length}/${bs.length} up` : '(no scrapes yet)';
  $('brokerRows').innerHTML = bs.map(b=>{
    if(!b.ok) return `<tr><td>${b.name}</td><td class="down" colspan="6">unreachable</td></tr>`;
    const r=b.rates||{}, t=b.totals||{};
    return `<tr><td>${b.name}</td><td class="n">${fmt(r.publishes_ok||0)}</td><td class="n">${fmt(r.deliveries||0)}</td>`
      + `<td class="n">${fmt(r.acks||0)}</td><td class="n">${fmt(r.nacks||0)}</td>`
      + `<td class="n">${fmt(t.publishes_rej||0)}</td><td class="n">${fmt(t.retry_exhausted||0)}</td></tr>`;
  }).join('') || '<tr><td class="muted" colspan="7">no brokers reporting</td></tr>';

  const o = s.broker_otel || {ok:false};
  if(o.ok){
    const t=o.totals, r=o.rates||{};
    $('otelNote').textContent = 'summed across all brokers';
    $('oPubOk').innerHTML = fmt(r.publishes_ok||0)+'<span class="u"> /s</span>';
    $('oPubRej').textContent = fmt(t.publishes_rej||0);
    $('oDeliv').innerHTML = fmt(r.deliveries||0)+'<span class="u"> /s</span>';
    $('oAckNack').innerHTML = fmt(r.acks||0)+' / '+fmt(r.nacks||0)+'<span class="u"> /s</span>';
    $('oDlq').textContent = fmt(t.retry_exhausted||0);
    $('oPubTot').textContent = fmt((t.publishes_ok||0)+(t.publishes_rej||0));
  } else {
    $('otelNote').textContent = '(no metrics endpoints reachable)';
  }

  const f = s.fleet;
  $('nProd').textContent = f.n_producers;
  $('nCons').textContent = f.n_consumers;
  $('fPub').innerHTML = fmt(f.pub_rate) + '<span class="u"> /s</span>';
  $('fAck').innerHTML = fmt(f.ack_rate) + '<span class="u"> /s</span>';
  $('prodRows').innerHTML = f.producers.map(p=>
    `<tr><td>${p.id}</td><td class="n">${fmt(p.rate)}</td><td class="n">${fmt(p.published)}</td><td class="n">${p.errors}</td><td class="n">${p.age}s</td></tr>`
  ).join('') || '<tr><td class="muted" colspan="5">none reporting</td></tr>';
  $('consRows').innerHTML = f.consumers.map(c=>
    `<tr><td>${c.id}</td><td class="n">${fmt(c.ack_rate)}</td><td class="n">${fmt(c.nack_rate)}</td><td class="n">${c.max_attempt}</td><td class="n">${c.evictions}</td><td class="n">${c.age}s</td></tr>`
  ).join('') || '<tr><td class="muted" colspan="6">none reporting</td></tr>';

  const tot = s.total || 1;
  $('topics').innerHTML = Object.entries(s.per_topic).map(([t,c])=>
    `<tr><td>${t}</td><td class="n">${fmt(c)}</td><td><div class="bar" style="width:${(100*c/tot).toFixed(1)}%"></div></td></tr>`
  ).join('') || '<tr><td class="muted" colspan="3">no traffic yet</td></tr>';

  const dlq = s.durable.dlq;
  $('dlqNote').textContent = dlq.length ? `(last ${dlq.length})` : '';
  $('dlq').innerHTML = dlq.slice().reverse().map(e=>
    `<tr><td>${e.topic}</td><td class="n">${e.attempts}</td><td class="n">${e.payload_bytes??'—'}</td></tr>`
  ).join('') || '<tr><td class="muted" colspan="3">empty</td></tr>';

  const audit = s.audit || [];
  $('auditRows').innerHTML = audit.map(a=>
    `<tr><td>${a.broker_id}</td><td class="n">${fmt(a.persisted)}</td><td class="n">${fmt(a.dlq)}</td></tr>`
  ).join('') || '<tr><td class="muted" colspan="3">no rows yet</td></tr>';

  $('reg').innerHTML = s.durable.topics.map(t=>
    `<span class="${t.replayable?'ok':'muted'}">${t.topic}</span>`
  ).join(' · ') || '—';
}

const es = new EventSource('/events');
es.onmessage = e => render(JSON.parse(e.data));
es.onerror = () => { $('meta').textContent = 'stream disconnected — retrying…'; };
</script>
</body>
</html>
"""
