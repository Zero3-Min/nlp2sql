const $ = (sel) => document.querySelector(sel);
const $$ = (sel) => Array.from(document.querySelectorAll(sel));

function setProgress(steps) {
  const ol = $("#progress");
  if (!Array.isArray(steps)) return;
  for (const s of steps) {
    const li = document.createElement('li');
    li.textContent = s;
    ol.appendChild(li);
  }
}

function clearProgress() {
  $("#progress").innerHTML = '';
}

function renderTable(result) {
  const wrap = $("#resultTable");
  wrap.innerHTML = '';
  if (!result || !Array.isArray(result.columns)) {
    wrap.textContent = '无数据';
    return;
  }
  const table = document.createElement('table');
  const thead = document.createElement('thead');
  const trh = document.createElement('tr');
  for (const c of result.columns) {
    const th = document.createElement('th');
    th.textContent = c;
    trh.appendChild(th);
  }
  thead.appendChild(trh);
  table.appendChild(thead);

  const tbody = document.createElement('tbody');
  for (const row of result.rows || []) {
    const tr = document.createElement('tr');
    for (const cell of row) {
      const td = document.createElement('td');
      td.textContent = cell === null || cell === undefined ? '' : String(cell);
      tr.appendChild(td);
    }
    tbody.appendChild(tr);
  }
  table.appendChild(tbody);
  wrap.appendChild(table);
}

function setBusy(busy) {
  const overlay = document.getElementById('loading');
  overlay.style.display = busy ? 'flex' : 'none';
  // disable buttons to avoid double submissions
  ['#btnLoadDBs','#btnLoadTables','#btnGenSQL','#btnExec'].forEach(sel => {
    const el = $(sel);
    if (el) el.disabled = !!busy;
  });
}

async function fetchJSON(url, options) {
  setBusy(true);
  const resp = await fetch(url, options);
  let data = null;
  try {
    data = await resp.json();
  } catch (e) {
    setBusy(false);
    throw new Error('响应解析失败');
  }
  if (!resp.ok) {
    const msg = (data && data.error) ? data.error : resp.statusText;
    setBusy(false);
    throw new Error(msg || '请求失败');
  }
  setBusy(false);
  return data;
}

async function loadDatabases() {
  clearProgress();
  setProgress(['开始: 加载数据库']);
  const data = await fetchJSON('/api/databases');
  setProgress(data.steps);
  const sel = $('#selectDB');
  sel.innerHTML = '';
  for (const name of data.databases || []) {
    const opt = document.createElement('option');
    opt.value = name; opt.textContent = name;
    sel.appendChild(opt);
  }
}

async function loadTables() {
  clearProgress();
  const db = $('#selectDB').value;
  if (!db) { alert('请先选择数据库'); return; }
  setProgress([`开始: 加载 ${db} 的表`]);
  const data = await fetchJSON('/api/tables?db=' + encodeURIComponent(db));
  setProgress(data.steps);
  const sel = $('#selectTable');
  sel.innerHTML = '';
  for (const name of data.tables || []) {
    const opt = document.createElement('option');
    opt.value = name; opt.textContent = name;
    sel.appendChild(opt);
  }
}

async function generateSQL() {
  clearProgress();
  const db = $('#selectDB').value;
  const tb = $('#selectTable').value;
  const query = $('#txtQuery').value.trim();
  if (!db || !tb || !query) { alert('请先选择数据库/表并输入自然语言'); return; }
  setProgress(['开始: 生成 SQL']);
  const data = await fetchJSON('/api/generate_sql', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ database: db, table: tb, query })
  });
  setProgress(data.steps);
  if (!data.ok) {
    alert(data.error || '生成失败');
    return;
  }
  $('#txtSQL').value = data.sql || '';
  if (data.timing?.total) {
    setProgress([`总耗时: ${data.timing.total}s`]);
  }
}

async function executeSQL() {
  clearProgress();
  $('#resultTable').textContent = '';
  $('#analysisTable').textContent = '';
  $('#analysisReport').textContent = '';
  const db = $('#selectDB').value;
  const tb = $('#selectTable').value;
  const query = $('#txtQuery').value.trim();
  const sql = $('#txtSQL').value.trim();
  if (!db || !sql) { alert('缺少 database 或 SQL'); return; }
  setProgress(['开始: 执行 SQL']);
  const data = await fetchJSON('/api/execute', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ database: db, table: tb, query, sql })
  });
  setProgress(data.steps);
  if (!data.ok) {
    alert(data.error || '执行失败');
    return;
  }
  renderTable(data.result);
  $('#analysisTable').textContent = String(data.analysis?.table ?? '');
  $('#analysisReport').textContent = String(data.analysis?.report ?? '');
}

window.addEventListener('DOMContentLoaded', () => {
  $('#btnLoadDBs').addEventListener('click', () => loadDatabases().catch(err => alert(err.message)));
  $('#btnLoadTables').addEventListener('click', () => loadTables().catch(err => alert(err.message)));
  $('#btnGenSQL').addEventListener('click', () => generateSQL().catch(err => alert(err.message)));
  $('#btnExec').addEventListener('click', () => executeSQL().catch(err => alert(err.message)));
});
