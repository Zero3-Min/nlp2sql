const $ = (sel) => document.querySelector(sel);
const $$ = (sel) => Array.from(document.querySelectorAll(sel));

function clearJudge() {
  const wrap = $("#judgeResults");
  if (wrap) wrap.innerHTML = '';
}

function createBadge(passed, text) {
  const span = document.createElement('span');
  span.className = `judge-badge ${passed ? 'badge-pass' : 'badge-fail'}`;
  span.textContent = text || (passed ? '通过' : '失败');
  return span;
}

function renderJudge(judgeRes) {
  const container = $("#judgeResults");
  if (!container) return;
  container.innerHTML = '';
  if (!judgeRes || !Array.isArray(judgeRes.iterations)) return;

  const last = judgeRes.last_judge || judgeRes.iterations[judgeRes.iterations.length - 1] || {};
  const summary = document.createElement('div');
  summary.className = `judge-summary ${last.valid ? 'summary-pass' : 'summary-fail'}`;
  const title = document.createElement('div');
  title.className = 'judge-summary-title';
  title.textContent = last.valid ? 'SQL 校验通过' : 'SQL 校验未通过';
  summary.appendChild(title);
  if (last.reason) {
    const reason = document.createElement('div');
    reason.className = 'judge-summary-text';
    reason.textContent = `说明：${last.reason}`;
    summary.appendChild(reason);
  }
  if (!last.valid && last.fix_suggestion) {
    const fix = document.createElement('div');
    fix.className = 'judge-summary-text';
    fix.textContent = `修复建议：${last.fix_suggestion}`;
    summary.appendChild(fix);
  }
  container.appendChild(summary);

  const layerOrder = [
    ['syntax', '语法校验'],
    ['semantic', '语义一致性'],
    ['sql2nl', 'SQL → 自然语言'],
    ['embedding', 'Embedding 相似度'],
    ['execution', '可执行性预检'],
  ];

  judgeRes.iterations.forEach((iteration, idx) => {
    const card = document.createElement('div');
    card.className = 'judge-iteration';

    const header = document.createElement('div');
    header.className = 'judge-iteration-header';
    const title = document.createElement('div');
    title.textContent = `第 ${iteration.iteration || idx + 1} 轮判别`;
    header.appendChild(title);
    header.appendChild(createBadge(!!iteration.valid));
    card.appendChild(header);

    if (iteration.sql) {
      const sqlBlock = document.createElement('pre');
      sqlBlock.className = 'judge-sql';
      sqlBlock.textContent = iteration.sql;
      card.appendChild(sqlBlock);
    }

    const details = iteration.details || {};
    layerOrder.forEach(([key, label]) => {
      const layer = details[key];
      if (!layer) return;
      const row = document.createElement('div');
      row.className = 'judge-layer';
      row.appendChild(createBadge(!!layer.valid, layer.valid ? '通过' : '失败'));

      const body = document.createElement('div');
      body.className = 'judge-layer-body';
      const heading = document.createElement('div');
      heading.className = 'judge-layer-title';
      heading.textContent = label;
      body.appendChild(heading);

      if (key === 'embedding') {
        const score = typeof layer.score === 'number' ? layer.score : iteration.semantic_similarity;
        const threshold = layer.threshold;
        const text = document.createElement('div');
        const parts = [];
        if (typeof score === 'number') parts.push(`相似度：${score.toFixed(3)}`);
        if (typeof threshold === 'number') parts.push(`阈值：${threshold.toFixed(2)}`);
        text.textContent = parts.join('，');
        body.appendChild(text);
      } else if (key === 'sql2nl') {
        if (layer.explanation) {
          const text = document.createElement('div');
          text.textContent = layer.explanation;
          body.appendChild(text);
        }
      } else if (key === 'semantic') {
        if (layer.reason) {
          const text = document.createElement('div');
          text.textContent = `判定：${layer.reason}`;
          body.appendChild(text);
        }
        if (layer.fix_suggestion) {
          const fix = document.createElement('div');
          fix.textContent = `建议：${layer.fix_suggestion}`;
          body.appendChild(fix);
        }
      } else if (key === 'syntax') {
        if (Array.isArray(layer.errors) && layer.errors.length) {
          const text = document.createElement('div');
          text.textContent = layer.errors.join('；');
          body.appendChild(text);
        }
        if (Array.isArray(layer.columns_used) && layer.columns_used.length) {
          const text = document.createElement('div');
          text.textContent = `使用字段：${layer.columns_used.join(', ')}`;
          body.appendChild(text);
        }
      } else if (key === 'execution') {
        if (layer.method) {
          const text = document.createElement('div');
          text.textContent = `检查方式：${layer.method}`;
          body.appendChild(text);
        }
      }

      if (Array.isArray(layer.errors) && layer.errors.length && key !== 'syntax') {
        const err = document.createElement('div');
        err.textContent = layer.errors.join('；');
        body.appendChild(err);
      }
      row.appendChild(body);
      card.appendChild(row);
    });

    if (iteration.fix_suggestion) {
      const fix = document.createElement('div');
      fix.className = 'judge-fix';
      fix.textContent = `修复建议：${iteration.fix_suggestion}`;
      card.appendChild(fix);
    }
    if (iteration.sql_nl_explanation && (!details.sql2nl || !details.sql2nl.explanation)) {
      const explain = document.createElement('div');
      explain.className = 'judge-explain';
      explain.textContent = iteration.sql_nl_explanation;
      card.appendChild(explain);
    }
    if (typeof iteration.semantic_similarity === 'number' && (!details.embedding || typeof details.embedding.score !== 'number')) {
      const scoreLine = document.createElement('div');
      scoreLine.className = 'judge-score';
      scoreLine.textContent = `Embedding 相似度：${iteration.semantic_similarity.toFixed(3)}`;
      card.appendChild(scoreLine);
    }

    container.appendChild(card);
  });
}

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
  clearJudge();
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
  $('#txtSQL').value = data.sql || '';
  renderJudge(data.judge);
  if (!data.ok) {
    const last = data.judge?.last_judge || {};
    const msgs = [data.error || '生成失败'];
    if (last.reason) msgs.push(`原因: ${last.reason}`);
    if (last.fix_suggestion) msgs.push(`建议: ${last.fix_suggestion}`);
    alert(msgs.join('\n'));
    return;
  }
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
