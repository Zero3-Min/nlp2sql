const $ = (sel) => document.querySelector(sel);
const chatWindow = $('#chat-window');
let chatHistory = [];
let currentDB = '';
let currentTable = '';

function setBusy(busy) {
  const overlay = document.getElementById('loading');
  overlay.style.display = busy ? 'flex' : 'none';
  $('#chat-input').disabled = !!busy;
  const btnDB = document.getElementById('btnSelectDB');
  const btnTB = document.getElementById('btnSelectTable');
  if (btnDB) btnDB.disabled = !!busy;
  if (btnTB) btnTB.disabled = !!busy;
}

function appendMessage(role, content, extra) {
  const msg = document.createElement('div');
  msg.className = 'message ' + role;
  const bubble = document.createElement('div');
  bubble.className = 'bubble';
  if (role === 'bot' && extra && extra.type === 'table') {
    bubble.appendChild(renderTable(extra.data));
  } else if (role === 'bot' && extra && extra.type === 'analysis') {
    bubble.appendChild(document.createElement('pre')).textContent = extra.data;
  } else {
    bubble.textContent = content;
  }
  msg.appendChild(bubble);
  chatWindow.appendChild(msg);
  chatWindow.scrollTop = chatWindow.scrollHeight;
}

function renderTable(result) {
  if (!result || !Array.isArray(result.columns)) {
    const div = document.createElement('div');
    div.textContent = '无数据';
    return div;
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
  return table;
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
  setBusy(false);
  if (!resp.ok) {
    const msg = (data && data.error) ? data.error : resp.statusText;
    throw new Error(msg || '请求失败');
  }
  return data;
}

async function loadDatabases() {
  const data = await fetchJSON('/api/databases');
  const sel = $('#selectDB');
  sel.innerHTML = '';
  for (const name of data.databases || []) {
    const opt = document.createElement('option');
    opt.value = name; opt.textContent = name;
    sel.appendChild(opt);
  }
  // 不自动选择，等待用户点击“选择数据库”
}


async function loadTables(selectedTable) {
  const db = $('#selectDB').value;
  if (!db) return;
  const data = await fetchJSON('/api/tables?db=' + encodeURIComponent(db));
  const sel = $('#selectTable');
  sel.innerHTML = '';
  let found = false;
  for (const name of data.tables || []) {
    const opt = document.createElement('option');
    opt.value = name; opt.textContent = name;
    if (selectedTable && name === selectedTable) {
      opt.selected = true;
      found = true;
    }
    sel.appendChild(opt);
  }
  // 不自动覆盖 currentTable，等待用户点击“选择数据表”
}


document.getElementById('btnSelectDB').addEventListener('click', async () => {
  const chosenDB = $('#selectDB').value;
  if (!chosenDB) { appendMessage('system', '请先选择数据库'); return; }
  currentDB = chosenDB;
  await loadTables();
  appendMessage('system', `已选择数据库：${currentDB}。请在下拉框选择数据表并点击“选择数据表”。`);
});

document.getElementById('btnSelectTable').addEventListener('click', async () => {
  const chosenTable = $('#selectTable').value;
  if (!chosenTable) { appendMessage('system', '请先选择数据表'); return; }
  currentTable = chosenTable;
  appendMessage('system', `已选择数据表：${currentTable}`);
});

$('#chat-form').addEventListener('submit', async (e) => {
  e.preventDefault();
  const input = $('#chat-input');
  const text = input.value.trim();
  if (!text) return;
  appendMessage('user', text);
  chatHistory.push({ role: 'user', content: text });
  input.value = '';
  try {
    const data = await fetchJSON('/api/chat', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        history: chatHistory,
        db: currentDB,
        table: currentTable
      })
    });
    for (const msg of data.messages) {
      if (msg.type === 'text') {
        appendMessage('bot', msg.content);
        chatHistory.push({ role: 'bot', content: msg.content });
      } else if (msg.type === 'sql') {
        appendMessage('bot', msg.content);
        chatHistory.push({ role: 'bot', content: msg.content });
      } else if (msg.type === 'judge') {
        // 以绿色/红色标记判别结果
        const ok = !!msg.valid;
        const pre = document.createElement('pre');
        pre.textContent = msg.content;
        const wrapper = document.createElement('div');
        wrapper.className = 'judge ' + (ok ? 'pass' : 'fail');
        wrapper.appendChild(pre);
        const div = document.createElement('div');
        div.className = 'message bot';
        const bubble = document.createElement('div');
        bubble.className = 'bubble';
        bubble.appendChild(wrapper);
        div.appendChild(bubble);
        chatWindow.appendChild(div);
        chatWindow.scrollTop = chatWindow.scrollHeight;
        chatHistory.push({ role: 'bot', content: '[判别]' });
      } else if (msg.type === 'table') {
        appendMessage('bot', '', { type: 'table', data: msg.data });
        chatHistory.push({ role: 'bot', content: '[表格结果]' });
      } else if (msg.type === 'analysis') {
        appendMessage('bot', '', { type: 'analysis', data: msg.data });
        chatHistory.push({ role: 'bot', content: '[分析报告]' });
      }
    }
  } catch (err) {
    appendMessage('system', err.message);
  }
});


window.addEventListener('DOMContentLoaded', async () => {
  await loadDatabases();
  appendMessage('system', '请先从左上角选择数据库，然后选择数据表。');
});
