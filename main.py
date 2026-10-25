import json
import os
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import List, Literal, Optional

import asyncpg
from dotenv import load_dotenv
from fastapi import FastAPI, Header, HTTPException, status
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
import uvicorn

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "admin128")

db_pool = None

# Кеш у пам'яті для уникнення навантаження на PostgreSQL
cached_active_poll = None

# Резервне збереження в пам'яті (якщо немає DATABASE_URL)
mem_polls = []
mem_votes = []

@asynccontextmanager
async def lifespan(app: FastAPI):
    global db_pool, cached_active_poll
    if DATABASE_URL:
        try:
            db_pool = await asyncpg.create_pool(
                DATABASE_URL,
                min_size=4,
                max_size=30,
                ssl="require"
            )
            async with db_pool.acquire() as conn:
                # Створюємо або оновлюємо таблиці під підтримку рейтингових опитувань
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS polls (
                        id TEXT PRIMARY KEY,
                        title TEXT NOT NULL,
                        poll_type TEXT NOT NULL DEFAULT 'standard',
                        options JSONB DEFAULT '[]'::jsonb,
                        is_active BOOLEAN NOT NULL DEFAULT TRUE,
                        created_at TIMESTAMPTZ NOT NULL
                    );
                    ALTER TABLE polls ADD COLUMN IF NOT EXISTS poll_type TEXT NOT NULL DEFAULT 'standard';
                    ALTER TABLE polls ADD COLUMN IF NOT EXISTS options JSONB DEFAULT '[]'::jsonb;

                    CREATE TABLE IF NOT EXISTS votes (
                        id TEXT PRIMARY KEY,
                        poll_id TEXT NOT NULL REFERENCES polls(id),
                        voter_number INT NOT NULL,
                        choice TEXT NOT NULL,
                        created_at TIMESTAMPTZ NOT NULL,
                        CONSTRAINT unique_voter_per_poll UNIQUE (poll_id, voter_number)
                    );
                """)
                
                # Підтягуємо активне опитування в кеш при запуску
                row = await conn.fetchrow("SELECT id, title, poll_type, options FROM polls WHERE is_active = TRUE ORDER BY created_at DESC LIMIT 1")
                if row:
                    opts = json.loads(row["options"]) if isinstance(row["options"], str) else (row["options"] or [])
                    cached_active_poll = {
                        "id": row["id"],
                        "title": row["title"],
                        "pollType": row["poll_type"],
                        "options": opts
                    }
            print("PostgreSQL успішно підключено та кеш ініціалізовано!")
        except Exception as e:
            print("Помилка підключення до PostgreSQL:", e)
    else:
        print("Працюємо в локальному режимі пам'яті.")
    yield
    if db_pool:
        await db_pool.close()

app = FastAPI(title="Voting App", lifespan=lifespan)

class VoteRequest(BaseModel):
    pollId: str
    voterNumber: int
    choice: Optional[str] = None
    selectedOptions: Optional[List[str]] = None

class CreatePollRequest(BaseModel):
    title: str
    pollType: Literal["standard", "ranking"] = "standard"
    options: Optional[List[str]] = []

def verify_admin(authorization: str = Header(None)):
    if authorization != f"Bearer {ADMIN_PASSWORD}":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Невірний пароль адміністратора",
        )

@app.get("/", response_class=HTMLResponse)
async def serve_index():
    return """<!DOCTYPE html>
<html lang="uk">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Голосування</title>
  <style>
    body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background: #eef2f7; margin: 0; padding: 20px; display: flex; flex-direction: column; align-items: center; min-height: 100vh; box-sizing: border-box; }
    .top-bar { width: 100%; max-width: 800px; display: flex; justify-content: flex-end; margin-bottom: 10px; }
    .btn-admin-toggle { background: #cbd5e1; color: #334155; border: none; padding: 6px 12px; border-radius: 6px; font-size: 0.85rem; font-weight: 600; cursor: pointer; transition: 0.2s; }
    .btn-admin-toggle:hover { background: #94a3b8; color: white; }
    
    .card { background: white; padding: 30px; border-radius: 16px; box-shadow: 0 10px 25px rgba(0,0,0,0.08); width: 100%; max-width: 460px; text-align: center; box-sizing: border-box; margin-bottom: 20px; }
    h2 { margin-top: 0; color: #1e293b; }
    .poll-title { font-size: 1.25rem; font-weight: 700; color: #0284c7; margin: 15px 0 10px 0; }
    .poll-badge { display: inline-block; font-size: 0.75rem; font-weight: 700; text-transform: uppercase; padding: 4px 8px; border-radius: 4px; background: #e0f2fe; color: #0369a1; margin-bottom: 15px; }
    
    input[type="number"], input[type="text"], input[type="password"], textarea, select { width: 100%; padding: 12px; font-size: 1rem; border: 2px solid #cbd5e1; border-radius: 10px; box-sizing: border-box; margin-bottom: 15px; outline: none; }
    input:focus, textarea:focus, select:focus { border-color: #0284c7; }
    
    .btn-group { display: flex; flex-direction: column; gap: 10px; }
    button.vote-btn { padding: 14px; font-size: 1.05rem; font-weight: bold; border: none; border-radius: 10px; cursor: pointer; color: white; transition: 0.15s; }
    button.vote-btn:disabled { opacity: 0.4; cursor: not-allowed; }
    .btn-yes { background: #16a34a; }
    .btn-no { background: #dc2626; }
    .btn-abstain { background: #64748b; }
    .btn-ranking-submit { background: #0284c7; }

    .ranking-list { text-align: left; margin: 15px 0; display: flex; flex-direction: column; gap: 10px; }
    .ranking-item { display: flex; align-items: center; gap: 10px; background: #f8fafc; padding: 10px 14px; border: 1px solid #e2e8f0; border-radius: 8px; cursor: pointer; user-select: none; }
    .ranking-item input[type="checkbox"] { width: 18px; height: 18px; cursor: pointer; }

    .msg { margin-top: 15px; font-weight: bold; padding: 12px; border-radius: 8px; font-size: 0.95rem; }
    .msg.success { background: #dcfce7; color: #15803d; }
    .msg.error { background: #fee2e2; color: #b91c1c; }

    /* Панель адміністратора */
    #adminPanel { display: none; width: 100%; max-width: 800px; text-align: left; }
    .admin-card { background: white; padding: 25px; border-radius: 16px; box-shadow: 0 10px 25px rgba(0,0,0,0.08); margin-bottom: 20px; }
    .admin-card h3 { margin-top: 0; color: #0f172a; }
    .stats-box { display: flex; gap: 10px; margin: 15px 0; flex-wrap: wrap; }
    .stat-item { flex: 1; min-width: 90px; padding: 10px; border-radius: 8px; text-align: center; color: white; }
    .stat-yes { background: #16a34a; }
    .stat-no { background: #dc2626; }
    .stat-abstain { background: #64748b; }
    .stat-rank { background: #0284c7; }
    .stat-number { font-size: 1.5rem; font-weight: bold; }
    table { width: 100%; border-collapse: collapse; margin-top: 15px; font-size: 0.9rem; }
    th, td { border: 1px solid #e2e8f0; padding: 8px 10px; text-align: left; }
    th { background: #f8fafc; }
    .btn-action { padding: 10px 16px; font-size: 0.95rem; font-weight: bold; border: none; border-radius: 8px; cursor: pointer; color: white; background: #0284c7; }
    .btn-danger { background: #ef4444; }
    .history-card { background: #f8fafc; border: 1px solid #e2e8f0; border-radius: 8px; padding: 12px; margin-bottom: 10px; }
    .history-badges { display: flex; gap: 6px; margin-top: 6px; font-weight: bold; font-size: 0.85rem; flex-wrap: wrap; }
    .badge { padding: 3px 6px; border-radius: 4px; }
    .badge-yes { background: #dcfce7; color: #166534; }
    .badge-no { background: #fee2e2; color: #991b1b; }
    .badge-abs { background: #f1f5f9; color: #475569; }
    .badge-rank { background: #e0f2fe; color: #0369a1; }
  </style>
</head>
<body>

  <div class="top-bar">
    <button class="btn-admin-toggle" onclick="toggleAdminPrompt()">⚙️ Адмін</button>
  </div>

  <!-- Блок голосування для користувачів -->
  <div class="card" id="userArea">
    <h2>🗳️ Електронне голосування</h2>
    <div id="noPoll" style="color: #64748b; font-size: 1.05rem;">Зараз немає активного голосування. Очікуйте...</div>

    <div id="pollArea" style="display: none;">
      <div id="pollTypeBadge" class="poll-badge"></div>
      <div class="poll-title" id="pollQuestion">Завантаження...</div>
      
      <input type="number" id="voterNumber" placeholder="Введіть ваш порядковий номер" min="1" style="text-align: center;">

      <!-- Варіант для стандартного опитування (За / Проти / Утримався) -->
      <div id="standardVoteButtons" class="btn-group" style="display: none;">
        <button class="vote-btn btn-yes" onclick="sendStandardVote('За')">👍 ЗА</button>
        <button class="vote-btn btn-no" onclick="sendStandardVote('Проти')">👎 ПРОТИ</button>
        <button class="vote-btn btn-abstain" onclick="sendStandardVote('Утримався')">✋ УТРИМАВСЯ</button>
      </div>

      <!-- Варіант для рейтингового опитування з вибором будь-яких варіантів -->
      <div id="rankingVoteArea" style="display: none;">
        <div style="font-size: 0.9rem; color: #475569; margin-bottom: 10px;">Оберіть усі бажані варіанти:</div>
        <div class="ranking-list" id="rankingOptionsList"></div>
        <button class="vote-btn btn-ranking-submit" style="width: 100%;" onclick="sendRankingVote()">✅ Підтвердити вибір</button>
      </div>
    </div>

    <div id="feedback" class="msg" style="display: none;"></div>
  </div>

  <!-- Панель керування адміна -->
  <div id="adminPanel">
    <div class="admin-card">
      <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 15px;">
        <h3>👑 Панель ведучого</h3>
        <button class="btn-admin-toggle" onclick="logoutAdmin()">Вийти з адмінки</button>
      </div>

      <label style="font-weight: bold; font-size: 0.9rem;">Тип голосування:</label>
      <select id="newPollType" onchange="toggleOptionsInput()">
        <option value="standard">Звичайне (За / Проти / Утримався)</option>
        <option value="ranking">Рейтингове (Вибір кількох кандидатів/варіантів)</option>
      </select>

      <textarea id="newQuestion" placeholder="Введіть формулювання питання чи посади..." rows="2"></textarea>

      <div id="rankingInputArea" style="display: none;">
        <label style="font-weight: bold; font-size: 0.9rem;">Варіанти вибору (кожен з нового рядка):</label>
        <textarea id="rankingOptionsText" placeholder="Кандидат 1&#10;Кандидат 2&#10;Кандидат 3" rows="4"></textarea>
      </div>

      <div style="display: flex; gap: 10px; margin-top: 10px;">
        <button class="btn-action" onclick="createPoll()">🚀 Запустити голосування</button>
        <button class="btn-action btn-danger" onclick="closePoll()">🛑 Закрити поточне</button>
      </div>
    </div>

    <div class="admin-card">
      <h3 id="currentPollTitle">Немає створених опитувань</h3>
      <p id="pollStatus" style="font-weight: bold; margin: 5px 0 15px 0;"></p>

      <div class="stats-box" id="adminStatsBox"></div>
      <p>Всього проголосувало учасників: <strong id="totalVotes">0</strong></p>

      <h4>Протокол голосування:</h4>
      <table>
        <thead><tr><th style="width: 90px;">Номер</th><th>Вибір</th></tr></thead>
        <tbody id="votersTable"></tbody>
      </table>
    </div>

    <div class="admin-card">
      <h3>📚 Архів усіх голосувань</h3>
      <div id="historyList" style="color: #64748b;">Завантаження...</div>
    </div>
  </div>

  <script>
    let currentPollId = null;
    let currentPollType = 'standard';
    let activePollId = null;
    let adminToken = localStorage.getItem('voting_admin_pass') || '';

    function toggleOptionsInput() {
      const type = document.getElementById('newPollType').value;
      document.getElementById('rankingInputArea').style.display = type === 'ranking' ? 'block' : 'none';
    }

    // Перевірка активного опитування для учасників
    async function checkPoll() {
      try {
        const res = await fetch('/api/poll/active');
        const data = await res.json();
        const noPoll = document.getElementById('noPoll');
        const pollArea = document.getElementById('pollArea');

        if (data.active) {
          if (currentPollId !== data.poll.id) {
            currentPollId = data.poll.id;
            currentPollType = data.poll.pollType || 'standard';
            
            document.getElementById('pollQuestion').innerText = data.poll.title;
            document.getElementById('voterNumber').disabled = false;
            document.getElementById('voterNumber').value = '';
            document.getElementById('feedback').style.display = 'none';

            const badge = document.getElementById('pollTypeBadge');
            const stdButtons = document.getElementById('standardVoteButtons');
            const rankArea = document.getElementById('rankingVoteArea');

            if (currentPollType === 'ranking') {
              badge.innerText = 'Рейтингове голосування';
              stdButtons.style.display = 'none';
              rankArea.style.display = 'block';

              const list = document.getElementById('rankingOptionsList');
              list.innerHTML = '';
              (data.poll.options || []).forEach((opt, idx) => {
                list.innerHTML += `
                  <label class="ranking-item">
                    <input type="checkbox" name="rankChoice" value="${opt}">
                    <span>${opt}</span>
                  </label>
                `;
              });
            } else {
              badge.innerText = 'Звичайне голосування';
              stdButtons.style.display = 'flex';
              rankArea.style.display = 'none';
              document.querySelectorAll('.vote-btn').forEach(b => b.disabled = false);
            }
          }
          noPoll.style.display = 'none';
          pollArea.style.display = 'block';
        } else {
          currentPollId = null;
          noPoll.style.display = 'block';
          pollArea.style.display = 'none';
        }
      } catch (err) {
        console.error(err);
      }
    }

    async function sendStandardVote(choice) {
      const voterInput = document.getElementById('voterNumber');
      const voterNumber = voterInput.value.trim();
      if (!voterNumber) return showMsg('Введіть свій порядковий номер!', 'error');

      const buttons = document.querySelectorAll('.vote-btn');
      buttons.forEach(b => b.disabled = true);

      await submitVote({
        pollId: currentPollId,
        voterNumber: parseInt(voterNumber, 10),
        choice: choice
      }, () => buttons.forEach(b => b.disabled = false));
    }

    async function sendRankingVote() {
      const voterInput = document.getElementById('voterNumber');
      const voterNumber = voterInput.value.trim();
      if (!voterNumber) return showMsg('Введіть свій порядковий номер!', 'error');

      const checked = Array.from(document.querySelectorAll('input[name="rankChoice"]:checked')).map(el => el.value);
      if (checked.length === 0) {
        return showMsg('Оберіть хоча б одного кандидата/варіант!', 'error');
      }

      const submitBtn = document.querySelector('.btn-ranking-submit');
      submitBtn.disabled = true;

      await submitVote({
        pollId: currentPollId,
        voterNumber: parseInt(voterNumber, 10),
        selectedOptions: checked
      }, () => submitBtn.disabled = false);
    }

    async function submitVote(payload, onError) {
      try {
        const res = await fetch('/api/vote', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify(payload)
        });
        const data = await res.json();
        if (res.ok) {
          showMsg('Ваш вибір успішно зараховано!', 'success');
          document.getElementById('voterNumber').disabled = true;
          document.querySelectorAll('input[name="rankChoice"]').forEach(c => c.disabled = true);
        } else {
          showMsg(data.detail || 'Помилка при збереженні голосу', 'error');
          if (onError) onError();
        }
      } catch (err) {
        showMsg("Помилка з'єднання із сервером", 'error');
        if (onError) onError();
      }
    }

    function showMsg(text, type) {
      const fb = document.getElementById('feedback');
      fb.innerText = text;
      fb.className = `msg ${type}`;
      fb.style.display = 'block';
    }

    // --- АДМІНІСТРАТОР ---
    function toggleAdminPrompt() {
      const panel = document.getElementById('adminPanel');
      if (panel.style.display === 'block') {
        panel.style.display = 'none';
        return;
      }

      if (!adminToken) {
        const pass = prompt('Введіть пароль адміністратора:');
        if (!pass) return;
        adminToken = pass;
      }
      loadAdminData(true);
    }

    function logoutAdmin() {
      adminToken = '';
      localStorage.removeItem('voting_admin_pass');
      document.getElementById('adminPanel').style.display = 'none';
    }

    function getAdminHeaders() {
      return { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + adminToken };
    }

    async function loadAdminData(initial = false) {
      if (!adminToken) return;

      try {
        const res = await fetch('/api/admin/stats', { headers: getAdminHeaders() });
        if (res.status === 401) {
          if (initial) alert('Невірний пароль!');
          adminToken = '';
          localStorage.removeItem('voting_admin_pass');
          document.getElementById('adminPanel').style.display = 'none';
          return;
        }

        if (initial) {
          localStorage.setItem('voting_admin_pass', adminToken);
          document.getElementById('adminPanel').style.display = 'block';
        }

        const data = await res.json();
        if (data.poll) {
          activePollId = data.poll.id;
          document.getElementById('currentPollTitle').innerText = data.poll.title;
          document.getElementById('pollStatus').innerText = data.poll.isActive ? '🟢 Голосування триває' : '🔴 Голосування закрито';
          document.getElementById('totalVotes').innerText = data.totalVotes;

          const statsBox = document.getElementById('adminStatsBox');
          statsBox.innerHTML = '';
          for (const [key, count] of Object.entries(data.stats)) {
            let colorClass = 'stat-rank';
            if (key === 'За') colorClass = 'stat-yes';
            if (key === 'Проти') colorClass = 'stat-no';
            if (key === 'Утримався') colorClass = 'stat-abstain';

            statsBox.innerHTML += `
              <div class="stat-item ${colorClass}">
                <div style="font-size: 0.85rem;">${key}</div>
                <div class="stat-number">${count}</div>
              </div>
            `;
          }

          const table = document.getElementById('votersTable');
          table.innerHTML = '';
          data.votersList.forEach(v => {
            table.innerHTML += `<tr><td>№ ${v.voterNumber}</td><td><b>${v.choice}</b></td></tr>`;
          });
        }

        const resHist = await fetch('/api/admin/history', { headers: getAdminHeaders() });
        if (resHist.ok) {
          const list = await resHist.json();
          const historyContainer = document.getElementById('historyList');
          if (!list || list.length === 0) {
            historyContainer.innerText = 'Історія опитувань порожня.';
          } else {
            historyContainer.innerHTML = '';
            list.forEach(item => {
              const statusText = item.isActive ? '<span style="color: green;">(Активне)</span>' : '<span style="color: gray;">(Завершено)</span>';
              let badgesHtml = '';
              for (const [name, cnt] of Object.entries(item.stats)) {
                let badgeClass = 'badge-rank';
                if (name === 'За') badgeClass = 'badge-yes';
                if (name === 'Проти') badgeClass = 'badge-no';
                if (name === 'Утримався') badgeClass = 'badge-abs';
                badgesHtml += `<span class="badge ${badgeClass}">${name}: ${cnt}</span>`;
              }

              historyContainer.innerHTML += `
                <div class="history-card">
                  <div><strong>${item.title}</strong> ${statusText}</div>
                  <div style="font-size: 0.8rem; color: #64748b; margin-top: 2px;">Тип: ${item.pollType === 'ranking' ? 'Рейтингове' : 'Звичайне'} | Створено: ${new Date(item.createdAt).toLocaleString('uk-UA')}</div>
                  <div class="history-badges">
                    ${badgesHtml}
                    <span style="font-size: 0.85rem; margin-left: auto; color: #334155;">Всього: ${item.totalVotes}</span>
                  </div>
                </div>
              `;
            });
          }
        }
      } catch (err) {
        console.error(err);
      }
    }

    async function createPoll() {
      const title = document.getElementById('newQuestion').value.trim();
      const pollType = document.getElementById('newPollType').value;
      if (!title) return alert('Введіть питання!');

      let options = [];
      if (pollType === 'ranking') {
        const text = document.getElementById('rankingOptionsText').value.trim();
        options = text.split('\\n').map(s => s.trim()).filter(s => s.length > 0);
        if (options.length === 0) return alert('Введіть хоча б 1 варіант вибору!');
      }

      const res = await fetch('/api/admin/polls', {
        method: 'POST',
        headers: getAdminHeaders(),
        body: JSON.stringify({ title, pollType, options })
      });
      if (res.ok) {
        document.getElementById('newQuestion').value = '';
        document.getElementById('rankingOptionsText').value = '';
        alert('Голосування розпочато!');
        loadAdminData();
        checkPoll();
      }
    }

    async function closePoll() {
      if (!activePollId) return alert('Немає активного опитування');
      const res = await fetch(`/api/admin/polls/${activePollId}/close`, {
        method: 'POST',
        headers: getAdminHeaders()
      });
      if (res.ok) {
        alert('Голосування закрито!');
        loadAdminData();
        checkPoll();
      }
    }

    function schedulePoll() {
      const delay = 4000 + Math.random() * 3000;
      setTimeout(async () => {
        await checkPoll();
        schedulePoll();
      }, delay);
    }

    if (adminToken) loadAdminData(true);

    setInterval(() => {
      if (adminToken && document.getElementById('adminPanel').style.display === 'block') {
        loadAdminData(false);
      }
    }, 3500);

    checkPoll();
    schedulePoll();
  </script>
</body>
</html>
"""

@app.get("/api/poll/active")
async def get_active_poll():
    global cached_active_poll
    if cached_active_poll:
        return {"active": True, "poll": cached_active_poll}

    if db_pool:
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT id, title, poll_type, options FROM polls WHERE is_active = TRUE ORDER BY created_at DESC LIMIT 1"
            )
            if not row:
                return {"active": False}
            opts = json.loads(row["options"]) if isinstance(row["options"], str) else (row["options"] or [])
            cached_active_poll = {
                "id": row["id"],
                "title": row["title"],
                "pollType": row["poll_type"],
                "options": opts
            }
            return {"active": True, "poll": cached_active_poll}
    else:
        active = [p for p in mem_polls if p.get("isActive")]
        if not active:
            return {"active": False}
        p = active[-1]
        return {
            "active": True,
            "poll": {
                "id": p["id"],
                "title": p["title"],
                "pollType": p.get("pollType", "standard"),
                "options": p.get("options", [])
            }
        }

@app.post("/api/vote")
async def cast_vote(req: VoteRequest):
    now = datetime.now(timezone.utc)
    
    # Визначаємо текст голосу: одиночний вибір або список обраних кандидатів через кому
    if req.selectedOptions:
        choice_text = ", ".join(req.selectedOptions)
    elif req.choice:
        choice_text = req.choice
    else:
        raise HTTPException(status_code=400, detail="Не вибрано жодного варіанту!")

    if db_pool:
        async with db_pool.acquire() as conn:
            poll = await conn.fetchrow("SELECT is_active FROM polls WHERE id = $1", req.pollId)
            if not poll or not poll["is_active"]:
                raise HTTPException(status_code=400, detail="Це голосування закрито")

            vote_id = f"{req.pollId}_{req.voterNumber}"
            try:
                await conn.execute(
                    "INSERT INTO votes (id, poll_id, voter_number, choice, created_at) VALUES ($1, $2, $3, $4, $5)",
                    vote_id, req.pollId, req.voterNumber, choice_text, now
                )
            except asyncpg.UniqueViolationError:
                raise HTTPException(status_code=409, detail=f"Номер {req.voterNumber} вже проголосував!")
    else:
        poll = next((p for p in mem_polls if p["id"] == req.pollId), None)
        if not poll or not poll.get("isActive"):
            raise HTTPException(status_code=400, detail="Це голосування закрито")

        if any(v["pollId"] == req.pollId and v["voterNumber"] == req.voterNumber for v in mem_votes):
            raise HTTPException(status_code=409, detail=f"Номер {req.voterNumber} вже проголосував!")

        mem_votes.append({
            "id": f"{req.pollId}_{req.voterNumber}",
            "pollId": req.pollId,
            "voterNumber": req.voterNumber,
            "choice": choice_text,
            "timestamp": now.isoformat()
        })

    return {"success": True, "message": "Голос зараховано!"}

@app.post("/api/admin/polls")
async def create_poll(req: CreatePollRequest, authorization: str = Header(None)):
    global cached_active_poll
    verify_admin(authorization)
    now = datetime.now(timezone.utc)
    new_id = str(int(time.time()))

    cached_active_poll = {
        "id": new_id,
        "title": req.title,
        "pollType": req.pollType,
        "options": req.options
    }

    if db_pool:
        async with db_pool.acquire() as conn:
            await conn.execute("UPDATE polls SET is_active = FALSE WHERE is_active = TRUE")
            await conn.execute(
                "INSERT INTO polls (id, title, poll_type, options, is_active, created_at) VALUES ($1, $2, $3, $4, TRUE, $5)",
                new_id, req.title, req.pollType, json.dumps(req.options), now
            )
    else:
        for p in mem_polls:
            p["isActive"] = False
        mem_polls.append({
            "id": new_id,
            "title": req.title,
            "pollType": req.pollType,
            "options": req.options,
            "isActive": True,
            "createdAt": now.isoformat()
        })

    return {"success": True, "poll": cached_active_poll}

@app.post("/api/admin/polls/{poll_id}/close")
async def close_poll(poll_id: str, authorization: str = Header(None)):
    global cached_active_poll
    verify_admin(authorization)
    cached_active_poll = None

    if db_pool:
        async with db_pool.acquire() as conn:
            res = await conn.execute("UPDATE polls SET is_active = FALSE WHERE id = $1", poll_id)
            if res == "UPDATE 0":
                raise HTTPException(status_code=404, detail="Опитування не знайдено")
    else:
        poll = next((p for p in mem_polls if p["id"] == poll_id), None)
        if not poll:
            raise HTTPException(status_code=404, detail="Опитування не знайдено")
        poll["isActive"] = False

    return {"success": True, "message": "Голосування закрито"}

@app.get("/api/admin/stats")
async def get_stats(authorization: str = Header(None)):
    verify_admin(authorization)
    if db_pool:
        async with db_pool.acquire() as conn:
            poll_row = await conn.fetchrow(
                "SELECT id, title, poll_type, options, is_active FROM polls ORDER BY created_at DESC LIMIT 1"
            )
            if not poll_row:
                return {"poll": None}
            
            opts = json.loads(poll_row["options"]) if isinstance(poll_row["options"], str) else (poll_row["options"] or [])
            current_poll = {
                "id": poll_row["id"],
                "title": poll_row["title"],
                "pollType": poll_row["poll_type"],
                "options": opts,
                "isActive": poll_row["is_active"]
            }

            vote_rows = await conn.fetch(
                "SELECT voter_number, choice FROM votes WHERE poll_id = $1 ORDER BY voter_number ASC",
                current_poll["id"]
            )
            votes = [{"voterNumber": r["voter_number"], "choice": r["choice"]} for r in vote_rows]
    else:
        if not mem_polls:
            return {"poll": None}
        p = mem_polls[-1]
        current_poll = {
            "id": p["id"],
            "title": p["title"],
            "pollType": p.get("pollType", "standard"),
            "options": p.get("options", []),
            "isActive": p.get("isActive", False)
        }
        votes = [
            {"voterNumber": v["voterNumber"], "choice": v["choice"]}
            for v in mem_votes if v["pollId"] == current_poll["id"]
        ]

    # Підрахунок статистики
    stats = {}
    if current_poll["pollType"] == "standard":
        stats = {"За": 0, "Проти": 0, "Утримався": 0}
        for v in votes:
            c = v.get("choice")
            if c in stats:
                stats[c] += 1
    else:
        # Для рейтингового — ініціалізуємо всі опції нулями
        for opt in current_poll["options"]:
            stats[opt] = 0
        for v in votes:
            selected = [s.strip() for s in v["choice"].split(",") if s.strip()]
            for item in selected:
                stats[item] = stats.get(item, 0) + 1

    return {
        "poll": current_poll,
        "totalVotes": len(votes),
        "stats": stats,
        "votersList": sorted(votes, key=lambda x: x["voterNumber"]),
    }

@app.get("/api/admin/history")
async def get_history(authorization: str = Header(None)):
    verify_admin(authorization)
    history = []

    if db_pool:
        async with db_pool.acquire() as conn:
            polls = await conn.fetch("SELECT id, title, poll_type, options, is_active, created_at FROM polls ORDER BY created_at DESC")
            for p in polls:
                opts = json.loads(p["options"]) if isinstance(p["options"], str) else (p["options"] or [])
                vote_rows = await conn.fetch("SELECT choice FROM votes WHERE poll_id = $1", p["id"])
                
                stats = {}
                if p["poll_type"] == "standard":
                    stats = {"За": 0, "Проти": 0, "Утримався": 0}
                    for r in vote_rows:
                        if r["choice"] in stats:
                            stats[r["choice"]] += 1
                else:
                    for opt in opts:
                        stats[opt] = 0
                    for r in vote_rows:
                        parts = [s.strip() for s in r["choice"].split(",") if s.strip()]
                        for part in parts:
                            stats[part] = stats.get(part, 0) + 1

                history.append({
                    "id": p["id"],
                    "title": p["title"],
                    "pollType": p["poll_type"],
                    "isActive": p["is_active"],
                    "createdAt": p["created_at"].isoformat(),
                    "totalVotes": len(vote_rows),
                    "stats": stats
                })
    else:
        for p in reversed(mem_polls):
            poll_votes = [v for v in mem_votes if v["pollId"] == p["id"]]
            stats = {}
            if p.get("pollType") == "standard":
                stats = {"За": 0, "Проти": 0, "Утримався": 0}
                for v in poll_votes:
                    if v["choice"] in stats:
                        stats[v["choice"]] += 1
            else:
                for opt in p.get("options", []):
                    stats[opt] = 0
                for v in poll_votes:
                    parts = [s.strip() for s in v["choice"].split(",") if s.strip()]
                    for part in parts:
                        stats[part] = stats.get(part, 0) + 1

            history.append({
                "id": p["id"],
                "title": p["title"],
                "pollType": p.get("pollType", "standard"),
                "isActive": p.get("isActive", False),
                "createdAt": p.get("createdAt"),
                "totalVotes": len(poll_votes),
                "stats": stats
            })

    return history

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=True)