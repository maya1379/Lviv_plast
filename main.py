import asyncio
import json
import logging
import os
import secrets
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import List, Literal, Optional

import asyncpg
from dotenv import load_dotenv
from fastapi import FastAPI, Header, HTTPException, Response, status
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field, field_validator, model_validator
import uvicorn

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("voting")

DATABASE_URL = os.getenv("DATABASE_URL")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD") or "admin128"
ADMIN_PASSWORD_IS_DEFAULT = not os.getenv("ADMIN_PASSWORD")
# disable / prefer / require / verify-ca / verify-full
DB_SSL = os.getenv("DB_SSL", "require")
# Пул створюється в КОЖНОМУ воркері: max_size × кількість воркерів має вміщатися в max_connections сервера
DB_POOL_MAX = int(os.getenv("DB_POOL_MAX", "10"))
# Кеш активного опитування живе окремо в кожному процесі (gunicorn -w N, кілька інстансів Azure),
# тому тримаємо його лише кілька секунд — інакше інші воркери віддають уже закрите опитування.
ACTIVE_POLL_CACHE_TTL = float(os.getenv("ACTIVE_POLL_CACHE_TTL", "2"))

STANDARD_CHOICES = ("За", "Проти", "Утримався")
MAX_VOTER_NUMBER = 100_000

db_pool = None

active_poll_cache = {"poll": None, "expires": 0.0, "generation": 0}
active_poll_lock = asyncio.Lock()

# Резервне збереження в пам'яті (якщо немає DATABASE_URL)
mem_polls = []
mem_votes = []

SCHEMA_SQL = """
    -- Серіалізуємо міграцію між воркерами, що стартують одночасно
    SELECT pg_advisory_xact_lock(724501);

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

    -- Активним може бути лише одне опитування: прибираємо можливі дублікати і фіксуємо це індексом
    UPDATE polls SET is_active = FALSE
    WHERE is_active AND id <> (SELECT id FROM polls WHERE is_active ORDER BY created_at DESC LIMIT 1);
    CREATE UNIQUE INDEX IF NOT EXISTS one_active_poll ON polls ((is_active)) WHERE is_active;
"""


@asynccontextmanager
async def lifespan(app: FastAPI):
    global db_pool
    if ADMIN_PASSWORD_IS_DEFAULT:
        logger.warning("ADMIN_PASSWORD не задано — використовується пароль за замовчуванням. Задайте його в налаштуваннях!")
    if DATABASE_URL:
        # Без переходу в режим пам'яті при помилці: у кількох воркерах кожен мав би власні дані
        db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=DB_POOL_MAX, ssl=DB_SSL)
        async with db_pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(SCHEMA_SQL)
        logger.info("PostgreSQL успішно підключено")
    else:
        logger.warning("DATABASE_URL не задано — працюємо в локальному режимі пам'яті")
    yield
    if db_pool:
        await db_pool.close()

app = FastAPI(title="Voting App", lifespan=lifespan)

class VoteRequest(BaseModel):
    pollId: str = Field(min_length=1, max_length=64)
    voterNumber: int = Field(ge=1, le=MAX_VOTER_NUMBER)
    choice: Optional[str] = None
    selectedOptions: Optional[List[str]] = None

class CreatePollRequest(BaseModel):
    title: str = Field(max_length=500)
    pollType: Literal["standard", "ranking"] = "standard"
    options: Optional[List[str]] = Field(default=None, max_length=100)

    @field_validator("title")
    @classmethod
    def title_not_blank(cls, v):
        v = v.strip()
        if not v:
            raise ValueError("Введіть питання")
        return v

    @field_validator("options")
    @classmethod
    def clean_options(cls, v):
        cleaned = [o.strip() for o in (v or []) if o and o.strip()]
        # Голоси рейтингового опитування зберігаються рядком через кому
        if any("," in o for o in cleaned):
            raise ValueError("Варіант не може містити кому")
        if any(len(o) > 200 for o in cleaned):
            raise ValueError("Варіант задовгий (максимум 200 символів)")
        if len(set(cleaned)) != len(cleaned):
            raise ValueError("Варіанти не повинні повторюватися")
        return cleaned

    @model_validator(mode="after")
    def options_match_type(self):
        if self.pollType == "ranking" and not self.options:
            raise ValueError("Рейтингове голосування потребує хоча б 1 варіанту")
        if self.pollType == "standard":
            self.options = []
        return self

def verify_admin(authorization: str = Header(None)):
    expected = f"Bearer {ADMIN_PASSWORD}".encode()
    if not authorization or not secrets.compare_digest(authorization.encode(), expected):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Невірний пароль адміністратора",
        )

def parse_options(raw):
    if isinstance(raw, str):
        raw = json.loads(raw)
    return raw or []

def poll_from_row(row):
    return {
        "id": row["id"],
        "title": row["title"],
        "pollType": row["poll_type"],
        "options": parse_options(row["options"]),
        "createdAt": row["created_at"].isoformat(),
    }

def public_mem_poll(p):
    return {
        "id": p["id"],
        "title": p["title"],
        "pollType": p.get("pollType", "standard"),
        "options": p.get("options", []),
        "createdAt": p.get("createdAt"),
    }

async def fetch_active_poll(conn):
    row = await conn.fetchrow(
        "SELECT id, title, poll_type, options, created_at FROM polls WHERE is_active ORDER BY created_at DESC LIMIT 1"
    )
    return poll_from_row(row) if row else None

def invalidate_active_poll_cache():
    active_poll_cache["expires"] = 0.0
    active_poll_cache["generation"] += 1

async def get_cached_active_poll():
    if time.monotonic() < active_poll_cache["expires"]:
        return active_poll_cache["poll"]
    # Лише один запит на воркер іде в БД після закінчення TTL, решта чекають його результат
    async with active_poll_lock:
        if time.monotonic() < active_poll_cache["expires"]:
            return active_poll_cache["poll"]
        generation = active_poll_cache["generation"]
        async with db_pool.acquire() as conn:
            poll = await fetch_active_poll(conn)
        # Якщо поки ми читали, опитування змінилося — не кешуємо можливо застарілий результат
        if generation == active_poll_cache["generation"]:
            active_poll_cache["poll"] = poll
            active_poll_cache["expires"] = time.monotonic() + ACTIVE_POLL_CACHE_TTL
        return poll

def build_choice_text(poll_type, options, req: VoteRequest):
    if poll_type == "standard":
        if not req.choice:
            raise HTTPException(status_code=400, detail="Не вибрано жодного варіанту!")
        if req.choice not in STANDARD_CHOICES:
            raise HTTPException(status_code=400, detail="Недопустимий варіант вибору")
        return req.choice

    selected = req.selectedOptions or []
    if not selected:
        raise HTTPException(status_code=400, detail="Не вибрано жодного варіанту!")
    if len(set(selected)) != len(selected) or any(s not in options for s in selected):
        raise HTTPException(status_code=400, detail="Недопустимий варіант вибору")
    # Список обраних кандидатів через кому, у порядку опитування
    return ", ".join(o for o in options if o in selected)

def compute_stats(poll_type, options, choices):
    if poll_type == "standard":
        stats = {c: 0 for c in STANDARD_CHOICES}
        for c in choices:
            if c in stats:
                stats[c] += 1
        return stats

    # Для рейтингового — ініціалізуємо всі опції нулями
    stats = {opt: 0 for opt in options}
    for c in choices:
        for item in (s.strip() for s in c.split(",")):
            if item:
                stats[item] = stats.get(item, 0) + 1
    return stats

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
    // Пароль живе лише до закриття вкладки; прибираємо копію, яку зберігали старі версії
    try { localStorage.removeItem('voting_admin_pass'); } catch (e) {}
    let adminToken = sessionStorage.getItem('voting_admin_pass') || '';

    // Опитування, які цей браузер бачив закритими/заміненими. Інший воркер сервера ще кілька секунд
    // може віддавати застарілу відповідь — не показуємо таке опитування повторно.
    const closedPollIds = new Set();
    let pollRequestSeq = 0;
    let appliedPollSeq = 0;

    function escapeHtml(value) {
      return String(value ?? '').replace(/[&<>"']/g, ch => ({
        '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
      })[ch]);
    }

    function errorText(data, fallback) {
      const detail = data && data.detail;
      if (Array.isArray(detail)) return detail.map(d => String(d.msg || '').replace('Value error, ', '')).join('; ');
      return detail || fallback;
    }

    function getVotedPolls() {
      try { return JSON.parse(localStorage.getItem('voting_voted_polls') || '[]'); } catch (e) { return []; }
    }

    function markVoted(pollId) {
      try {
        const voted = getVotedPolls();
        if (!voted.includes(pollId)) {
          voted.push(pollId);
          localStorage.setItem('voting_voted_polls', JSON.stringify(voted.slice(-50)));
        }
      } catch (e) {}
    }

    function toggleOptionsInput() {
      const type = document.getElementById('newPollType').value;
      document.getElementById('rankingInputArea').style.display = type === 'ranking' ? 'block' : 'none';
    }

    function setVotingDisabled(disabled) {
      document.getElementById('voterNumber').disabled = disabled;
      document.querySelectorAll('.vote-btn').forEach(b => b.disabled = disabled);
      document.querySelectorAll('input[name="rankChoice"]').forEach(c => c.disabled = disabled);
    }

    function renderPoll(poll) {
      currentPollId = poll.id;
      currentPollType = poll.pollType || 'standard';

      document.getElementById('pollQuestion').innerText = poll.title;
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
        (poll.options || []).forEach(opt => {
          const label = document.createElement('label');
          label.className = 'ranking-item';
          const checkbox = document.createElement('input');
          checkbox.type = 'checkbox';
          checkbox.name = 'rankChoice';
          checkbox.value = opt;
          const text = document.createElement('span');
          text.textContent = opt;
          label.append(checkbox, text);
          list.appendChild(label);
        });
      } else {
        badge.innerText = 'Звичайне голосування';
        stdButtons.style.display = 'flex';
        rankArea.style.display = 'none';
      }

      const alreadyVoted = getVotedPolls().includes(poll.id);
      setVotingDisabled(alreadyVoted);
      if (alreadyVoted) showMsg('Ваш вибір уже зараховано!', 'success');
    }

    // Перевірка активного опитування для учасників
    async function checkPoll() {
      const seq = ++pollRequestSeq;
      try {
        const res = await fetch('/api/poll/active', { cache: 'no-store' });
        if (!res.ok) return;
        const data = await res.json();
        // Відповідь на старіший запит прийшла пізніше за новіший — ігноруємо
        if (seq < appliedPollSeq) return;
        appliedPollSeq = seq;

        const noPoll = document.getElementById('noPoll');
        const pollArea = document.getElementById('pollArea');
        const poll = data.active && !closedPollIds.has(data.poll.id) ? data.poll : null;

        if (poll) {
          if (currentPollId !== poll.id) {
            if (currentPollId) closedPollIds.add(currentPollId);
            renderPoll(poll);
          }
          noPoll.style.display = 'none';
          pollArea.style.display = 'block';
        } else {
          if (currentPollId) closedPollIds.add(currentPollId);
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
          markVoted(payload.pollId);
          showMsg('Ваш вибір успішно зараховано!', 'success');
          setVotingDisabled(true);
        } else {
          showMsg(errorText(data, 'Помилка при збереженні голосу'), 'error');
          if (onError) onError();
          // Опитування могло закритися — оновлюємо стан одразу, не чекаючи таймера
          if (res.status === 400) checkPoll();
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
      sessionStorage.removeItem('voting_admin_pass');
      document.getElementById('adminPanel').style.display = 'none';
    }

    function getAdminHeaders() {
      return { 'Content-Type': 'application/json', 'Authorization': 'Bearer ' + adminToken };
    }

    let adminLoading = false;

    async function loadAdminData(initial = false) {
      if (!adminToken || adminLoading) return;
      adminLoading = true;

      try {
        const res = await fetch('/api/admin/stats', { headers: getAdminHeaders(), cache: 'no-store' });
        if (res.status === 401) {
          if (initial) alert('Невірний пароль!');
          adminToken = '';
          sessionStorage.removeItem('voting_admin_pass');
          document.getElementById('adminPanel').style.display = 'none';
          return;
        }
        if (!res.ok) return;

        if (initial) {
          sessionStorage.setItem('voting_admin_pass', adminToken);
          document.getElementById('adminPanel').style.display = 'block';
        }

        const data = await res.json();
        if (data.poll) {
          activePollId = data.poll.id;
          document.getElementById('currentPollTitle').innerText = data.poll.title;
          document.getElementById('pollStatus').innerText = data.poll.isActive ? '🟢 Голосування триває' : '🔴 Голосування закрито';
          document.getElementById('totalVotes').innerText = data.totalVotes;

          let statsHtml = '';
          for (const [key, count] of Object.entries(data.stats)) {
            let colorClass = 'stat-rank';
            if (key === 'За') colorClass = 'stat-yes';
            if (key === 'Проти') colorClass = 'stat-no';
            if (key === 'Утримався') colorClass = 'stat-abstain';

            statsHtml += `
              <div class="stat-item ${colorClass}">
                <div style="font-size: 0.85rem;">${escapeHtml(key)}</div>
                <div class="stat-number">${escapeHtml(count)}</div>
              </div>
            `;
          }
          document.getElementById('adminStatsBox').innerHTML = statsHtml;

          document.getElementById('votersTable').innerHTML = data.votersList
            .map(v => `<tr><td>№ ${escapeHtml(v.voterNumber)}</td><td><b>${escapeHtml(v.choice)}</b></td></tr>`)
            .join('');
        }

        const resHist = await fetch('/api/admin/history', { headers: getAdminHeaders(), cache: 'no-store' });
        if (resHist.ok) {
          const list = await resHist.json();
          const historyContainer = document.getElementById('historyList');
          if (!list || list.length === 0) {
            historyContainer.innerText = 'Історія опитувань порожня.';
          } else {
            historyContainer.innerHTML = list.map(item => {
              const statusText = item.isActive ? '<span style="color: green;">(Активне)</span>' : '<span style="color: gray;">(Завершено)</span>';
              let badgesHtml = '';
              for (const [name, cnt] of Object.entries(item.stats)) {
                let badgeClass = 'badge-rank';
                if (name === 'За') badgeClass = 'badge-yes';
                if (name === 'Проти') badgeClass = 'badge-no';
                if (name === 'Утримався') badgeClass = 'badge-abs';
                badgesHtml += `<span class="badge ${badgeClass}">${escapeHtml(name)}: ${escapeHtml(cnt)}</span>`;
              }

              return `
                <div class="history-card">
                  <div><strong>${escapeHtml(item.title)}</strong> ${statusText}</div>
                  <div style="font-size: 0.8rem; color: #64748b; margin-top: 2px;">Тип: ${item.pollType === 'ranking' ? 'Рейтингове' : 'Звичайне'} | Створено: ${escapeHtml(new Date(item.createdAt).toLocaleString('uk-UA'))}</div>
                  <div class="history-badges">
                    ${badgesHtml}
                    <span style="font-size: 0.85rem; margin-left: auto; color: #334155;">Всього: ${escapeHtml(item.totalVotes)}</span>
                  </div>
                </div>
              `;
            }).join('');
          }
        }
      } catch (err) {
        console.error(err);
      } finally {
        adminLoading = false;
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

      try {
        const res = await fetch('/api/admin/polls', {
          method: 'POST',
          headers: getAdminHeaders(),
          body: JSON.stringify({ title, pollType, options })
        });
        const data = await res.json().catch(() => ({}));
        if (res.ok) {
          document.getElementById('newQuestion').value = '';
          document.getElementById('rankingOptionsText').value = '';
          alert('Голосування розпочато!');
          loadAdminData();
          checkPoll();
        } else {
          alert('Помилка: ' + errorText(data, 'HTTP ' + res.status));
        }
      } catch (err) {
        alert("Помилка з'єднання із сервером");
      }
    }

    async function closePoll() {
      if (!activePollId) return alert('Немає активного опитування');
      try {
        const res = await fetch(`/api/admin/polls/${encodeURIComponent(activePollId)}/close`, {
          method: 'POST',
          headers: getAdminHeaders()
        });
        const data = await res.json().catch(() => ({}));
        if (res.ok) {
          alert('Голосування закрито!');
          loadAdminData();
          checkPoll();
        } else {
          alert('Помилка: ' + errorText(data, 'HTTP ' + res.status));
        }
      } catch (err) {
        alert("Помилка з'єднання із сервером");
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
async def get_active_poll(response: Response):
    response.headers["Cache-Control"] = "no-store"
    if db_pool:
        poll = await get_cached_active_poll()
    else:
        poll = next((public_mem_poll(p) for p in reversed(mem_polls) if p.get("isActive")), None)

    if not poll:
        return {"active": False}
    return {"active": True, "poll": poll}

@app.post("/api/vote")
async def cast_vote(req: VoteRequest):
    now = datetime.now(timezone.utc)
    vote_id = f"{req.pollId}_{req.voterNumber}"

    if db_pool:
        async with db_pool.acquire() as conn:
            poll = await conn.fetchrow("SELECT poll_type, options, is_active FROM polls WHERE id = $1", req.pollId)
            if not poll or not poll["is_active"]:
                raise HTTPException(status_code=400, detail="Це голосування закрито")
            choice_text = build_choice_text(poll["poll_type"], parse_options(poll["options"]), req)

            try:
                # Повторна перевірка активності в тому ж запиті — щоб голос не потрапив у щойно закрите опитування
                inserted = await conn.fetchval(
                    """
                    INSERT INTO votes (id, poll_id, voter_number, choice, created_at)
                    SELECT $1, $2, $3, $4, $5
                    WHERE EXISTS (SELECT 1 FROM polls WHERE id = $2 AND is_active)
                    RETURNING 1
                    """,
                    vote_id, req.pollId, req.voterNumber, choice_text, now
                )
            except asyncpg.UniqueViolationError:
                raise HTTPException(status_code=409, detail=f"Номер {req.voterNumber} вже проголосував!")
            if not inserted:
                raise HTTPException(status_code=400, detail="Це голосування закрито")
    else:
        poll = next((p for p in mem_polls if p["id"] == req.pollId), None)
        if not poll or not poll.get("isActive"):
            raise HTTPException(status_code=400, detail="Це голосування закрито")
        choice_text = build_choice_text(poll.get("pollType", "standard"), poll.get("options", []), req)

        if any(v["pollId"] == req.pollId and v["voterNumber"] == req.voterNumber for v in mem_votes):
            raise HTTPException(status_code=409, detail=f"Номер {req.voterNumber} вже проголосував!")

        mem_votes.append({
            "id": vote_id,
            "pollId": req.pollId,
            "voterNumber": req.voterNumber,
            "choice": choice_text,
            "timestamp": now.isoformat()
        })

    return {"success": True, "message": "Голос зараховано!"}

@app.post("/api/admin/polls")
async def create_poll(req: CreatePollRequest, authorization: str = Header(None)):
    verify_admin(authorization)
    now = datetime.now(timezone.utc)
    poll = {
        "id": uuid.uuid4().hex,
        "title": req.title,
        "pollType": req.pollType,
        "options": req.options,
        "createdAt": now.isoformat(),
    }

    if db_pool:
        async with db_pool.acquire() as conn:
            async with conn.transaction():
                # Серіалізуємо одночасні створення, щоб UPDATE бачив опитування, щойно створене іншим запитом
                await conn.execute("SELECT pg_advisory_xact_lock(724502)")
                await conn.execute("UPDATE polls SET is_active = FALSE WHERE is_active")
                await conn.execute(
                    "INSERT INTO polls (id, title, poll_type, options, is_active, created_at) VALUES ($1, $2, $3, $4, TRUE, $5)",
                    poll["id"], req.title, req.pollType, json.dumps(req.options), now
                )
        invalidate_active_poll_cache()
    else:
        for p in mem_polls:
            p["isActive"] = False
        mem_polls.append({**poll, "isActive": True})

    logger.info("Запущено голосування %s: %s", poll["id"], req.title)
    return {"success": True, "poll": poll}

@app.post("/api/admin/polls/{poll_id}/close")
async def close_poll(poll_id: str, authorization: str = Header(None)):
    verify_admin(authorization)

    if db_pool:
        async with db_pool.acquire() as conn:
            res = await conn.execute("UPDATE polls SET is_active = FALSE WHERE id = $1", poll_id)
        if res == "UPDATE 0":
            raise HTTPException(status_code=404, detail="Опитування не знайдено")
        invalidate_active_poll_cache()
    else:
        poll = next((p for p in mem_polls if p["id"] == poll_id), None)
        if not poll:
            raise HTTPException(status_code=404, detail="Опитування не знайдено")
        poll["isActive"] = False

    logger.info("Закрито голосування %s", poll_id)
    return {"success": True, "message": "Голосування закрито"}

@app.get("/api/admin/stats")
async def get_stats(authorization: str = Header(None)):
    verify_admin(authorization)
    if db_pool:
        async with db_pool.acquire() as conn:
            poll_row = await conn.fetchrow(
                "SELECT id, title, poll_type, options, is_active, created_at FROM polls ORDER BY created_at DESC LIMIT 1"
            )
            if not poll_row:
                return {"poll": None}
            current_poll = {**poll_from_row(poll_row), "isActive": poll_row["is_active"]}

            vote_rows = await conn.fetch(
                "SELECT voter_number, choice FROM votes WHERE poll_id = $1 ORDER BY voter_number ASC",
                current_poll["id"]
            )
            votes = [{"voterNumber": r["voter_number"], "choice": r["choice"]} for r in vote_rows]
    else:
        if not mem_polls:
            return {"poll": None}
        p = mem_polls[-1]
        current_poll = {**public_mem_poll(p), "isActive": p.get("isActive", False)}
        votes = [
            {"voterNumber": v["voterNumber"], "choice": v["choice"]}
            for v in mem_votes if v["pollId"] == current_poll["id"]
        ]

    return {
        "poll": current_poll,
        "totalVotes": len(votes),
        "stats": compute_stats(current_poll["pollType"], current_poll["options"], [v["choice"] for v in votes]),
        "votersList": sorted(votes, key=lambda x: x["voterNumber"]),
    }

@app.get("/api/admin/history")
async def get_history(authorization: str = Header(None)):
    verify_admin(authorization)
    history = []

    if db_pool:
        async with db_pool.acquire() as conn:
            polls = await conn.fetch("""
                SELECT p.id, p.title, p.poll_type, p.options, p.is_active, p.created_at,
                       COALESCE(array_agg(v.choice) FILTER (WHERE v.choice IS NOT NULL), '{}') AS choices
                FROM polls p
                LEFT JOIN votes v ON v.poll_id = p.id
                GROUP BY p.id
                ORDER BY p.created_at DESC
            """)
        for p in polls:
            history.append({
                "id": p["id"],
                "title": p["title"],
                "pollType": p["poll_type"],
                "isActive": p["is_active"],
                "createdAt": p["created_at"].isoformat(),
                "totalVotes": len(p["choices"]),
                "stats": compute_stats(p["poll_type"], parse_options(p["options"]), p["choices"])
            })
    else:
        for p in reversed(mem_polls):
            choices = [v["choice"] for v in mem_votes if v["pollId"] == p["id"]]
            history.append({
                "id": p["id"],
                "title": p["title"],
                "pollType": p.get("pollType", "standard"),
                "isActive": p.get("isActive", False),
                "createdAt": p.get("createdAt"),
                "totalVotes": len(choices),
                "stats": compute_stats(p.get("pollType", "standard"), p.get("options", []), choices)
            })

    return history

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
