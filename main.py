import os
import time
from datetime import datetime, timezone
from typing import Literal
from contextlib import asynccontextmanager

from azure.cosmos.aio import CosmosClient
from azure.cosmos import PartitionKey
from dotenv import load_dotenv
from fastapi import FastAPI, Header, HTTPException, status
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
import uvicorn

load_dotenv()

COSMOS_ENDPOINT = os.getenv("COSMOS_ENDPOINT")
COSMOS_KEY = os.getenv("COSMOS_KEY")
DATABASE_NAME = os.getenv("COSMOS_DB_NAME", "VotingDB")
ADMIN_PASSWORD = os.getenv("ADMIN_PASSWORD", "admin123")

# Резервна база в пам'яті
mem_polls = []
mem_votes = []

cosmos_client = None
polls_container = None
votes_container = None

@asynccontextmanager
async def lifespan(app: FastAPI):
    global cosmos_client, polls_container, votes_container
    if COSMOS_ENDPOINT and COSMOS_KEY and "your-cosmos" not in COSMOS_ENDPOINT:
        try:
            cosmos_client = CosmosClient(COSMOS_ENDPOINT, credential=COSMOS_KEY)
            database = await cosmos_client.create_database_if_not_exists(id=DATABASE_NAME)
            polls_container = await database.create_container_if_not_exists(
                id="polls", partition_key=PartitionKey(path="/id")
            )
            votes_container = await database.create_container_if_not_exists(
                id="votes", partition_key=PartitionKey(path="/pollId")
            )
            print("🚀 Cosmos DB успішно підключено!")
        except Exception as e:
            print("⚠️ Помилка з'єднання з Cosmos DB, увімкнено резервний режим пам'яті:", e)
    else:
        print("ℹ️ Працює локальний режим пам'яті.")
    yield
    if cosmos_client:
        await cosmos_client.close()

app = FastAPI(title="Azure Voting App with History", lifespan=lifespan)

class VoteRequest(BaseModel):
    pollId: str
    voterNumber: int
    choice: Literal["За", "Проти", "Утримався"]

class CreatePollRequest(BaseModel):
    title: str

def verify_admin(authorization: str = Header(None)):
    if authorization != f"Bearer {ADMIN_PASSWORD}":
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Невірний пароль адміністратора",
        )

# Головна сторінка учасника
@app.get("/", response_class=HTMLResponse)
async def serve_index():
    return """<!DOCTYPE html>
<html lang="uk">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Голосування</title>
  <style>
    body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; display: flex; justify-content: center; align-items: center; min-height: 100vh; background: #eef2f7; margin: 0; padding: 20px; }
    .card { background: white; padding: 30px; border-radius: 16px; box-shadow: 0 10px 25px rgba(0,0,0,0.08); width: 100%; max-width: 400px; text-align: center; }
    h2 { margin-top: 0; color: #1e293b; }
    .poll-title { font-size: 1.25rem; font-weight: 700; color: #0284c7; margin: 20px 0; min-height: 40px; }
    input[type="number"] { width: 100%; padding: 14px; font-size: 1.1rem; border: 2px solid #cbd5e1; border-radius: 10px; box-sizing: border-box; text-align: center; margin-bottom: 20px; outline: none; }
    input[type="number"]:focus { border-color: #0284c7; }
    .btn-group { display: flex; flex-direction: column; gap: 12px; }
    button.vote-btn { padding: 14px; font-size: 1.1rem; font-weight: bold; border: none; border-radius: 10px; cursor: pointer; color: white; transition: 0.15s; }
    button.vote-btn:disabled { opacity: 0.4; cursor: not-allowed; }
    .btn-yes { background: #16a34a; }
    .btn-no { background: #dc2626; }
    .btn-abstain { background: #64748b; }
    .msg { margin-top: 20px; font-weight: bold; padding: 12px; border-radius: 8px; }
    .msg.success { background: #dcfce7; color: #15803d; }
    .msg.error { background: #fee2e2; color: #b91c1c; }
  </style>
</head>
<body>
  <div class="card">
    <h2>🗳️ Електронне голосування</h2>
    <div id="noPoll" style="color: #64748b; font-size: 1.05rem;">Зараз немає активного голосування. Очікуйте...</div>

    <div id="pollArea" style="display: none;">
      <div class="poll-title" id="pollQuestion">Завантаження...</div>
      <input type="number" id="voterNumber" placeholder="Введіть ваш порядковий номер" min="1">
      <div class="btn-group">
        <button class="vote-btn btn-yes" onclick="sendVote('За')">👍 ЗА</button>
        <button class="vote-btn btn-no" onclick="sendVote('Проти')">👎 ПРОТИ</button>
        <button class="vote-btn btn-abstain" onclick="sendVote('Утримався')">✋ УТРИМАВСЯ</button>
      </div>
    </div>

    <div id="feedback" class="msg" style="display: none;"></div>
  </div>

  <script>
    let currentPollId = null;

    async function checkPoll() {
      try {
        const res = await fetch('/api/poll/active');
        const data = await res.json();
        const noPoll = document.getElementById('noPoll');
        const pollArea = document.getElementById('pollArea');

        if (data.active) {
          if (currentPollId !== data.poll.id) {
            currentPollId = data.poll.id;
            document.getElementById('pollQuestion').innerText = data.poll.title;
            document.getElementById('voterNumber').disabled = false;
            document.querySelectorAll('.vote-btn').forEach(b => b.disabled = false);
            document.getElementById('feedback').style.display = 'none';
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

    async function sendVote(choice) {
      const voterInput = document.getElementById('voterNumber');
      const voterNumber = voterInput.value.trim();

      if (!voterNumber) {
        showMsg('Введіть свій порядковий номер!', 'error');
        return;
      }

      const buttons = document.querySelectorAll('.vote-btn');
      buttons.forEach(b => b.disabled = true);

      try {
        const res = await fetch('/api/vote', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ pollId: currentPollId, voterNumber: parseInt(voterNumber, 10), choice })
        });
        const data = await res.json();

        if (res.ok) {
          showMsg(`Успіх! Ваш голос "${choice}" зараховано.`, 'success');
          voterInput.disabled = true;
        } else {
          showMsg(data.detail || 'Помилка', 'error');
          buttons.forEach(b => b.disabled = false);
        }
      } catch (err) {
        showMsg("Помилка з'єднання із сервером", 'error');
        buttons.forEach(b => b.disabled = false);
      }
    }

    function showMsg(text, type) {
      const fb = document.getElementById('feedback');
      fb.innerText = text;
      fb.className = `msg ${type}`;
      fb.style.display = 'block';
    }

    function schedulePoll() {
      const delay = 5000 + Math.random() * 4000;
      setTimeout(async () => {
        await checkPoll();
        schedulePoll();
      }, delay);
    }

    checkPoll();
    schedulePoll();
  </script>
</body>
</html>
"""

# Адмін-панель з Архівом
@app.get("/admin", response_class=HTMLResponse)
async def serve_admin():
    return """<!DOCTYPE html>
<html lang="uk">
<head>
  <meta charset="UTF-8">
  <title>Адмін-панель</title>
  <style>
    body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background: #f8fafc; margin: 0; padding: 25px; }
    .container { max-width: 800px; margin: 0 auto; }
    .card { background: white; padding: 20px; border-radius: 12px; box-shadow: 0 4px 15px rgba(0,0,0,0.05); margin-bottom: 20px; }
    input, textarea, button { font-size: 1rem; padding: 10px; border-radius: 8px; border: 1px solid #cbd5e1; box-sizing: border-box; }
    button { cursor: pointer; background: #0284c7; color: white; border: none; font-weight: bold; }
    button.danger { background: #ef4444; }
    .stats-box { display: flex; gap: 15px; margin: 15px 0; }
    .stat-item { flex: 1; padding: 15px; border-radius: 8px; text-align: center; color: white; }
    .stat-yes { background: #16a34a; }
    .stat-no { background: #dc2626; }
    .stat-abstain { background: #64748b; }
    .stat-number { font-size: 2rem; font-weight: bold; }
    table { width: 100%; border-collapse: collapse; margin-top: 15px; }
    th, td { border: 1px solid #e2e8f0; padding: 10px; text-align: left; }
    th { background: #f1f5f9; }
    .history-card { background: #f8fafc; border: 1px solid #e2e8f0; border-radius: 8px; padding: 15px; margin-bottom: 12px; }
    .history-badges { display: flex; gap: 10px; margin-top: 8px; font-weight: bold; }
    .badge { padding: 4px 8px; border-radius: 6px; font-size: 0.9rem; }
    .badge-yes { background: #dcfce7; color: #166534; }
    .badge-no { background: #fee2e2; color: #991b1b; }
    .badge-abs { background: #f1f5f9; color: #475569; }
  </style>
</head>
<body>
  <div class="container">
    <div class="card">
      <h2>⚙️ Вхід для адміністратора</h2>
      <input type="password" id="adminPass" value="admin123" placeholder="Пароль" style="width: 65%;">
      <button onclick="loadAllData()">Увійти / Оновити</button>
    </div>

    <div class="card">
      <h3>Створити нове голосування</h3>
      <textarea id="newQuestion" placeholder="Введіть питання..." rows="2" style="width: 100%; margin-bottom: 10px;"></textarea>
      <button onclick="createPoll()">Запустити голосування</button>
      <button class="danger" onclick="closePoll()">Закрити поточне</button>
    </div>

    <div class="card">
      <h3 id="currentPollTitle">Немає створених опитувань</h3>
      <p id="pollStatus" style="font-weight: bold;"></p>
      
      <div class="stats-box">
        <div class="stat-item stat-yes"><div>ЗА</div><div class="stat-number" id="countYes">0</div></div>
        <div class="stat-item stat-no"><div>ПРОТИ</div><div class="stat-number" id="countNo">0</div></div>
        <div class="stat-item stat-abstain"><div>УТРИМАВСЯ</div><div class="stat-number" id="countAbstain">0</div></div>
      </div>
      <p>Всього голосів: <strong id="totalVotes">0</strong></p>

      <h4>Список проголосувавших у поточному:</h4>
      <table>
        <thead><tr><th>Номер</th><th>Голос</th></tr></thead>
        <tbody id="votersTable"></tbody>
      </table>
    </div>

    <!-- БЛОК АРХІВУ ВСІХ ГОЛОСУВАНЬ -->
    <div class="card">
      <h3>📚 Архів усіх голосувань</h3>
      <div id="historyList" style="color: #64748b;">Завантаження архіву...</div>
    </div>
  </div>

  <script>
    let activePollId = null;

    function getHeaders() {
      return {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer ' + document.getElementById('adminPass').value
      };
    }

    async function loadStats() {
      try {
        const res = await fetch('/api/admin/stats', { headers: getHeaders() });
        if (!res.ok) return;
        const data = await res.json();

        if (!data.poll) return;

        activePollId = data.poll.id;
        document.getElementById('currentPollTitle').innerText = data.poll.title;
        document.getElementById('pollStatus').innerText = data.poll.isActive ? '🟢 Голосування триває' : '🔴 Голосування закрито';
        document.getElementById('countYes').innerText = data.stats['За'] || 0;
        document.getElementById('countNo').innerText = data.stats['Проти'] || 0;
        document.getElementById('countAbstain').innerText = data.stats['Утримався'] || 0;
        document.getElementById('totalVotes').innerText = data.totalVotes;

        const table = document.getElementById('votersTable');
        table.innerHTML = '';
        data.votersList.forEach(v => {
          table.innerHTML += `<tr><td>№ ${v.voterNumber}</td><td><b>${v.choice}</b></td></tr>`;
        });
      } catch (err) {
        console.error(err);
      }
    }

    async function loadHistory() {
      try {
        const res = await fetch('/api/admin/history', { headers: getHeaders() });
        if (!res.ok) return;
        const list = await res.json();
        const historyContainer = document.getElementById('historyList');

        if (!list || list.length === 0) {
          historyContainer.innerText = 'Історія поки що порожня.';
          return;
        }

        historyContainer.innerHTML = '';
        list.forEach(item => {
          const statusText = item.isActive ? '<span style="color: green;">(Активне)</span>' : '<span style="color: gray;">(Завершено)</span>';
          historyContainer.innerHTML += `
            <div class="history-card">
              <div><strong>${item.title}</strong> ${statusText}</div>
              <div style="font-size: 0.85rem; color: #64748b; margin-top: 4px;">Створено: ${new Date(item.createdAt).toLocaleString('uk-UA')}</div>
              <div class="history-badges">
                <span class="badge badge-yes">За: ${item.stats['За']}</span>
                <span class="badge badge-no">Проти: ${item.stats['Проти']}</span>
                <span class="badge badge-abs">Утрималися: ${item.stats['Утримався']}</span>
                <span style="font-size: 0.9rem; margin-left: auto; color: #334155;">Всього: ${item.totalVotes}</span>
              </div>
            </div>
          `;
        });
      } catch (err) {
        console.error(err);
      }
    }

    function loadAllData() {
      loadStats();
      loadHistory();
    }

    async function createPoll() {
      const title = document.getElementById('newQuestion').value.trim();
      if (!title) return alert('Введіть питання!');

      const res = await fetch('/api/admin/polls', {
        method: 'POST',
        headers: getHeaders(),
        body: JSON.stringify({ title })
      });
      if (res.ok) {
        document.getElementById('newQuestion').value = '';
        alert('Голосування розпочато!');
        loadAllData();
      }
    }

    async function closePoll() {
      if (!activePollId) return alert('Немає активного опитування');
      const res = await fetch(`/api/admin/polls/${activePollId}/close`, {
        method: 'POST',
        headers: getHeaders()
      });
      if (res.ok) {
        alert('Голосування закрито!');
        loadAllData();
      }
    }

    setInterval(() => {
      if (document.getElementById('adminPass').value) loadAllData();
    }, 4000);
  </script>
</body>
</html>
"""

# 1. Активне голосування
@app.get("/api/poll/active")
async def get_active_poll():
    if polls_container:
        query = "SELECT * FROM c WHERE c.isActive = true ORDER BY c.createdAt DESC OFFSET 0 LIMIT 1"
        items = [item async for item in polls_container.query_items(query=query)]
        if not items:
            return {"active": False}
        poll = items[0]
        return {"active": True, "poll": {"id": poll["id"], "title": poll["title"]}}
    else:
        active_polls = [p for p in mem_polls if p.get("isActive")]
        if not active_polls:
            return {"active": False}
        poll = active_polls[-1]
        return {"active": True, "poll": {"id": poll["id"], "title": poll["title"]}}

# 2. Голосування
@app.post("/api/vote")
async def cast_vote(req: VoteRequest):
    if polls_container and votes_container:
        try:
            poll = await polls_container.read_item(item=req.pollId, partition_key=req.pollId)
        except Exception:
            raise HTTPException(status_code=404, detail="Голосування не знайдено")

        if not poll.get("isActive"):
            raise HTTPException(status_code=400, detail="Це голосування вже завершено")

        query = "SELECT * FROM c WHERE c.pollId = @pollId AND c.voterNumber = @voterNumber"
        params = [
            {"name": "@pollId", "value": req.pollId},
            {"name": "@voterNumber", "value": req.voterNumber},
        ]
        existing = [item async for item in votes_container.query_items(query=query, parameters=params)]
        if existing:
            raise HTTPException(status_code=409, detail=f"Номер {req.voterNumber} вже проголосував!")

        vote_item = {
            "id": f"{req.pollId}_{req.voterNumber}",
            "pollId": req.pollId,
            "voterNumber": req.voterNumber,
            "choice": req.choice,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        await votes_container.create_item(body=vote_item)
    else:
        poll = next((p for p in mem_polls if p["id"] == req.pollId), None)
        if not poll or not poll.get("isActive"):
            raise HTTPException(status_code=400, detail="Це голосування вже завершено")

        already_voted = any(v["pollId"] == req.pollId and v["voterNumber"] == req.voterNumber for v in mem_votes)
        if already_voted:
            raise HTTPException(status_code=409, detail=f"Номер {req.voterNumber} вже проголосував!")

        mem_votes.append({
            "id": f"{req.pollId}_{req.voterNumber}",
            "pollId": req.pollId,
            "voterNumber": req.voterNumber,
            "choice": req.choice,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })

    return {"success": True, "message": "Голос зараховано!"}

# 3. Створити нове опитування
@app.post("/api/admin/polls")
async def create_poll(req: CreatePollRequest, authorization: str = Header(None)):
    verify_admin(authorization)

    if polls_container:
        active_polls = [item async for item in polls_container.query_items(query="SELECT * FROM c WHERE c.isActive = true")]
        for p in active_polls:
            p["isActive"] = False
            await polls_container.upsert_item(body=p)

        new_poll = {
            "id": str(int(time.time())),
            "title": req.title,
            "isActive": True,
            "createdAt": datetime.now(timezone.utc).isoformat(),
        }
        await polls_container.create_item(body=new_poll)
    else:
        for p in mem_polls:
            p["isActive"] = False
        new_poll = {
            "id": str(int(time.time())),
            "title": req.title,
            "isActive": True,
            "createdAt": datetime.now(timezone.utc).isoformat(),
        }
        mem_polls.append(new_poll)

    return {"success": True, "poll": new_poll}

# 4. Закрити опитування
@app.post("/api/admin/polls/{poll_id}/close")
async def close_poll(poll_id: str, authorization: str = Header(None)):
    verify_admin(authorization)

    if polls_container:
        try:
            poll = await polls_container.read_item(item=poll_id, partition_key=poll_id)
            poll["isActive"] = False
            await polls_container.upsert_item(body=poll)
        except Exception:
            raise HTTPException(status_code=404, detail="Опитування не знайдено")
    else:
        poll = next((p for p in mem_polls if p["id"] == poll_id), None)
        if not poll:
            raise HTTPException(status_code=404, detail="Опитування не знайдено")
        poll["isActive"] = False

    return {"success": True, "message": "Голосування закрито"}

# 5. Статистика активного
@app.get("/api/admin/stats")
async def get_stats(authorization: str = Header(None)):
    verify_admin(authorization)

    if polls_container and votes_container:
        polls = [item async for item in polls_container.query_items(query="SELECT * FROM c ORDER BY c.createdAt DESC OFFSET 0 LIMIT 1")]
        if not polls:
            return {"poll": None}
        current_poll = polls[0]
        votes = [
            item async for item in votes_container.query_items(
                query="SELECT c.voterNumber, c.choice FROM c WHERE c.pollId = @pollId",
                parameters=[{"name": "@pollId", "value": current_poll["id"]}]
            )
        ]
    else:
        if not mem_polls:
            return {"poll": None}
        current_poll = mem_polls[-1]
        votes = [v for v in mem_votes if v["pollId"] == current_poll["id"]]

    stats = {"За": 0, "Проти": 0, "Утримався": 0}
    for v in votes:
        c = v.get("choice")
        if c in stats:
            stats[c] += 1

    return {
        "poll": current_poll,
        "totalVotes": len(votes),
        "stats": stats,
        "votersList": sorted(votes, key=lambda x: x["voterNumber"]),
    }

# 6. АРХІВ УСІХ ОПИТУВАНЬ ТА РЕЗУЛЬТАТІВ
@app.get("/api/admin/history")
async def get_history(authorization: str = Header(None)):
    verify_admin(authorization)

    history = []

    if polls_container and votes_container:
        polls = [item async for item in polls_container.query_items(query="SELECT * FROM c ORDER BY c.createdAt DESC")]
        for p in polls:
            votes = [
                v async for v in votes_container.query_items(
                    query="SELECT c.choice FROM c WHERE c.pollId = @pollId",
                    parameters=[{"name": "@pollId", "value": p["id"]}]
                )
            ]
            stats = {"За": 0, "Проти": 0, "Утримався": 0}
            for v in votes:
                c = v.get("choice")
                if c in stats:
                    stats[c] += 1
            history.append({
                "id": p["id"],
                "title": p["title"],
                "isActive": p.get("isActive", False),
                "createdAt": p.get("createdAt"),
                "totalVotes": len(votes),
                "stats": stats
            })
    else:
        for p in reversed(mem_polls):
            votes = [v for v in mem_votes if v["pollId"] == p["id"]]
            stats = {"За": 0, "Проти": 0, "Утримався": 0}
            for v in votes:
                c = v.get("choice")
                if c in stats:
                    stats[c] += 1
            history.append({
                "id": p["id"],
                "title": p["title"],
                "isActive": p.get("isActive", False),
                "createdAt": p.get("createdAt"),
                "totalVotes": len(votes),
                "stats": stats
            })

    return history

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=True)
