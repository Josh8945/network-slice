"""Distributed Network Slicing Platform - one service, three roles.

The same file runs as the controller (port 8000) or as a worker (8001, 8002).
All addresses come from environment variables, so it runs unchanged on
localhost, in Docker Compose, or across separate machines.

  NODE_NAME    logical name of this node         (default from port)
  NODE_ID      numeric Bully-election priority    (default from port)
  WORKER_NODES comma-separated worker URLs        (controller only)
  PEERS        comma-separated name=id=url of every OTHER node (for election)
  HOST         bind address                       (default 0.0.0.0)
"""
import sys, os, time, socket, threading, asyncio
from contextlib import asynccontextmanager
from multiprocessing import Process, Queue
import psutil, httpx, uvicorn
from fastapi import FastAPI
from pydantic import BaseModel

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else int(os.getenv("PORT", 8001))
_DEFAULTS = {8000: ("controller", 1), 8001: ("node1", 2), 8002: ("node2", 3)}
NODE = os.getenv("NODE_NAME", _DEFAULTS.get(PORT, (f"node-{PORT}", PORT))[0])
NODE_ID = int(os.getenv("NODE_ID", _DEFAULTS.get(PORT, (NODE, PORT))[1]))
HOST = os.getenv("HOST", "0.0.0.0")

workers = [w.strip() for w in os.getenv(
    "WORKER_NODES", "http://127.0.0.1:8001,http://127.0.0.1:8002").split(",") if w.strip()]

def _parse_peers():
    raw = os.getenv("PEERS")
    if raw is None:  # localhost default: every other default node
        return [{"name": n, "id": i, "url": f"http://127.0.0.1:{p}"}
                for p, (n, i) in _DEFAULTS.items() if n != NODE]
    out = []
    for entry in filter(None, (e.strip() for e in raw.split(","))):
        name, nid, url = entry.split("=", 2)
        out.append({"name": name, "id": int(nid), "url": url})
    return out

peers = _parse_peers()

client: httpx.AsyncClient  # one pooled async client, created at startup

@asynccontextmanager
async def lifespan(_app):
    global client
    client = httpx.AsyncClient(timeout=3.0)
    yield
    await client.aclose()

app = FastAPI(title=f"Network Slice {NODE}", lifespan=lifespan)
logical_clock = 0; lock = threading.Lock()
allocated = {}
rr = 0; rrlock = threading.Lock()

# Bully election state
leader = None             # {"name":..., "id":...} of the current coordinator
election_messages = 0     # messages this node has SENT in elections
election_lock = asyncio.Lock()

class SliceRequest(BaseModel):
    slice_id: str; cpu_percent: int; memory_mb: int; bandwidth_mbps: int; priority: int = 1
class TaskRequest(BaseModel):
    task_id: str; work_units: int = 100000
class ElectionMsg(BaseModel):
    sender: str; sender_id: int
class CoordinatorMsg(BaseModel):
    leader: str; leader_id: int

def tick():
    global logical_clock
    with lock:
        logical_clock += 1
        return logical_clock

def cpu_work(n):
    total = 0
    for i in range(n): total += (i * i) % 9973
    return total

def child(n, q): q.put(cpu_work(n))

async def _get(url, timeout=2.0):
    """GET returning JSON, or None if the node is unreachable."""
    try:
        r = await client.get(url, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except (httpx.HTTPError, ValueError):
        return None

# ---------------------------------------------------------------- node API

@app.get("/health")
def health(): return {"node": NODE, "node_id": NODE_ID, "status": "online"}

@app.get("/status")
def status():
    vm = psutil.virtual_memory()
    return {"node": NODE, "node_id": NODE_ID, "host": socket.gethostname(),
            "cpu_percent": psutil.cpu_percent(.05), "memory_percent": vm.percent,
            "available_memory_mb": round(vm.available / 1024 / 1024, 2),
            "allocated_slices": len(allocated), "logical_clock": logical_clock,
            "leader": leader}

@app.post("/allocate")
def allocate(req: SliceRequest):
    tick(); allocated[req.slice_id] = req.model_dump()
    return {"status": "allocated", "node": NODE, "slice": req.model_dump(), "logical_clock": logical_clock}

@app.post("/task")
def task(req: TaskRequest):
    # Sync endpoint: FastAPI runs it in a thread pool, so the child process
    # never blocks the event loop.
    tick(); q = Queue(); t = time.perf_counter()
    p = Process(target=child, args=(req.work_units, q)); p.start()
    result = q.get(timeout=20); p.join(timeout=2)
    return {"task_id": req.task_id, "node": NODE, "result": result,
            "execution_time_seconds": time.perf_counter() - t, "logical_clock": logical_clock}

@app.post("/clock/event")
def clock_event(payload: dict):
    global logical_clock
    with lock: logical_clock = max(logical_clock, int(payload.get("clock", 0))) + 1
    return {"node": NODE, "logical_clock": logical_clock, "event": payload.get("event")}

# ------------------------------------------------------ Bully leader election
#
# 1. A node P starts an election by sending ELECTION to every peer with a
#    higher ID than its own.
# 2. Any higher node that is alive answers OK and starts its own election.
# 3. If nobody higher answers, P wins and broadcasts COORDINATOR to all peers.
# 4. If someone answered, P waits for the COORDINATOR message.
# Message complexity: O(n^2) worst case (lowest ID starts), O(n) best case.

async def run_election():
    global leader, election_messages
    async with election_lock:
        higher = [p for p in peers if p["id"] > NODE_ID]
        msg = {"sender": NODE, "sender_id": NODE_ID}

        async def send(p):
            try:
                r = await client.post(p["url"] + "/election/message", json=msg, timeout=1.0)
                return r.status_code == 200
            except httpx.HTTPError:
                return False

        election_messages += len(higher)
        answers = await asyncio.gather(*(send(p) for p in higher))
        if any(answers):
            return False  # a higher node took over
        leader = {"name": NODE, "id": NODE_ID}
        election_messages += len(peers)
        await asyncio.gather(*(client.post(p["url"] + "/election/coordinator",
                                           json={"leader": NODE, "leader_id": NODE_ID},
                                           timeout=1.0) for p in peers),
                             return_exceptions=True)
        return True

@app.post("/election/message")
async def election_message(msg: ElectionMsg):
    # Reply OK immediately, then bully the lower node by running our own election.
    if not election_lock.locked():  # already campaigning: don't start a second one
        asyncio.create_task(run_election())
    return {"reply": "OK", "from": NODE, "id": NODE_ID}

@app.post("/election/coordinator")
async def election_coordinator(msg: CoordinatorMsg):
    global leader
    leader = {"name": msg.leader, "id": msg.leader_id}
    return {"ack": NODE, "leader": leader}

@app.post("/election/start")
async def election_start():
    global leader
    leader = None
    t = time.perf_counter()
    won = await run_election()
    while leader is None and time.perf_counter() - t < 5:  # wait for COORDINATOR
        await asyncio.sleep(0.05)
    return {"initiator": NODE, "won": won, "leader": leader,
            "election_time_seconds": round(time.perf_counter() - t, 4)}

@app.get("/election/state")
def election_state():
    return {"node": NODE, "node_id": NODE_ID, "leader": leader, "messages_sent": election_messages}

# ------------------------------------------------------- controller API

@app.get("/architecture")
def architecture():
    return {"client": "VS Code/API client",
            "controller": {"port": 8000, "role": "Core / Controller"},
            "workers": [{"url": w, "role": "Edge / Worker"} for w in workers],
            "communication": "REST/HTTP (async httpx)"}

@app.get("/deployment")
def deployment():
    return {"layers": [{"layer": "Client", "component": "API client"},
                       {"layer": "Core", "component": "Controller / Scheduler / Resource Manager"}]
                      + [{"layer": "Edge", "component": f"Worker {w}"} for w in workers]}

@app.get("/workers")
async def workers_status():
    results = await asyncio.gather(*(_get(w + "/status") for w in workers))
    return [r if r is not None else {"worker": w, "status": "offline"}
            for w, r in zip(workers, results)]

@app.post("/slices")
async def create_slice(req: SliceRequest):
    global rr
    # Query every worker in parallel instead of one after another.
    statuses = await asyncio.gather(*(_get(w + "/status") for w in workers))
    candidates = [w for w, s in zip(workers, statuses)
                  if s and s["cpu_percent"] < 90 and s["memory_percent"] < 90]
    if not candidates: return {"status": "rejected", "reason": "No available worker"}
    with rrlock: target = candidates[rr % len(candidates)]; rr += 1
    try:
        r = await client.post(target + "/allocate", json=req.model_dump(), timeout=3)
        r.raise_for_status()
    except httpx.HTTPError as e:
        return {"status": "failed", "selected_worker": target, "error": str(e)}
    return {"status": "created", "selected_worker": target, "allocation": r.json(),
            "scheduler": "resource-aware round-robin"}

@app.post("/tasks")
async def dispatch(req: TaskRequest):
    global rr
    # Try each worker at most once, starting from the round-robin position,
    # so a dead worker causes failover instead of a failed task.
    with rrlock: start = rr; rr += 1
    errors = []
    for i in range(len(workers)):
        target = workers[(start + i) % len(workers)]
        try:
            r = await client.post(target + "/task", json=req.model_dump(), timeout=20)
            r.raise_for_status()
            return {**r.json(), "failover_attempts": i}
        except httpx.HTTPError as e:
            errors.append(f"{target}: {e!r}")
    return {"status": "failed", "errors": errors}

@app.post("/coordination/election")
async def election():
    """Trigger a Bully election from the lowest-ID reachable node (the
    worst case, so the measured message count is an upper bound)."""
    everyone = sorted(peers + [{"name": NODE, "id": NODE_ID, "url": None}], key=lambda p: p["id"])
    before = await _collect_election_state(everyone)
    t = time.perf_counter()
    result = None
    for p in everyone:
        if p["url"] is None:
            result = await election_start(); break
        try:
            r = await client.post(p["url"] + "/election/start", timeout=8)
            r.raise_for_status(); result = r.json(); break
        except httpx.HTTPError:
            continue  # lowest node is down, try the next one
    await asyncio.sleep(0.3)  # let the last COORDINATOR broadcasts land
    after = await _collect_election_state(everyone)
    participants = [s["node"] for s in after if s]
    views = {s["node"]: (s["leader"] or {}).get("name") for s in after if s}
    msgs = sum(a["messages_sent"] - b["messages_sent"]
               for a, b in zip(after, before) if a and b)
    return {"algorithm": "Bully",
            "initiator": (result or {}).get("initiator"),
            "participants": participants,
            "leader": ((result or {}).get("leader") or {}).get("name"),
            "leader_as_seen_by_each_node": views,
            "consistent": len(set(views.values())) == 1,
            "coordination_message_count": msgs,
            "election_time_seconds": round(time.perf_counter() - t, 4)}

async def _collect_election_state(nodes):
    async def one(p):
        if p["url"] is None: return election_state()
        return await _get(p["url"] + "/election/state", timeout=1.0)
    return await asyncio.gather(*(one(p) for p in nodes))

@app.post("/coordination/event")
async def coordination_event(payload: dict):
    start = time.perf_counter(); c = tick()
    resps = await asyncio.gather(
        *(client.post(w + "/clock/event", json={"event": payload.get("event"), "clock": c}, timeout=2)
          for w in workers), return_exceptions=True)
    acks = [r.json() for r in resps if isinstance(r, httpx.Response) and r.status_code == 200]
    return {"event": payload.get("event"), "logical_clock": c,
            "acknowledgements": acks,
            "synchronization_delay_seconds": time.perf_counter() - start}

if __name__ == "__main__": uvicorn.run(app, host=HOST, port=PORT, log_level="warning")
