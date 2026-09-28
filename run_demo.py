"""Milestones 1-4 demonstration.

  python run_demo.py            start the 3 nodes locally, run the demo, stop them
  python run_demo.py --docker   use the containers from `docker compose up -d`,
                                and also run the failure demo (stop node2,
                                re-elect, fail over, restart, measure recovery)
"""
import json, os, subprocess, sys, time, statistics
from pathlib import Path
import requests

ROOT = Path(__file__).resolve().parent
RESULTS = ROOT / "results"; RESULTS.mkdir(exist_ok=True)
DOCKER = "--docker" in sys.argv
CONTROLLER = os.getenv("CONTROLLER_URL", "http://127.0.0.1:8000")
WORKERS = ["http://127.0.0.1:8001", "http://127.0.0.1:8002"]  # as seen from this machine
procs = []

def wait(url, timeout=30):
    end = time.time() + timeout
    while time.time() < end:
        try:
            if requests.get(url, timeout=1).ok: return True
        except requests.RequestException: pass
        time.sleep(.2)
    return False

def start():
    for port in (8001, 8002, 8000):
        procs.append(subprocess.Popen([sys.executable, "service.py", str(port)],
                                      cwd=ROOT, stdout=subprocess.DEVNULL,
                                      stderr=subprocess.DEVNULL))
    if not wait(CONTROLLER + "/health"): raise RuntimeError("Controller failed")
    for w in WORKERS:
        if not wait(w + "/health"): raise RuntimeError("Worker failed: " + w)

def stop():
    for p in procs:
        if p.poll() is None: p.terminate()

def run_tasks(n, prefix):
    lat = []; ok = failed = 0; t0 = time.perf_counter()
    for i in range(n):
        s = time.perf_counter()
        try:
            r = requests.post(CONTROLLER + "/tasks",
                              json={"task_id": f"{prefix}-{i+1}", "work_units": 150000 + i * 10000},
                              timeout=30)
            if r.ok and r.json().get("status") != "failed": ok += 1; lat.append(time.perf_counter() - s)
            else: failed += 1
        except requests.RequestException: failed += 1
    total = time.perf_counter() - t0
    return {"successful_tasks": ok, "failed_tasks": failed,
            "throughput_tasks_per_second": round(ok / total, 4),
            "average_latency_seconds": round(statistics.mean(lat), 4) if lat else None,
            "jitter_seconds": round(statistics.pstdev(lat), 4) if len(lat) > 1 else 0,
            "packet_loss_percent": round(failed / n * 100, 2)}

def docker(*args):
    subprocess.run(["docker", *args], check=True, capture_output=True)

def failure_demo():
    """Failure -> Detection -> Recovery -> Service Restoration (defense Part E)."""
    out = {"leader_before": requests.post(CONTROLLER + "/coordination/election").json()["leader"]}
    docker("stop", "node2"); t_fail = time.perf_counter()
    workers = requests.get(CONTROLLER + "/workers").json()
    out["detected_offline"] = [w["worker"] for w in workers if w.get("status") == "offline"]
    out["re_election"] = requests.post(CONTROLLER + "/coordination/election").json()
    out["tasks_during_failure"] = run_tasks(4, "failover")
    docker("start", "node2")
    wait(WORKERS[1] + "/health")
    out["mttr_seconds"] = round(time.perf_counter() - t_fail, 3)
    out["leader_after_recovery"] = requests.post(CONTROLLER + "/coordination/election").json()["leader"]
    return out

def main():
    if DOCKER:
        if not wait(CONTROLLER + "/health", 5):
            sys.exit("Controller not reachable - run `docker compose up -d --build` first.")
    else:
        start()
    try:
        out = {}
        # M1
        out["milestone_1"] = {
            "architecture": requests.get(CONTROLLER + "/architecture").json(),
            "workers_before": requests.get(CONTROLLER + "/workers").json(),
            "slice_creation": requests.post(CONTROLLER + "/slices", json={
                "slice_id": "slice-m1-demo", "cpu_percent": 20,
                "memory_mb": 128, "bandwidth_mbps": 10, "priority": 5}).json()
        }
        # M2
        out["milestone_2"] = {**run_tasks(10, "task"),
                              "worker_measurements": requests.get(CONTROLLER + "/workers").json()}
        # M3
        out["milestone_3"] = {"deployment": requests.get(CONTROLLER + "/deployment").json()}
        # M4
        out["milestone_4"] = {
            "leader_election": requests.post(CONTROLLER + "/coordination/election").json(),
            "logical_clock_event": requests.post(CONTROLLER + "/coordination/event",
                                                 json={"event": "slice-created"}).json()
        }
        if DOCKER: out["failure_demo"] = failure_demo()

        path = RESULTS / ("docker_results.json" if DOCKER else "milestone_results.json")
        path.write_text(json.dumps(out, indent=2), encoding="utf-8")
        m2 = out["milestone_2"]; el = out["milestone_4"]["leader_election"]
        print("\n" + "=" * 62)
        print("DISTRIBUTED NETWORK SLICING PLATFORM - MILESTONES 1-4" + (" (Docker)" if DOCKER else ""))
        print("=" * 62)
        print("MILESTONE 1: OK distributed foundation, slice ->",
              out["milestone_1"]["slice_creation"].get("selected_worker"))
        print(f"MILESTONE 2: OK throughput={m2['throughput_tasks_per_second']} tasks/s")
        print(f"             latency={m2['average_latency_seconds']} s")
        print(f"             jitter={m2['jitter_seconds']} s")
        print(f"             packet loss={m2['packet_loss_percent']} %")
        print("MILESTONE 3: OK distributed architecture")
        print(f"MILESTONE 4: OK Bully election leader={el['leader']} "
              f"messages={el['coordination_message_count']} consistent={el['consistent']}")
        print(f"             logical clock={out['milestone_4']['logical_clock_event']['logical_clock']}")
        if DOCKER:
            f = out["failure_demo"]; t = f["tasks_during_failure"]
            print("FAILURE:     stopped node2 ->", "detected offline:", f["detected_offline"])
            print(f"             re-elected leader={f['re_election']['leader']}, "
                  f"tasks during failure {t['successful_tasks']}/{t['successful_tasks'] + t['failed_tasks']} succeeded")
            print(f"             node2 restarted, MTTR={f['mttr_seconds']} s, "
                  f"leader after recovery={f['leader_after_recovery']}")
        print(f"\nResults: {path}")
    finally: stop()

if __name__ == "__main__": main()
