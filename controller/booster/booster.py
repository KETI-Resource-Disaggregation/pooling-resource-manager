"""Turnaround Booster 본체 — TERM 이벤트 구독 → 선제 해지 → 즉시 재할당 준비 (Exp_28, G4).

트리거 결정문 (Exp_28 report §2):
  v1 트리거 = **신속 사후** (vram_release, HIGH) — 프로세스 소멸 직후 VRAM 해제
  신호로 폴링 대기 없이 즉시 해지. idle_hold(LOW)는 관측·감사만 (오탐 방어 정책
  = HIGH confidence 만 액션). 진짜 선제(종료 '전' 해지)는 periodicity 기반 ETA 의
  온라인화가 선행 — 이연.

오탐 방어 (v1 규칙):
  ① confidence HIGH 만 액션  ② 액션 전 생존 재확인 1회 (/proc/<pid> — grace
  대기 없음: 실측 근거 없는 상수 도입 금지 규약). pid 는 이벤트가 아니라
  **Booster 등록부**에서 얻는다 (vram_release 시점엔 프로세스 소멸 → 이벤트
  pids=[] 가 정상, Exp_24 이연 #2 — term_online 수정 불필요로 결정).

파이프라인 (각 단계 타임스탬프 기록):
  t_detect(이벤트 수신/폴링 인지) → ① 생존 재확인 → ② feeder 정리(release+
  deregister) → ③ ledger 자원 반환 → ④ controller /deregister_by_id (로컬 HTTP,
  watcher.go 와 동일 경로) → ⑤ 재할당 준비: 대기 요청에 decide() → t_realloc_ready

mode="event" = TERM_PREDICTED 구독 (본체).
mode="poll"  = OFF 기준선 — 기존 감지 경로의 로컬 대응물: 1.0s 폴링
  (근거: prism_controller.py:186 idle 감지 루프 1초 간격 — 저장소 내 기존
  회수 경로의 폴링 주기. watcher.go checkpoint 15s 는 K8s 경로라 제외).
  파이프라인 ②~⑤ 는 양 모드 동일 — 차이는 감지뿐 (단계 분해 대조용).
"""
import json
import os
import threading
import time
import urllib.request

POLL_BASELINE_S = 1.0   # prism_controller.py:186 (기존 idle 감지 루프 1초 간격)


def _pid_alive(pid):
    return os.path.exists(f"/proc/{pid}")


class Booster:
    def __init__(self, feeder, ledger, decide_fn, subscriber_factory):
        """feeder/ledger: Exp_26 인스턴스 재사용. decide_fn: ratio.decide.
        subscriber_factory(events_path, handler) -> EventSubscriber (Exp_26)."""
        self._feeder = feeder
        self._ledger = ledger
        self._decide = decide_fn
        self._sub_factory = subscriber_factory
        self._lock = threading.Lock()
        self._tenants = {}        # name -> {pid, gpu}
        self._pending = []        # 재할당 대기 요청 [{r, workload_class}]
        self._enabled = False
        self._mode = None
        self._base_url = None
        self._subscriber = None
        self._poll_thread = None
        self._poll_stop = threading.Event()
        self.audit = []
        self.actions = []         # 완료된 해지 액션 (단계 타임스탬프)

    def _log(self, kind, **kw):
        with self._lock:
            self.audit.append({"t": round(time.time(), 6), "kind": kind, **kw})

    # ---- 등록 ----
    def register_tenant(self, name, pid, gpu):
        with self._lock:
            self._tenants[name] = {"pid": int(pid), "gpu": str(gpu)}
        self._log("tenant_registered", tenant=name, pid=pid, gpu=gpu)

    def add_pending(self, r, workload_class):
        with self._lock:
            self._pending.append({"r": float(r),
                                  "workload_class": workload_class})
        self._log("pending_added", r=r, workload_class=workload_class)

    # ---- 활성/비활성 ----
    def enable(self, mode, events_path=None, base_url=None):
        self.disable()
        self._mode = mode
        self._base_url = base_url
        self._enabled = True
        if mode == "event":
            self._subscriber = self._sub_factory(events_path, self._on_event)
            self._subscriber.start()
        elif mode == "poll":
            self._poll_stop.clear()
            self._poll_thread = threading.Thread(target=self._poll_loop,
                                                 daemon=True)
            self._poll_thread.start()
        else:
            raise ValueError(f"mode ∈ {{event, poll}}: {mode!r}")
        self._log("enabled", mode=mode, events_path=events_path)

    def disable(self):
        self._enabled = False
        if self._subscriber:
            self._subscriber.stop()
            self._subscriber = None
        if self._poll_thread:
            self._poll_stop.set()
            self._poll_thread.join(timeout=2.0)
            self._poll_thread = None
        self._log("disabled")

    # ---- 감지: event 모드 ----
    def _on_event(self, e):
        t_detect = time.time()
        if not self._enabled or e.get("event") != "TERM_PREDICTED":
            return
        ev = e.get("evidence", {})
        if ev.get("confidence") != "HIGH":            # 오탐 방어 ①
            self._log("skip_low_confidence", basis=ev.get("basis"),
                      confidence=ev.get("confidence"))
            return
        tenant = self._match_tenant(e)
        if tenant is None:
            self._log("skip_no_match", gpu=e.get("gpu"), pids=e.get("pids"))
            return
        self._run_pipeline(tenant, t_detect, trigger="TERM_PREDICTED",
                           event_ts=e.get("ts"))

    def _match_tenant(self, e):
        """gpu 일치 (+이벤트 pids 있으면 pid 교차 확인). 모호하면 None."""
        with self._lock:
            cand = [n for n, v in self._tenants.items()
                    if v["gpu"] == str(e.get("gpu"))]
            if e.get("pids"):
                cand = [n for n in cand
                        if self._tenants[n]["pid"] in e["pids"]] or cand
        return cand[0] if len(cand) == 1 else None

    # ---- 감지: poll 모드 (OFF 기준선) ----
    def _poll_loop(self):
        while not self._poll_stop.is_set():
            with self._lock:
                snapshot = dict(self._tenants)
            for name, v in snapshot.items():
                if not _pid_alive(v["pid"]):
                    self._run_pipeline(name, time.time(), trigger="poll_1s")
            self._poll_stop.wait(POLL_BASELINE_S)

    # ---- 해지 파이프라인 ----
    def _run_pipeline(self, tenant, t_detect, trigger, event_ts=None):
        with self._lock:
            info = self._tenants.get(tenant)
        if info is None:
            return
        act = {"tenant": tenant, "trigger": trigger, "event_ts": event_ts,
               "t_detect": round(t_detect, 6)}
        # ① 생존 재확인 (오탐 방어 ②) — 즉시 1회, grace 없음
        if _pid_alive(info["pid"]):
            act["aborted"] = "alive_recheck"
            self._log("action_aborted_alive", **act)
            return
        act["t_recheck"] = round(time.time(), 6)
        # ② feeder 정리 (release 는 소멸 프로세스라 발신 실패 무해)
        self._feeder.release(tenant)
        self._feeder.deregister(tenant)
        act["t_feeder_done"] = round(time.time(), 6)
        # ③ ledger 자원 반환
        self._ledger.remove(tenant)
        act["t_ledger_done"] = round(time.time(), 6)
        # ④ controller 등록 해제 (watcher.go 동일 경로 — 로컬 HTTP)
        if self._base_url:
            try:
                req = urllib.request.Request(
                    self._base_url + "/deregister_by_id",
                    json.dumps({"tenant_id": tenant}).encode(),
                    {"Content-Type": "application/json"})
                with urllib.request.urlopen(req, timeout=5) as r:
                    act["deregister"] = json.loads(r.read())
            except Exception as ex:
                act["deregister"] = {"error": repr(ex)}
        act["t_dereg_done"] = round(time.time(), 6)
        # ⑤ 즉시 재할당 준비: 반환분으로 대기 요청 decide()
        with self._lock:
            pending = self._pending.pop(0) if self._pending else None
            self._tenants.pop(tenant, None)
        if pending:
            view = self._ledger.view()
            dec = self._decide(pending["r"], pending["workload_class"],
                               free_sm_ratio=view["free_sm_ratio"],
                               free_time_ratio=view["free_time_ratio"])
            act["realloc_decision"] = {"feasible": dec["feasible"],
                                       "s": dec["space_ratio"],
                                       "t": dec["time_ratio"],
                                       "rule": dec["rule_applied"]}
        act["t_realloc_ready"] = round(time.time(), 6)
        with self._lock:
            self.actions.append(act)
        self._log("action_done", **{k: act[k] for k in
                                    ("tenant", "trigger", "t_detect",
                                     "t_realloc_ready")})

    def status(self):
        with self._lock:
            return {"enabled": self._enabled, "mode": self._mode,
                    "tenants": dict(self._tenants),
                    "pending": list(self._pending),
                    "actions": self.actions[-30:],
                    "audit": self.audit[-40:]}
