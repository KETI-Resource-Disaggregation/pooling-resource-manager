"""폐루프 HTTP 배선 — feeder/lifecycle 라우트 (Exp_26, G2·G3).

prism_controller.py 에 최소 침습으로 얹는 디스패치 모듈. 상태는 여기서 보유.
정책 '판단'은 없음 — 트리거는 (a) 수동 API, (b) 구독 규칙(호출자가 선언한
transition→ratios 매핑; adaptive_map lookup 은 감사 기록용으로 첨부).

라우트:
  GET  /feeder/status            피더 상태 (목표/armed/재설정 이력)
  GET  /feeder/occupancy         time_stats 샘플 → 직전 대비 실측 점유
  POST /feeder/register          {tenant, sock, log, time_ratio}
  POST /feeder/arm|release       {tenant}
  POST /feeder/ratios            {ratios:{...}, reason}
  POST /feeder/env_policy        {policy, reason}   [Exp_40] strict|relaxed_hetero|capped_hetero
  GET  /lifecycle/state          상태머신 현재 상태 + 감사 로그
  GET  /lifecycle/ledger         residual 장부 view + 감사 로그
  POST /lifecycle/swap           make-before-break 사이클 (§swap body 참조)
  POST /subscribe                {events_path, rules:[{to, min_epoch, ratios,
                                  reason}]} — phase_online 구독 배선
swap body:
  {old:{tenant, drain_file}, gate:{ratios:{...}, reason},
   standby:{name, cmd:[...], env:{...}, log, ready_file,
            r, workload_class, confidence?},
   ready_timeout_s?}
  standby.r/workload_class → ratio.decide() 로 (s,t) 결정, BLESS_LIMIT_PCT 를
  controller 가 env 에 명시 주입 (rule_applied 감사 로그 — Exp_25 §5 인수인계).
"""
import json
import os
import subprocess
import threading
import time

from feeder import TimeCreditFeeder
from feeder.feeder import _send as feeder_send   # [Exp_27] 발신 경로 재사용
from lifecycle import (StateMachine, SwapOrchestrator, IllegalTransition,
                       ResidualLedger, EventSubscriber)
from priority import PriorityManager              # [Exp_27]
from booster import Booster                       # [Exp_28]
from ratio import decide, bless_limit_pct
from ratio.adaptive_iface import lookup as map_lookup

_feeder = None
_priority = None   # [Exp_27]
_booster = None    # [Exp_28]
_sm = None
_ledger = None
_subscriber = None
_audit = []            # lifecycle 감사 로그 (상태 전이 + 결정)
_swap_thread = None
_swap_procs = {}       # standby name -> Popen


def init():
    global _feeder, _sm, _ledger, _priority
    _feeder = TimeCreditFeeder()
    _feeder.start()
    _sm = StateMachine(audit=_audit)
    _ledger = ResidualLedger()
    # [Exp_27] 소켓 조회는 feeder 공개 status() 경유 (등록부 단일 출처)
    _priority = PriorityManager(
        send_fn=feeder_send,
        sock_resolver=lambda t: _feeder.status()["tenants"].get(t, {}).get("sock"))
    # [Exp_28] Booster — feeder/ledger/decide/EventSubscriber 재사용
    # (match: TERM_PREDICTED 전용 — 기본 transition 필터와 별개 구독)
    global _booster
    _booster = Booster(_feeder, _ledger, decide,
                       subscriber_factory=lambda p, h: EventSubscriber(
                           p, h, audit=_audit,
                           match=lambda e: e.get("event") == "TERM_PREDICTED"))


def stop():
    if _feeder:
        _feeder.stop()
    if _subscriber:
        _subscriber.stop()


def handle_get(h, path):
    if path == "/feeder/status":
        h._send_json(_feeder.status())
    elif path == "/feeder/occupancy":
        h._send_json(_feeder.sample_occupancy())
    elif path == "/priority/status":              # [Exp_27]
        h._send_json(_priority.status())
    elif path == "/booster/status":               # [Exp_28]
        h._send_json(_booster.status())
    elif path == "/lifecycle/state":
        h._send_json({"state": _sm.state, "audit": _audit[-50:]})
    elif path == "/lifecycle/ledger":
        h._send_json({"view": _ledger.view(), "audit": _ledger.audit[-50:]})
    else:
        h._send_json({"error": "not found"}, 404)


def handle_post(h, path, body):
    try:
        if path == "/feeder/register":
            # resource (Exp_45): "gpu" 기본 | "npu"=npu-proxy 소켓 경유
            # (채널 계약 동일 — NPU 는 시간 축 단독, s 미적용)
            _feeder.register(body["tenant"], body["sock"], body["log"],
                             float(body.get("time_ratio", 0.5)),
                             resource=body.get("resource", "gpu"))
            h._send_json({"ok": True})
        elif path == "/feeder/arm":
            _feeder.arm(body["tenant"])
            h._send_json({"ok": True})
        elif path == "/feeder/release":
            _feeder.release(body["tenant"])
            h._send_json({"ok": True})
        elif path == "/feeder/ratios":
            _feeder.set_ratios(body["ratios"], reason=body.get("reason", ""))
            h._send_json({"ok": True})
        elif path == "/feeder/env_policy":         # [Exp_40] 봉투 정책 opt-in
            _feeder.set_env_policy(body["policy"],
                                   reason=body.get("reason", ""))
            h._send_json({"ok": True, "policy": body["policy"]})
        elif path == "/priority":                  # [Exp_27] 등급 설정
            _priority.set_priority(body["tenant"], body["class"])
            h._send_json({"ok": True})
        elif path == "/urgent":                    # [Exp_27] 선점 (상태 — clear 까지)
            others = [t for t in _feeder.status()["tenants"]
                      if t != body["tenant"]]
            sent = _priority.urgent(body["tenant"], others)
            h._send_json({"ok": True, "pending_sent": sent})
        elif path == "/urgent_clear":              # [Exp_27] 선점 해제
            h._send_json({"ok": True, "cleared": _priority.urgent_clear()})
        elif path == "/booster/enable":            # [Exp_28]
            _booster.enable(body.get("mode", "event"),
                            events_path=body.get("events_path"),
                            base_url=body.get("base_url"))
            h._send_json({"ok": True, "mode": body.get("mode", "event")})
        elif path == "/booster/disable":           # [Exp_28]
            _booster.disable()
            h._send_json({"ok": True})
        elif path == "/booster/register":          # [Exp_28]
            _booster.register_tenant(body["tenant"], body["pid"], body["gpu"])
            h._send_json({"ok": True})
        elif path == "/booster/pending":           # [Exp_28]
            _booster.add_pending(body["r"], body["workload_class"])
            h._send_json({"ok": True})
        elif path == "/lifecycle/swap":
            r = _start_swap(body)
            h._send_json(r, 200 if "error" not in r else 409)
        elif path == "/subscribe":
            _start_subscribe(body)
            h._send_json({"ok": True})
        else:
            h._send_json({"error": "not found"}, 404)
    except IllegalTransition as e:
        h._send_json({"error": f"illegal transition: {e}"}, 409)
    except (KeyError, ValueError) as e:            # [Exp_27] 미등록/등급 오류
        h._send_json({"error": repr(e)}, 400)
    except Exception as e:
        h._send_json({"error": repr(e)}, 400)


# ---- swap (make-before-break, Exp_22 combined 이식) ----
def _start_swap(body):
    global _swap_thread
    if _swap_thread and _swap_thread.is_alive():
        return {"error": "swap already in progress"}

    old = body["old"]
    gate = body.get("gate", {})
    stand = body["standby"]

    # ratio 엔진 결정 (Exp_25 인수인계 경로) — residual 은 장부 view 에서
    view = _ledger.view()
    dec = decide(float(stand["r"]), stand["workload_class"],
                 free_sm_ratio=view["free_sm_ratio"],
                 free_time_ratio=max(view["free_time_ratio"],
                                     float(stand.get("min_free_time", 0.0))),
                 confidence=stand.get("confidence", "HIGH"))
    _audit.append({"t": round(time.time(), 3), "kind": "ratio_decision",
                   "tenant": stand["name"], "decision": dec})
    if not dec["feasible"]:
        return {"error": "ratio engine INFEASIBLE", "decision": dec}
    pct = bless_limit_pct(dec["space_ratio"])

    def gate_on():
        if gate.get("ratios"):
            _feeder.set_ratios(gate["ratios"],
                               reason=gate.get("reason", "swap gate_on"))
        for t in gate.get("ratios", {}):
            _feeder.arm(t)

    def gate_off():
        for t in gate.get("ratios", {}):
            _feeder.release(t)

    def spawn_standby():
        env = dict(os.environ)
        env.update({k: str(v) for k, v in stand.get("env", {}).items()})
        env["BLESS_LIMIT_PCT"] = str(pct)   # 항상 명시 (암묵 50% 캡 회피)
        logf = open(stand["log"], "a")
        p = subprocess.Popen(stand["cmd"], env=env, stdout=logf, stderr=logf)
        _swap_procs[stand["name"]] = p
        return p

    ready_file = stand["ready_file"]
    orch = SwapOrchestrator(
        _sm, gate_on, gate_off, spawn_standby,
        standby_ready=lambda p: os.path.exists(ready_file),
        standby_dead=lambda p: p.poll() is not None,
        drain_old=lambda: open(old["drain_file"], "w").close(),
        ready_timeout_s=float(body.get("ready_timeout_s", 60.0)))

    def run():
        final = orch.run(decision_audit={"rule_applied": dec["rule_applied"],
                                         "s": dec["space_ratio"],
                                         "t": dec["time_ratio"],
                                         "limit_pct": pct})
        if final == "NORMAL'":
            _ledger.remove(old["tenant"])
            _ledger.place(stand["name"], dec["space_ratio"],
                          dec["time_ratio"], gated=False)

    _swap_thread = threading.Thread(target=run, daemon=True)
    _swap_thread.start()
    return {"ok": True, "decision": {"space_ratio": dec["space_ratio"],
                                     "time_ratio": dec["time_ratio"],
                                     "limit_pct": pct,
                                     "rule_applied": dec["rule_applied"]}}


# ---- phase_online 구독 (Exp_20 폐루프 배선) ----
def _start_subscribe(body):
    global _subscriber
    if _subscriber:
        _subscriber.stop()
    rules = body.get("rules", [])

    def handler(e):
        tr = e["transition"]
        for rule in rules:
            if tr.get("to") != rule.get("to"):
                continue
            if e.get("t", 0) < float(rule.get("min_epoch", 0)):
                continue
            # adaptive_map lookup 은 감사 기록용 (규칙 자체는 호출자 선언)
            rec = (map_lookup(rule["lookup_labels"])
                   if rule.get("lookup_labels") else None)
            _feeder.set_ratios(rule["ratios"], reason=rule.get("reason", ""))
            _audit.append({"t": round(time.time(), 3), "kind": "loop_reset",
                           "transition": tr, "event_t": e.get("t"),
                           "ratios": rule["ratios"],
                           "map_lookup": rec,
                           "reason": rule.get("reason", "")})
            return

    _subscriber = EventSubscriber(body["events_path"], handler, audit=_audit)
    _subscriber.start()
