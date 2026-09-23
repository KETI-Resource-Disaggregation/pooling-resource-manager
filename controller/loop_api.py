"""폐루프 HTTP 배선 — feeder/lifecycle 라우트 (Exp_26, G2·G3).

kraken_controller.py 에 최소 침습으로 얹는 디스패치 모듈. 상태는 여기서 보유.
정책 '판단'은 없음 — 트리거는 (a) 수동 API, (b) 구독 규칙(호출자가 선언한
transition→ratios 매핑; adaptive_map lookup 은 감사 기록용으로 첨부).

라우트:
  GET  /feeder/status            피더 상태 (목표/armed/재설정 이력)
  GET  /feeder/occupancy         time_stats 샘플 → 직전 대비 실측 점유
  POST /feeder/register          {tenant, sock, log, time_ratio}
  POST /feeder/arm|release       {tenant}
  POST /feeder/deregister        {tenant}           [Exp_53] watcher 자동 정리
  POST /feeder/ratios            {ratios:{...}, reason, lease_s?}  [Exp_138 3-D·141]
                                  lease_s 미지정=기본 임대(만료 시 계약 복귀),
                                  >0=그 값, <=0=무기한 opt-out
  POST /feeder/ratios_release    {tenants:true|[...]}  [Exp_138 3-D] 임대 즉시 해제
  POST /feeder/mode_event        {reason_kind?, tenant, mode, reason, class,
                                  pod?, pod_ns?, pod_uid?}  [Exp_138 2-B 5·140 3부]
                                  ③ 전환·Slice 이벤트 발행 (K8s Event)
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
from scanner import StderrSignals          # [Exp_126 4부]
from ratio.adaptive_iface import lookup as map_lookup

_feeder = None
_priority = None   # [Exp_27]
_booster = None    # [Exp_28]
_sm = None
_ledger = None
_subscriber = None
_stderr = None
_audit = []            # lifecycle 감사 로그 (상태 전이 + 결정)
_swap_thread = None
_swap_procs = {}       # standby name -> Popen


_relocation_events = []   # [Exp_108 D-3] 재배치 추천 보관
RETENTION = {}            # [Exp_151 4-G] tenant → ④ 유지율 보고(신호 있는 것만)


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
    # [Exp_126 4부] stderr 신호 수집기 — 호출될 때만 동작한다(엔드포인트 opt-in).
    global _stderr
    _stderr = StderrSignals(os.environ.get("KRAKEN_SOCK_DIR", "/var/lib/kraken/socks"),
                            send_fn=feeder_send)
    global _booster
    _booster = Booster(_feeder, _ledger, decide,
                       subscriber_factory=lambda p, h: EventSubscriber(
                           p, h, audit=_audit,
                           match=lambda e: e.get("event") == "TERM_PREDICTED"))
    # [Exp_126 3-A] 종료 감지 자동 활성 — **기본 꺼짐**.
    #   ★근거: Booster 는 자원을 **해지**하는 액추에이터다. 기본 켜짐으로 두면
    #     오탐 하나가 남의 자원을 회수한다. 그렇다고 매 DS 재기동마다 사람이
    #     POST 를 날려야 하면 실무에서 영영 안 켜진다. 그래서 KRAKEN_REDIST(Exp_121)
    #     와 같은 형태 — 환경 변수로 켜고, 켜진 채 뜨면 로그에 남긴다.
    if os.environ.get("KRAKEN_BOOSTER", "0") == "1":
        ev = os.environ.get("KRAKEN_BOOSTER_EVENTS", "/var/lib/kraken/term_events.jsonl")
        try:
            _booster.enable("event", events_path=ev)
            print(f"[loop_api] 종료 감지 자동 활성: mode=event events={ev}", flush=True)
        except Exception as e:
            # [Exp_107 T-5] 조용한 폴백 금지
            print(f"[loop_api][경고] 종료 감지 자동 활성 실패: {e} — 꺼진 채로 진행한다",
                  flush=True)


def stop():
    if _feeder:
        _feeder.stop()
    if _subscriber:
        _subscriber.stop()


def handle_get(h, path):
    if path == "/feeder/status":
        h._send_json(_feeder.status())

    elif path == "/feeder/retention":       # [Exp_151 4-G] 유지율 조회(/metrics 소스)
        h._send_json({"retention": RETENTION})
    elif path == "/events/relocation":      # [Exp_108 D-3] 발행된 추천 조회
        h._send_json({"events": _relocation_events[-20:], "count": len(_relocation_events)})
    elif path == "/feeder/occupancy":
        h._send_json(_feeder.sample_occupancy())
    elif path == "/priority/status":              # [Exp_27]
        h._send_json(_priority.status())
    elif path == "/scanner/stderr":               # [Exp_126 4부]
        # libbless stderr 신호 — 호출 시에만 수집한다(주기 관측용, 고빈도 아님).
        out, tenants = {}, _feeder.status().get("tenants", {})
        for name, t in tenants.items():
            r = _stderr.collect(name, t.get("sock"))
            if r is not None:
                out[name] = r
        h._send_json({"tenants": out, "n": len(out), "asked": len(tenants)})
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
        # [Exp_108 D-3] 재배치 추천 수신 — 3단계 산출물. **실행하지 않고** 보관·중계만
        #   한다(오케스트로 수집 경로·고려대 스케줄링 계층이 소비 대상).
        if path == "/events/relocation":
            payload = body or {}
            print(f"[loop_api] RelocationRecommendation: "
                  f"aggressor={payload.get('aggressor')} reason={payload.get('reason')}",
                  flush=True)
            _relocation_events.append(payload)
            del _relocation_events[:-50]      # 최근 50건만 보관
            h._send_json({"ok": True, "queued": len(_relocation_events)})
        elif path == "/feeder/register":
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
        elif path == "/feeder/deregister":         # [Exp_53] 생명주기 자동화
            _feeder.deregister(body["tenant"])
            h._send_json({"ok": True})
        elif path == "/feeder/ratios":
            # [Exp_138 3-D] lease_s 를 주면 그 시간 뒤 계약값으로 자동 복귀한다.
            #   주지 않으면 구 동작(무기한) — 기존 호출자 무영향.
            _feeder.set_ratios(body["ratios"], reason=body.get("reason", ""),
                               lease_s=body.get("lease_s"))
            h._send_json({"ok": True})
        elif path == "/feeder/ratios_release":     # [Exp_138 3-D] 임대 즉시 해제
            h._send_json({"ok": True,
                          "restored": _feeder.release_ratio_lease(
                              body.get("tenants", True))})
        elif path == "/feeder/retention":          # [Exp_151 4-G] ④ 유지율 보고
            # 신호 있는 테넌트만 온다(④가 0 을 채워 보내지 않는다). /metrics 의
            # kraken_slice_retention_ratio 가 이 저장소를 읽는다. stale(>10s)은
            # /metrics 쪽에서 버린다 — 죽은 ④의 값이 계속 나가지 않게.
            t = body.get("tenant")
            if not t:
                h._send_json({"ok": False, "error": "tenant 필요"}, 400)
                return
            RETENTION[t] = {"retention": body.get("retention"),
                            "p95_ms": body.get("p95_ms"),
                            "base_ms": body.get("base_ms"),
                            "name": body.get("name"), "ts": time.time()}
            for k in [k for k, v in RETENTION.items()
                      if time.time() - v.get("ts", 0) > 60]:
                RETENTION.pop(k, None)
            h._send_json({"ok": True})
        elif path == "/feeder/mode_event":         # [Exp_138 2-B 5] ③ 전환 이벤트
            # Go 와이어러가 모드를 바꿀 때마다 사유와 함께 부른다. 발행 실패는
            # 제어를 막지 않으나 조용히 넘기지 않는다(T-5) — 사유를 응답에 싣는다.
            # [Exp_140 3부] reason 확장 — 새 발행 경로를 만들지 않고 이 경로에
            #   SliceCreated/SliceDestroyed 를 더한다(지시서 3-A). reason 은
            #   화이트리스트로 제한한다(임의 reason 주입 방지).
            from scanner.mode_event import emit as _emit_mode
            # [Exp_151 1부] ④의 이벤트를 이 경로로 모은다(발행 경로 일원화 —
            #   결함을 두 곳에서 고치지 않는다. Exp_150 422 가 사례).
            _ALLOWED = ("ControlModeSwitched", "SliceCreated", "SliceDestroyed",
                        "InterferenceDetected", "SpaceControlApplied")
            _reason = body.get("reason_kind", "ControlModeSwitched")
            if _reason not in _ALLOWED:
                h._send_json({"ok": False,
                              "error": f"reason {_reason!r} not in {_ALLOWED}"}, 400)
                return
            _t = body.get("tenant", "")
            _pod = body.get("pod", "")
            _pod_ns = body.get("pod_ns", "")
            if _reason in ("ControlModeSwitched", "InterferenceDetected",
                           "SpaceControlApplied"):
                _msg = (f"tenant={_t} mode={body.get('mode','')} "
                        f"reason={body.get('reason','')} class={body.get('class','')}")
                _name = ""            # 전환은 반복 사건 — generateName 유지
            else:
                _msg = (f"tenant={_t} pod={_pod_ns}/{_pod} "
                        f"{body.get('detail','')}").strip()
                # 결정적 이름 = 파드 UID 기준 멱등 (재시도·재조정·재기동 중복 방지)
                _uid = body.get("pod_uid", "")
                _suffix = _uid if _uid else _t
                _name = ("kraken-slice-created-" if _reason == "SliceCreated"
                         else "kraken-slice-destroyed-") + _suffix.lower()
            try:
                _r = _emit_mode(_reason, _msg, tenant=_t,
                                pod=_pod, pod_ns=_pod_ns, event_name=_name)
                h._send_json({"ok": True, "dup": _r == "dup"})
            except Exception as _e:
                print(f"[loop_api][경고] {_reason} 발행 실패: {_e!r} "
                      f"({_msg})", flush=True)
                h._send_json({"ok": False, "error": str(_e)})
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
        elif path == "/booster/register":          # [Exp_28] +[Exp_134] alive_path
            _booster.register_tenant(body["tenant"], body.get("pid", 0),
                                     body["gpu"], body.get("alive_path"))
            h._send_json({"ok": True})
        elif path == "/booster/deregister":        # [Exp_134] 파드 종료 시 해제
            h._send_json({"ok": _booster.deregister_tenant(body["tenant"])})
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
            # [Exp_141] lease_s=0 = 무기한 opt-out 명시. swap 게이트는 전환 창
            #   동안 유지돼야 하는데 창 길이(standby 기동 대기)가 기본 임대 3s 를
            #   넘을 수 있다. gate_off/deregister 가 정리 경로다.
            _feeder.set_ratios(gate["ratios"],
                               reason=gate.get("reason", "swap gate_on"),
                               lease_s=0)
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
