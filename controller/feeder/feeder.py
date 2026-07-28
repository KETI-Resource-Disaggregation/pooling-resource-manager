"""time_credit 상주 피더 — controller↔libbless 시간 노브 채널 (Exp_26, G3).

Exp_25 run_e2e.py feed() 참조 구현(출처 md5 ffc34811)의 controller 상주 승격.
프로토콜은 Exp_16 보강판 실측 검증 경로 그대로 (libbless.cpp time_mode/
time_credit/time_add/time_stats — 정본 ee7e63e7):
  arm    : "time_mode 1" + "time_credit 0"  (즉시 게이트 무장, Exp_16 §go 시점)
  tick   : TICK_S(10ms, Exp_16)마다 각 테넌트에 "time_add <TICK×1e6×scale×ratio_i/Σ>"
  release: "time_credit -1"  (unlimited 플래그 — Exp_16 보강 ① 의미론, Exp_22 gate_off)
  샘플   : "time_stats" → 테넌트 stderr 로그의 total=(청구 μs) 파싱 → 점유 산출

envelope 스케일: 기본 Σ=1.0 (in-envelope — Exp_16 격리 조건). 완화값(예: 1.6,
Exp_9 §MPS 중첩 근거)은 옵션으로만 — 기본값 아님 (Exp_26 스펙 §작업1).
"""
import json
import os
import re
import socket
import threading
import time

TICK_S = 0.010            # Exp_16 실측 검증 tick (10ms) — 오차 ≤0.007 의 조건
DEFAULT_ENV_SCALE = 1.0   # in-envelope Σ=TICK (Exp_16). 완화(1.6 등)는 옵션

# 봉투 정책 (Exp_40): strict = Σ(등록 ratio) ≤ 1.0 강제 (현행 의미론).
# relaxed_hetero/capped_hetero = 이종 페어 완화 규약에서만 Σ>1 등록 허용
# (피해자 t=1.0 은 게이트 미무장이 정상 경로 — budgets() 의 in-envelope
# 불변식은 armed 집합에만 걸리므로 그대로 유지된다. Exp_39b C/P 팔).
ENV_POLICIES = ("strict", "relaxed_hetero", "capped_hetero")


def _send(sock_path, msg):
    """libbless control socket (SOCK_DGRAM 1-way, Exp_16 send() 동일)."""
    try:
        s = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
        s.sendto(msg.encode(), sock_path)
        s.close()
        return True
    except OSError:
        return False


def read_time_stats(logpath):
    """테넌트 stderr 로그의 마지막 'total=<μs> kernels=<n>' (Exp_16 동일)."""
    try:
        txt = open(logpath).read()
    except OSError:
        return None
    m = re.findall(r"total=(-?\d+) kernels=(\d+)", txt)
    return (int(m[-1][0]), int(m[-1][1])) if m else None


class TimeCreditFeeder:
    """상주 피더 스레드. send_fn/clock 주입 가능 (단위 테스트 mock 용)."""

    def __init__(self, env_scale=DEFAULT_ENV_SCALE, send_fn=_send,
                 sleep_fn=time.sleep):
        self._lock = threading.Lock()
        self._tenants = {}        # name -> {sock, log, ratio, armed}
        self._history = []        # 재설정 이력 [{t, ratios, reason}]
        self._last_sample = {}    # name -> (total_us, kernels, t)
        self._env_scale = float(env_scale)
        self._env_policy = "strict"   # 기본값 = 현행 동작 (Exp_40 opt-in)
        self._send = send_fn
        self._sleep = sleep_fn
        self._stop = threading.Event()
        self._thread = None

    # ---- 봉투 정책 (Exp_40) ----
    def set_env_policy(self, policy, reason=""):
        if policy not in ENV_POLICIES:
            raise ValueError(f"unknown env_policy {policy!r} — {ENV_POLICIES}")
        with self._lock:
            self._env_policy = policy
            self._history.append({"t": round(time.time(), 3),
                                  "env_policy": policy, "reason": reason})

    # ---- 테넌트 관리 ----
    def register(self, name, sock_path, log_path, time_ratio, resource="gpu"):
        """등록. Σratio 검증 (Exp_40 Phase 0 확정 사항):

        기존 의미론(하위호환 — test_10 이 감시)은 Σ>1 등록을 거부하지 않고
        budgets() 의 denom=max(1,Σ) 정규화로 봉투를 방어한다. strict 는 그
        동작을 그대로 유지하되, Σ>1 이 되는 등록을 이력에 경고로 남긴다
        (관측 가능성). relaxed/capped 정책에서는 Σt>1 이 규약상 정상
        (피해자 t=1.0 미무장 경로)이므로 경고도 남기지 않는다.
        """
        with self._lock:
            others = sum(t["ratio"] for n, t in self._tenants.items()
                         if n != name)
            if (self._env_policy == "strict"
                    and others + float(time_ratio) > 1.0 + 1e-9):
                self._history.append(
                    {"t": round(time.time(), 3),
                     "warning": f"strict 봉투에서 Σratio="
                                f"{others + float(time_ratio):.4f} > 1.0 등록"
                                f" — budgets 정규화로 방어됨 (완화 규약은 "
                                f"env_policy opt-in, Exp_40)",
                     "tenant": name})
            # resource (Exp_45): "gpu"(libbless 직접) | "npu"(npu-proxy 대행).
            # 채널·명령·회계가 동일하므로 feeder 동작은 타입 무관 — 태그는
            # 관측/문서용. ★NPU 는 공간 축(s) 없음: PE 는 배타 단위(Exp_36)라
            # NPU 테넌트 분해는 시간 축 단독 — decide_pair 의 s 개념 미적용.
            self._tenants[name] = {"sock": sock_path, "log": log_path,
                                   "ratio": float(time_ratio), "armed": False,
                                   "resource": str(resource)}

    def deregister(self, name):
        with self._lock:
            self._tenants.pop(name, None)
            self._last_sample.pop(name, None)

    def arm(self, name):
        """게이트 무장 (Exp_16: time_mode 1 + time_credit 0)."""
        with self._lock:
            t = self._tenants[name]
            t["armed"] = True
        self._send(t["sock"], "time_mode 1")
        self._send(t["sock"], "time_credit 0")

    def release(self, name):
        """게이트 해제 = unlimited (Exp_22 gate_off: time_credit -1)."""
        with self._lock:
            t = self._tenants.get(name)
            if t:
                t["armed"] = False
        if t:
            self._send(t["sock"], "time_credit -1")

    # ---- 목표 변경 (Exp_20 재설정 경로) ----
    def set_ratios(self, ratios, reason=""):
        """time_ratio 목표 런타임 변경 — 다음 tick 부터 반영."""
        with self._lock:
            for name, r in ratios.items():
                if name in self._tenants:
                    self._tenants[name]["ratio"] = float(r)
            self._history.append({"t": round(time.time(), 3),
                                  "ratios": dict(ratios), "reason": reason})

    def budgets(self):
        """이번 tick 의 테넌트별 time_add 예산(μs) — in-envelope 분배.

        불변식: Σbudget ≤ TICK×1e6×env_scale.
        분모는 max(1, Σratio) — Σ≤1 이면 ratio 를 절대 duty 로 해석
        (단독 게이트 duty 0.5 = Exp_20/22 gate 케이스), Σ>1 이면 비례 정규화.
        """
        with self._lock:
            armed = {n: t for n, t in self._tenants.items() if t["armed"]}
            tot = sum(t["ratio"] for t in armed.values())
            if tot <= 0:
                return {}
            pool = TICK_S * 1e6 * self._env_scale
            denom = max(1.0, tot)
            return {n: max(1, int(pool * t["ratio"] / denom))
                    for n, t in armed.items()}

    # ---- 상주 루프 ----
    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=2.0)

    def _run(self):
        while not self._stop.is_set():
            for name, b in self.budgets().items():
                with self._lock:
                    sock = self._tenants.get(name, {}).get("sock")
                if sock:
                    self._send(sock, f"time_add {b}")
            self._sleep(TICK_S)

    # ---- 상태 조회 (점유 실측) ----
    def sample_occupancy(self, settle_s=0.1):
        """time_stats 스냅샷 → 직전 샘플 대비 charged_us 점유 비율."""
        with self._lock:
            tenants = dict(self._tenants)
        for name, t in tenants.items():
            self._send(t["sock"], "time_stats")
        self._sleep(settle_s)
        now = time.time()
        deltas = {}
        for name, t in tenants.items():
            st = read_time_stats(t["log"])
            if st is None:
                continue
            prev = self._last_sample.get(name)
            self._last_sample[name] = (st[0], st[1], now)
            if prev:
                deltas[name] = st[0] - prev[0]
        tot = sum(deltas.values())
        share = ({n: round(d / tot, 4) for n, d in deltas.items()}
                 if tot > 0 else {})
        return {"charged_delta_us": deltas, "observed_share": share}

    def status(self):
        with self._lock:
            tenants = {n: {"ratio": t["ratio"], "armed": t["armed"],
                           "sock": t["sock"],
                           "resource": t.get("resource", "gpu")}
                       for n, t in self._tenants.items()}
            hist = list(self._history[-20:])
        tot = max(1.0, sum(t["ratio"] for t in tenants.values() if t["armed"]))
        for n, t in tenants.items():
            t["target_share"] = round(t["ratio"] / tot, 4) if t["armed"] else None
        return {"tick_ms": TICK_S * 1000, "env_scale": self._env_scale,
                "env_policy": self._env_policy,
                "tenants": tenants, "recent_resets": hist}
