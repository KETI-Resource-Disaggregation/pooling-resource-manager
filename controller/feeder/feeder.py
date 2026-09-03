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

# ── [Exp_121] 유휴 몫 재분배 ─────────────────────────────────────────────────
#   ★기본 꺼짐(opt-in). KRAKEN_REDIST=1 로 켠다. 꺼진 상태의 동작은 기존과 동일하다.
REDIST_ON = os.environ.get("KRAKEN_REDIST", "0") == "1"
#   "더 쓸 의사" 임계 — 한 평가창에서 **준 것 대비 안 쓰고 남긴 양**이 그 창의
#   tick 하나 분 이하면 "다 쓰고 더 원한다"로 본다.
#   ★임의 상수를 쓰지 않으려고 절대 μs 도, 임의 퍼센트도 아닌 **계측 입자 = tick 하나**
#     로 잡는다: 허용 여유 = 부여량 ÷ REDIST_EVAL_TICKS (창이 25 틱이면 4%).
#     tick 이나 창 길이를 바꿔도 "한 틱 분 오차는 봐준다"는 의미가 유지된다.
REDIST_HUNGRY_FRAC = float(os.environ.get("KRAKEN_REDIST_HUNGRY_FRAC", "1.0"))
#   진동 방지 — 연속 N 틱 같은 판정이어야 상태를 바꾸고(HOLD), 바꾼 뒤 M 틱은 고정(DWELL).
#   근거: closed_loop 의 intervene-hold/min-dwell 과 같은 형태. 값은 그쪽 기본(3s/1s)을
#   tick(10ms) 단위로 옮긴 것이 아니라, **재분배는 tick 마다 도는 값싼 조정**이므로
#   훨씬 짧게 잡는다. 실측으로 진동이 보이면 늘린다(Exp_121 2-E).
REDIST_HOLD = int(os.environ.get("KRAKEN_REDIST_HOLD", "5"))
REDIST_DWELL = int(os.environ.get("KRAKEN_REDIST_DWELL", "10"))
#   의사 판정 주기 — tick(10ms)마다 재판정하면 (ㄱ) libbless 로그가 초당 수백 줄로 불고
#   (ㄴ) 크레딧이 갱신되기도 전에 다시 읽는다. tick 배수로 둔다.
#   창 길이는 (ㄱ) 허용 여유가 계측 잡음보다 커야 하고 (ㄴ) 판정이 응답성을 잃지
#   않아야 한다. 25 tick = 250ms → 여유 4%, HOLD 5 회에 1.25s 만에 전환된다.
REDIST_EVAL_TICKS = int(os.environ.get("KRAKEN_REDIST_EVAL_TICKS", "25"))

_REDIST_WARNED = set()


def _redist_warn(msg):
    """[Exp_107 T-5] 조용한 폴백 금지 — 같은 사유는 1회만."""
    if msg not in _REDIST_WARNED:
        _REDIST_WARNED.add(msg)
        print(f"[feeder][경고] 재분배: {msg}", flush=True)


def _send(sock_path, msg):
    """libbless control socket (SOCK_DGRAM 1-way, Exp_16 send() 동일)."""
    try:
        s = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
        s.sendto(msg.encode(), sock_path)
        s.close()
        return True
    except OSError:
        return False


def read_gate_stats(logpath):
    """[Exp_108 D-1/D-2] libbless gate_stats 발화 줄 파싱.
    형식: "gate_count=<n> gate_total_us=<us> gate_max_us=<us>"
    BLESS_GATE_METRICS 가 꺼져 있으면 줄이 없어 None 을 반환한다(정상)."""
    try:
        with open(logpath) as f:
            last = None
            for line in f:
                if line.startswith("gate_count="):
                    last = line
            if not last:
                return None
            kv = dict(p.split("=", 1) for p in last.split())
            return (int(kv["gate_count"]), int(kv["gate_total_us"]),
                    int(kv["gate_max_us"]))
    except Exception:
        return None


# [Exp_121 실측] libbless 의 잔여 크레딧(time_credit)을 **직접 읽는 경로는 없다.**
#   libbless 는 time_stats 응답을 두 곳에 낸다 — 전체 필드(time_credit 포함)는 stderr,
#   feed.log 에는 "total=<μs> kernels=<n>" 두 값만 쓴다(libbless.cpp:1023/1033).
#   stderr 은 워크로드 컨테이너 안이라 컨트롤러가 볼 수 없다. libbless 를 고치는 방법도
#   있으나 .so 는 Exp_85 이후 무수정 상태를 유지 중이라 건드리지 않았다.
#   → 대신 피더가 **자기가 준 것(Σtime_add) 빼기 실제로 쓴 것(Δtotal)** 으로 잔량을
#     재구성한다. libbless 내부 회계와 같은 양이다(credit 은 time_add 로 더해지고
#     커널 실행 elapsed 만큼 빠지며, total_time_us 는 같은 elapsed 를 더한다 —
#     libbless.cpp:361/376, 547/550).


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
        self._last_gate = {}      # [Exp_108 D-1] name -> (count, total_us, max_us)
        # [Exp_121] 재분배 상태 — name -> {hungry, streak, since}
        self._redist_state = {}
        self._redist_extra = {}   # 직전 평가에서 확정한 가산분(μs)
        self._redist_eval_t = 0.0
        self._redist_last = {}    # 관측용 스냅샷(로그·status 노출)
        self._redist_probe_due = False
        self._granted = {}        # name -> 누적 부여 예산(μs)
        self._redist_win = {}     # name -> (granted_cum, charged_cum) 직전 평가창
        self._redist_trace = []   # [Exp_121 2-E] 배분값 시간 궤적
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

    def _redist_forget(self, name):
        """[Exp_121] 테넌트가 빠지면 재구성 상태도 버린다(다음 입주자 오염 방지)."""
        self._redist_state.pop(name, None)
        self._redist_win.pop(name, None)
        self._granted.pop(name, None)
        self._redist_extra.pop(name, None)

    def deregister(self, name):
        with self._lock:
            self._tenants.pop(name, None)
            self._last_sample.pop(name, None)
            self._redist_forget(name)

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

    # ── [Exp_107 T-3] weight 변환부 ───────────────────────────────────────
    # 배선과 변환을 분리한다. 고려대와 몫 모델(예약형 vs 비례형)을 협의 중이므로
    # 변환 규칙은 확정할 수 없다. 그러나 weight 가 게이트까지 **도달하는 배선**은
    # 어느 모델에서도 필요하므로 이번에는 배선만 만든다.
    #
    #   ★잠정 선택: effective_ratio = ratio(LSU 비례) × weight
    #   ★기본값 1.0 — weight 미설정 시 기존 LSU 비례 동작이 그대로 유지된다
    #     (Exp_106 회귀 기준: LSU 비 3:1 → 실측 2.94).
    #   협의 결과가 나오면 이 함수만 바꿔 끼운다.
    @staticmethod
    def _apply_weight(ratio, weight):
        return ratio * weight

    def set_weights(self, weights):
        """[Exp_107 T-3] controller 가 /policy/weights 수신 시 밀어넣는다.
        weights: {tenant_id: float}. 미포함 테넌트는 1.0(=기존 LSU 비례 유지).
        ★feeder 가 SHM 을 직접 읽지 않는다 — 판독 책임을 controller 에 두어
          feeder 는 순수 분배기로 남긴다."""
        with self._lock:
            self._weights = dict(weights or {})
            self._w_ver = getattr(self, "_w_ver", 0) + 1

    def budgets(self):
        """이번 tick 의 테넌트별 time_add 예산(μs) — in-envelope 분배.

        불변식: Σbudget ≤ TICK×1e6×env_scale.
        분모는 max(1, Σratio) — Σ≤1 이면 ratio 를 절대 duty 로 해석
        (단독 게이트 duty 0.5 = Exp_20/22 gate 케이스), Σ>1 이면 비례 정규화.
        [Exp_107 T-3] ratio 에 weight 를 곱한 유효 비율로 분배한다.

        [Exp_121] ★유휴 몫 재분배(opt-in). 기본 꺼짐이라 위 동작이 그대로다.
          Σ<1 이면 1-Σ 만큼이 아무에게도 배정되지 않는다(Exp_108 D-4: LSU 60 둘이면
          32.6% 유휴). 그 남는 몫을 **더 쓸 의사가 있는** 테넌트에게 얹는다.
        """
        with self._lock:
            w = getattr(self, "_weights", {})
            armed = {n: t for n, t in self._tenants.items() if t["armed"]}
            eff = {n: self._apply_weight(t["ratio"], w.get(n, 1.0))
                   for n, t in armed.items()}
            tot = sum(eff.values())
            if tot <= 0:
                return {}
            pool = TICK_S * 1e6 * self._env_scale
            denom = max(1.0, tot)
            base = {n: max(1, int(pool * eff[n] / denom)) for n in armed}
            if not REDIST_ON or denom > 1.0:
                # 꺼짐이거나 Σ>1(남는 몫 없음) → 기존 동작 그대로
                return base
            extra = self._redistribute(armed, eff, tot, pool)
            if not extra:
                return base
            return {n: base[n] + extra.get(n, 0) for n in base}

    def _redistribute(self, armed, eff, tot, pool):
        """[Exp_121 1-B~1-D] 남는 몫을 '더 쓸 의사가 있는' 테넌트에게 나눈다.

        ★의사 판단 = **잔여 크레딧**. gate_count 는 쓰지 않는다(Exp_120: 몫이 클수록
          늘어나는 역반응 — 많이 도니까 더 자주 게이트를 지난다).
          libbless 의 time_credit 은 컨테이너 밖에서 읽을 수 없어(위 주석) 피더가
          **준 것(Δ Σtime_add) − 쓴 것(Δcharged)** 으로 한 평가창의 잔량을 재구성한다.
          그 잔량(누적, 결정 지평으로 클램프)이 허용 여유(tick 하나 분 부여량) 이하면
          "다 쓰고 더 원한다"로 본다.
        ★판정 시점 — Exp_118 교훈(제어가 관측을 바꾸면 판정이 요동친다). 여기도 같은
          구조라 세 겹으로 막는다.
            (ㄱ) 판정은 tick 이 아니라 REDIST_EVAL_TICKS 주기로만 한다
            (ㄴ) 연속 REDIST_HOLD 회 같은 판정이어야 상태를 바꾼다
            (ㄷ) 바꾼 뒤 REDIST_DWELL 평가주기 동안은 다시 바꾸지 않는다
        ★분배 비율 = **원래 몫에 비례**(1-C). 근거 — 균등 분배는 LSU 10 과 LSU 150 이
          같은 여유를 받아 계약 순서가 뒤집힌다. 비례는 순서를 보존한다. 몫이 작은 쪽이
          계속 불리하다는 우려는, **최소 보장이 base 로 이미 깔려 있으므로** '불리'가
          아니라 '덜 유리'일 뿐이다.
        ★최소 보장 침범 방지(1-D): 이 함수는 **가산분만** 돌려준다. base 를 깎는 경로가
          코드에 존재하지 않는다. 총합도 봉투를 넘지 않는다:
            Σbase + Σextra ≤ pool·Σeff + pool·(1-Σeff) = pool.
        """
        idle = 1.0 - tot
        if idle <= 1e-6:
            return {}
        now = time.time()
        if (now - self._redist_eval_t) < REDIST_EVAL_TICKS * TICK_S:
            # 평가 주기 밖 — 직전 확정분을 그대로 유지(진동 방지 (ㄱ))
            return {n: v for n, v in self._redist_extra.items() if n in armed}
        self._redist_eval_t = now
        self._redist_probe_due = True     # 이번 평가 뒤 한 번만 프로브
        win = max(1, REDIST_EVAL_TICKS)
        want, slack_us, allow_us = [], {}, {}
        for n, t in armed.items():
            st_ts = read_time_stats(t["log"])
            state = self._redist_state.setdefault(
                n, {"hungry": False, "streak": 0, "since": 0.0})
            if st_ts is None:
                # [Exp_107 T-5] 조용한 폴백 금지 — 못 읽으면 '안 배고픔'으로 넘어가지
                #   않고 대상에서 빼고 경고한다.
                _redist_warn(f"{n}: charged 를 읽지 못했다 → 재분배 대상 제외"
                             f" (log={t['log']}). time_stats 미발화 또는 경로 오류")
                state["hungry"] = False
                state["streak"] = 0
                self._redist_win.pop(n, None)
                continue
            g_cum, c_cum = self._granted.get(n, 0), st_ts[0]
            anchor = self._redist_win.get(n)
            if anchor is None:
                self._redist_win[n] = (g_cum, c_cum)
                continue          # 기준점만 잡고 이번 창은 판정하지 않는다
            bal = (g_cum - anchor[0]) - (c_cum - anchor[1])
            # ★누적 잔량을 쓴다(창 델타가 아니라). 실측 근거 — charged 는 배치 단위로
            #   몰려 갱신돼(250ms 창에서 0 과 0.8s 가 번갈아 나온다) 창 델타는 판정이
            #   튄다. 누적은 적분이라 그 요동이 상쇄된다.
            # ★단, 무한정 쌓이면 오래된 과거가 판정을 지배한다. **결정 지평**
            #   (HOLD × 창 = 상태를 바꾸는 데 필요한 시간)의 부여량으로 양쪽을 자른다.
            #   그 밖의 값은 판정에 추가 정보를 주지 않는다.
            per_tick = pool * eff[n]
            cap = per_tick * REDIST_HOLD * win
            if abs(bal) > cap:
                signed = cap if bal > 0 else -cap
                self._redist_win[n] = (g_cum - signed, c_cum)   # 앵커를 당겨 클램프
                bal = signed
            # ★판정의 성격: 잔량이 적분이므로 **조금이라도 꾸준히 남기는 작업은
            #   결국 임계를 넘어 제외된다**(3% 를 남기면 33 tick 만에 넘는다).
            #   "몫을 다 못 쓰는 작업에는 더 주지 않는다"가 규칙이고, 임계는 그 경계를
            #   tick 하나 분 부여량으로 놓은 것이다. 반대로 실측 워크로드는 부여량보다
            #   **더 쓰는**(적자) 쪽이라 −cap 에 눌러앉는다(2부 §slack 관측).
            allow = per_tick * REDIST_HUNGRY_FRAC   # tick 하나 분 부여량
            slack_us[n] = int(bal)
            allow_us[n] = int(allow)
            cur = (bal <= allow)
            if cur == state["hungry"]:
                state["streak"] = 0
            else:
                state["streak"] += 1
                if (state["streak"] >= REDIST_HOLD
                        and (now - state["since"])
                            >= REDIST_DWELL * REDIST_EVAL_TICKS * TICK_S):
                    state["hungry"] = cur
                    state["since"] = now
                    state["streak"] = 0
            if state["hungry"]:
                want.append(n)
        wtot = sum(eff[n] for n in want)
        out = ({n: int(pool * idle * eff[n] / wtot) for n in want}
               if wtot > 0 else {})
        self._redist_extra = out
        snap = {"t": round(now, 3), "idle": round(idle, 4),
                "slack_us": slack_us, "allow_us": allow_us,
                "want": sorted(want), "extra_us": out}
        self._redist_last = snap
        self._redist_trace.append(snap)
        if len(self._redist_trace) > 4000:
            del self._redist_trace[:2000]
        return out

    def _redist_probe(self, names):
        """다음 평가에서 읽을 크레딧 값을 libbless 가 로그에 찍게 만든다.
        (읽기 → 다음 probe 순서라 항상 '한 평가주기 전' 값을 본다 = 정착 시간 확보)"""
        with self._lock:
            socks = [self._tenants[n]["sock"] for n in names
                     if n in self._tenants]
        for sk in socks:
            self._send(sk, "time_stats")

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
            bud = self.budgets()
            for name, b in bud.items():
                with self._lock:
                    sock = self._tenants.get(name, {}).get("sock")
                if sock:
                    self._send(sock, f"time_add {b}")
                    if REDIST_ON:
                        self._granted[name] = self._granted.get(name, 0) + b
            if REDIST_ON and bud and self._redist_probe_due:
                self._redist_probe_due = False
                self._redist_probe(list(bud))
            self._sleep(TICK_S)

    # ---- 상태 조회 (점유 실측) ----
    def sample_occupancy(self, settle_s=0.1):
        """time_stats 스냅샷 → 직전 샘플 대비 charged_us 점유 비율."""
        with self._lock:
            tenants = dict(self._tenants)
        for name, t in tenants.items():
            self._send(t["sock"], "time_stats")
            # [Exp_108 D-1] 게이트 전환 신호도 함께 발화시킨다. libbless 가
            #   BLESS_GATE_METRICS=1 로 떠 있을 때만 gate_* 줄이 로그에 찍힌다.
            self._send(t["sock"], "gate_stats")
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
        # [Exp_108 D-1] 게이트 대기 신호 — 적응형 슬라이싱이 "예산 부족"과
        #   "예산 과다"를 가르는 두 번째 축(사용률만으로는 못 가른다).
        gate = {}
        for name, t in tenants.items():
            g = read_gate_stats(t["log"])
            if g is None:
                continue
            prev = self._last_gate.get(name)
            self._last_gate[name] = g
            if prev:
                gate[name] = g[0] - prev[0]      # 구간 내 전환 횟수 증가분
        return {"charged_delta_us": deltas, "observed_share": share,
                "gate_wait": gate}

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
                "tenants": tenants, "recent_resets": hist,
                # [Exp_121] 재분배 관측 — 무엇을 누구에게 얼마나 줬는지(1-D)
                "redist": {"on": REDIST_ON,
                           "hungry_frac": REDIST_HUNGRY_FRAC,
                           "hold": REDIST_HOLD, "dwell": REDIST_DWELL,
                           "eval_ticks": REDIST_EVAL_TICKS,
                           "last": self._redist_last,
                           "trace": self._redist_trace[-200:]}}
