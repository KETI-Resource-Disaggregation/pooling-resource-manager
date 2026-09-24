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

# [Exp_141 2부] 몫 재설정의 기본 임대 수명(s) — set_ratios 가 lease_s 를 안 줘도
#   이 만료가 붙는다(무기한 고착 경로 제거).
# ★유도 (임의 상수 아님): 갱신 주체의 실제 주기에서 나온다.
#   - ④ closed_loop  : --interval 0.5s, 개입 중 매 루프 renew_lease (Exp_138 3-D)
#   - 적응형 slice_loop: --interval 1.0s, 조절 중 매 루프 재전송 (Exp_141 보강)
#   최장 갱신 주기 1.0s × 3배 = 3.0s — 일시 지연(관측 파일 stale·HTTP 재시도)을
#   2회까지 흡수하면서, 갱신 주체가 죽은 뒤 고착이 3초를 넘지 않는다.
#   Exp_138 3-D 가 ④ 기준(0.5s×6)으로 잡은 기본값과 같은 수로 수렴한다.
#   명시 opt-out: lease_s<=0 → 무기한(계약 재정규화 등 — 호출자가 책임, 이력에 남김).
RATIO_LEASE_DEFAULT_S = float(os.environ.get("KRAKEN_RATIO_LEASE_S", "3.0"))

# ── [Exp_138 3-D] 몫 재설정 임대(lease) ──────────────────────────────────────
# ★왜 필요한가 (Exp_138 0-B 실측). set_ratios 는 계약값을 남기지 않고 t["ratio"] 를
#   제자리에서 덮어썼다. 원복 경로는 closed_loop 의 finally 블록뿐인데 **SIGKILL 이면
#   실행되지 않는다.** Go 와이어러도 LSU·소켓이 그대로면 재등록하지 않으므로, 안정
#   파드셋에서는 내려간 몫을 아무도 되돌리지 않는다 — kill -9 한 번으로 몫이 0.4 에
#   영구 고정되고 /feeder/status 는 정상으로 보인다(T-5 최악급).
#
# 고침: 재설정에 **만료 시각**을 붙인다. 갱신이 끊기면 feeder 가 스스로 계약값으로
#   되돌린다. 제어기의 정리 코드가 도는지에 의존하지 않는 **구조적** 복귀다.
#
# ★RATIO_LEASE_S 는 새 상수가 아니다 — closed_loop 이 자기 관측을 stale 로 보는
#   기준(--stale 기본 3.0s)을 그대로 가져왔다. "3초 넘게 소식이 없으면 믿지 않는다"는
#   판단을 양쪽이 같은 값으로 쓴다.
RATIO_LEASE_S = float(os.environ.get("KRAKEN_RATIO_LEASE_S", "3.0"))
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


# ── [Exp_135 5부] 제어 적용 여부 탐지 ─────────────────────────────────────────
#   ★소켓이 살아 있고 feeder 에 armed 로 등록돼도 **제어가 걸렸다는 뜻이 아니다.**
#     Exp_135 3부 실측(o4): 워크로드가 `LD_PRELOAD=남의것:libbless.so` 로 뜨고
#     남의 .so 가 libcudart 를 직접 잡아 체인을 끊으면, libbless 는 적재되고
#     init 로그 9줄에 sm_limit 까지 찍히는데 cudaLaunchKernel 후킹은 안 걸린다.
#     처리량이 제어 없음(57.83)과 같은 57.82 로 나오고, 소켓·feeder 지표는 전부 정상이다.
#     갈리는 것은 **게이트를 통과한 커널 수** 하나뿐이다(정상 57,042→105,712 / o4 0→0).
#   기본 꺼짐. 이번 범위는 **드러내기까지**이며 배치를 막지 않는다.
CTLCHECK_ON = os.environ.get("KRAKEN_CTLCHECK", "0") == "1"
CTLCHECK_EVAL_TICKS = int(os.environ.get("KRAKEN_CTLCHECK_TICKS", "200"))


def _ctlcheck_grace_s():
    """유예 = 소켓 등장 뒤 커널을 기다려 주는 시간. **임의 상수가 아니다.**

    근거 둘 (Exp_135 5-C 실측):
      · 소켓 등장(=libbless init, 첫 CUDA 호출) → 첫 커널까지 **0.34~0.88 s**.
        컨텍스트가 생기면 커널은 곧바로 흐른다.
      · 모델 로드는 약 25 s 인데, 소켓은 그 **안에서** 생긴다(로드도 커널을 쓴다).
    기본 30 s = 실측 최대(0.88 s)의 34배이자 모델 로드 시간보다 길다. 컨텍스트를
    만든 뒤 CPU 전처리가 길어지는 워크로드에서도 오탐이 나지 않는 쪽으로 잡았다.
    탐지는 로그만 내므로 늦는 비용은 없고 오탐의 비용만 있다 — 넉넉한 쪽이 맞다.
    """
    try:
        return max(1.0, float(os.environ.get("KRAKEN_CTLCHECK_GRACE_S", "30")))
    except ValueError:
        return 30.0


CTLCHECK_GRACE_S = _ctlcheck_grace_s()


def _ctl_warn(msg):
    print(f"[feeder][제어미적용] {msg}", flush=True)


def _redist_cap_ticks():
    """[Exp_130 1-A] **결정 지평**(잔량 클램프 상한)의 틱 수.

    ★왜 분리하는가 — Exp_129 4-1. `HOLD` 하나가 서로 다른 두 일을 했다:
      (ㄱ) 히스테리시스 — 같은 판정 연속 N회여야 전환 (왕복 방지)
      (ㄴ) 결정 지평   — 잔량 클램프 `cap = per_tick × HOLD × win`
      묶여 있어 HOLD 를 낮추면 단독 재분배는 걸리지만(재현율 30→100%)
      쌍 조건 진동이 113배로 늘고 총처리량이 9.9% 떨어졌다(Exp_129 1-D).
      **둘은 다른 양이다.** 지평만 줄이고 히스테리시스는 유지할 수 있어야 한다.

    ★기본값은 기존 동작 그대로(`HOLD × win` = 125틱 = 1.25초 분)를 유지한다.
      바꾸는 것은 설정으로 하고, 검증 뒤에 기본값 변경을 판단한다(Exp_130 5-A).
    """
    raw = os.environ.get("KRAKEN_REDIST_CAP_TICKS")
    if raw is None:
        return REDIST_HOLD * REDIST_EVAL_TICKS
    try:
        v = int(raw)
    except ValueError:
        v = 0
    if v <= 0:
        # [Exp_107 T-5] 조용한 폴백 금지
        _redist_warn(f"KRAKEN_REDIST_CAP_TICKS={raw!r} 를 해석할 수 없다(양의 정수) → "
                     f"기본값 HOLD×win={REDIST_HOLD * REDIST_EVAL_TICKS} 사용")
        return REDIST_HOLD * REDIST_EVAL_TICKS
    return v




_REDIST_WARNED = set()


def _redist_warn(msg):
    """[Exp_107 T-5] 조용한 폴백 금지 — 같은 사유는 1회만."""
    if msg not in _REDIST_WARNED:
        _REDIST_WARNED.add(msg)
        print(f"[feeder][경고] 재분배: {msg}", flush=True)


# [Exp_130] 결정 지평 확정 — 경고 헬퍼가 정의된 뒤여야 한다(폴백 시 경고).
REDIST_CAP_TICKS = _redist_cap_ticks()

# [Exp_158] 시간축 WC 시제 — 기본 꺼짐. 켜지면 feeder 가 틱마다 armed 테넌트에
#   "다른 armed 테넌트가 직전 틱에 커널을 냈는가"를 peer_idle 0|1 로 push 한다.
#   관측 소스 = 재분배 hungry 판정과 같은 time_stats(kernels 누적) — 새 경로 없음.
TIME_WC_ON = os.environ.get("KRAKEN_TIME_WC", "0") == "1"


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
        # [Exp_135 5부] name -> {t_seen, kernels, applied, warned}
        self._ctl = {}
        self._ctl_eval_t = 0.0
        self._ctl_probe_due = False
        # [Exp_138 3-D → Exp_141] name -> {"expire": 만료 시각, "reason": 사유}
        #   (계약값은 여기 스냅샷하지 않는다 — 진실은 t["contract"] 하나, Exp_141 1부)
        self._ratio_lease = {}
        self._lease_restores = []       # 복귀 이력 최근분 (관측 가능성)
        self._lease_restore_total = 0   # [Exp_141 2부] 복귀 누적 카운터 (조용한 복귀 금지)
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
            # [Exp_141 1부] 계약값을 적용값과 별개 자리에 보관한다. 계약값을
            #   바꿀 수 있는 것은 등록·재등록뿐 — ④·적응형·재분배는 ratio(적용값)만
            #   건드린다. 재기동 복원(Exp_140 RestoreAllocations)도 이 경로로
            #   들어오므로(units→ratioOf 재산정) 복원 시 계약값이 올바로 선다.
            self._tenants[name] = {"sock": sock_path, "log": log_path,
                                   "ratio": float(time_ratio),
                                   "contract": float(time_ratio),
                                   "armed": False,
                                   "resource": str(resource)}
            # 재등록 = 계약 변경. 낡은 임대(이전 계약 스냅샷)를 남기면 만료 시
            #   새 계약이 아니라 옛 값으로 되돌린다 — devID 재사용 오염(Exp_110
            #   계열)과 같은 함정이라 여기서 지운다.
            self._ratio_lease.pop(name, None)

    def _ctl_forget(self, name):
        """[Exp_135] devID 재사용 오염 방지 — 해제 시 판정 상태를 지운다."""
        self._ctl.pop(name, None)

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
            self._ctl_forget(name)
            # [Exp_141 1부] 임대도 함께 소거 — 남기면 같은 devID 를 받은 다음
            #   파드의 몫이 이전 회차의 계약값으로 되돌아간다(재사용 오염).
            self._ratio_lease.pop(name, None)

    def arm(self, name):
        """게이트 무장 (Exp_16: time_mode 1 + time_credit 0)."""
        with self._lock:
            t = self._tenants[name]
            t["armed"] = True
        self._send(t["sock"], "time_mode 1")
        self._send(t["sock"], "time_credit 0")
        # [Exp_146] 급함 등급의 크레딧 대출 한도 = 충전 1주기분.
        #   libbless 코드 기본값과 같은 값을 명시로 내려 둘이 어긋나지 않게 한다
        #   (KRAKEN_CTLCHECK 처럼 코드·배포가 갈리는 상태를 만들지 않는다).
        self._send(t["sock"], "urgent_limit %d" % int(TICK_S * 1e6))

    def release(self, name):
        """게이트 해제 = unlimited (Exp_22 gate_off: time_credit -1)."""
        with self._lock:
            t = self._tenants.get(name)
            if t:
                t["armed"] = False
        if t:
            self._send(t["sock"], "time_credit -1")

    # ---- 목표 변경 (Exp_20 재설정 경로) ----
    def set_ratios(self, ratios, reason="", lease_s=None):
        """time_ratio(적용값) 런타임 변경 — 다음 tick 부터 반영.

        [Exp_138 3-D → Exp_141 2부] 모든 재설정에 만료가 붙는다. 갱신이 끊기면
        feeder 가 스스로 **계약값(등록 시점 몫, t["contract"])** 으로 되돌린다.
        갱신은 같은 값으로 다시 부르면 된다(만료가 밀린다).

          lease_s=None  → 기본 만료 RATIO_LEASE_DEFAULT_S (Exp_141: 무기한 고착
                          경로 제거 — slice_loop 등 lease 미인지 호출자 방어)
          lease_s>0     → 그 값 (④ closed_loop 등 명시 호출자)
          lease_s<=0    → 무기한 opt-out — 계약 재정규화(NPU 정책 B)처럼 재설정
                          자체가 새 계약인 경우만. 이력에 명시돼 남는다
        """
        now = time.time()
        with self._lock:
            for name, r in ratios.items():
                if name not in self._tenants:
                    continue
                if lease_s is not None and float(lease_s) <= 0:
                    # 명시 opt-out — 임대 없음. 기존 임대가 있으면 걷어낸다
                    # (opt-out 재설정이 새 기준이므로 옛 만료가 덮치면 안 된다).
                    self._ratio_lease.pop(name, None)
                else:
                    ttl = float(lease_s) if lease_s else RATIO_LEASE_DEFAULT_S
                    self._ratio_lease[name] = {
                        "expire": now + ttl, "reason": reason}
                self._tenants[name]["ratio"] = float(r)
            self._history.append({"t": round(now, 3),
                                  "ratios": dict(ratios), "reason": reason,
                                  "lease_s": lease_s})

    def release_ratio_lease(self, names=None):
        """임대 해제 — 계약값으로 즉시 복귀. 정상 종료 경로에서 부른다."""
        return self._expire_leases(force=names)

    def _expire_leases(self, force=None):
        """만료된(또는 force 로 지정된) 임대를 계약값으로 되돌린다.

        [Exp_141 1부] 복귀 목적지는 임대 시점 스냅샷이 아니라 **현재 계약값
        t["contract"]** — 재등록으로 계약이 바뀌었어도 항상 최신 계약으로 간다.
        복귀는 즉시·전량이다(단계적 복귀 없음 — 계약이기 때문).
        """
        now = time.time()
        restored = []
        with self._lock:
            for name in list(self._ratio_lease):
                lz = self._ratio_lease[name]
                due = (force is not None and (force is True or name in force)) or \
                      (lz.get("expire") is not None and now >= lz["expire"])
                if not due:
                    continue
                t = self._tenants.get(name)
                if t is not None:
                    was = t["ratio"]
                    contract = t.get("contract", lz.get("contract", was))
                    t["ratio"] = contract
                    restored.append({"t": round(now, 3), "tenant": name,
                                     "from": was, "to": contract,
                                     "reason": lz.get("reason", ""),
                                     "cause": "force" if force else "expired"})
                del self._ratio_lease[name]
            self._lease_restore_total += len(restored)
        for r in restored:
            self._lease_restores.append(r)
            # [T-5] 조용히 되돌리지 않는다 — 계약이 바뀌었다 되돌아온 사실을 남긴다
            print(f"[feeder][Exp_138] 몫 임대 만료 복귀: {r['tenant']} "
                  f"{r['from']} → {r['to']} (사유={r['reason']!r} {r['cause']})",
                  flush=True)
        del self._lease_restores[:-100]     # [Exp_141] 이력 유계 (카운터는 total 로)
        return restored

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
        # [Exp_129 1-B] streak/hungry 를 궤적에 함께 싣는다. slack 만으로는
        #   "왜 전환이 안 됐는지"(streak 리셋)를 볼 수 없다 — 관측 코드는 판정에
        #   쓰이는 상태를 다 내야 한다(T-5 파생).
        want, slack_us, allow_us, dbg = [], {}, {}, {}
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
            # [Exp_130] 결정 지평은 히스테리시스(HOLD)와 **독립**이다
            cap = per_tick * REDIST_CAP_TICKS
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
            cap_us = int(cap)
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
            dbg[n] = {"streak": state["streak"], "hungry": state["hungry"],
                      "cur": bool(cur)}
            if state["hungry"]:
                want.append(n)
        wtot = sum(eff[n] for n in want)
        out = ({n: int(pool * idle * eff[n] / wtot) for n in want}
               if wtot > 0 else {})
        self._redist_extra = out
        snap = {"t": round(now, 3), "idle": round(idle, 4),
                "slack_us": slack_us, "allow_us": allow_us, "state": dbg,
                "cap_ticks": REDIST_CAP_TICKS,
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
            if self._ratio_lease:
                self._expire_leases()
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
            if CTLCHECK_ON and bud:
                self._ctlcheck(list(bud))
            if TIME_WC_ON and len(bud) >= 2:
                self._wc_push(list(bud))
            self._sleep(TICK_S)

    # ---- [Exp_158] WC peer 유휴 신호 ----
    def _wc_push(self, names):
        """armed 테넌트별로 '다른 테넌트 유휴 여부'를 push.
        kernels 누적의 틱 간 delta==0 → 그 테넌트는 이번 틱 유휴로 본다.
        읽기 실패는 '활동 중'으로 취급(편승을 열지 않는 안전 방향 — T-5:
        신호를 조용히 유휴로 바꾸면 계약 밖 통과가 근거 없이 늘어난다)."""
        if not hasattr(self, "_wc_kern"):
            self._wc_kern = {}
        delta = {}
        with self._lock:
            infos = {n: dict(self._tenants.get(n, {})) for n in names}
        for n in names:
            st = read_time_stats(infos[n].get("log", ""))
            if st is None:
                delta[n] = 1          # 못 읽음 = 활동 중 취급(안전 방향)
                continue
            k = st[1]
            delta[n] = k - self._wc_kern.get(n, k)
            self._wc_kern[n] = k
        for n in names:
            others_active = any(delta[m] > 0 for m in names if m != n)
            sock = infos[n].get("sock")
            if sock:
                self._send(sock, f"peer_idle {0 if others_active else 1}")

    # ---- [Exp_135 5부] 제어 적용 여부 판정 ----
    def _ctlcheck(self, names):
        """armed 테넌트의 게이트 통과 커널이 실제로 늘고 있는지 본다.

        판정은 **읽고 나서 다음 프로브**를 낸다(재분배와 같은 순서) — 항상 한
        주기 전 값을 보므로 정착 시간이 확보된다.
        """
        now = time.time()
        if (now - self._ctl_eval_t) < CTLCHECK_EVAL_TICKS * TICK_S:
            return
        self._ctl_eval_t = now
        with self._lock:
            tenants = {n: dict(self._tenants[n]) for n in names
                       if n in self._tenants}
        for n, t in tenants.items():
            st = self._ctl.setdefault(n, {"t_seen": now, "kernels": None,
                                          "applied": None, "warned": False})
            if self._ctl_probe_due:
                cur = read_time_stats(t["log"])
                k = cur[1] if cur else None
                if k is not None:
                    if k > 0:
                        if st["applied"] is not True:
                            st["applied"] = True
                        st["kernels"] = k
                    else:
                        st["kernels"] = 0
                        if (now - st["t_seen"]) > CTLCHECK_GRACE_S:
                            st["applied"] = False
                            if not st["warned"]:
                                st["warned"] = True
                                _ctl_warn(
                                    f"{n}: 소켓·등록은 정상인데 게이트 통과 커널이"
                                    f" {int(now - st['t_seen'])}s 동안 0 이다 —"
                                    " libbless 인터포지션이 안 걸린 것으로 본다."
                                    " LD_PRELOAD 앞에 다른 .so 가 있는지 확인하라"
                                    f" (log={t['log']})")
                elif (now - st["t_seen"]) > CTLCHECK_GRACE_S and not st["warned"]:
                    # [T-5] 못 읽는 것을 '정상'으로 넘기지 않는다
                    st["warned"] = True
                    _ctl_warn(f"{n}: time_stats 를 읽지 못한다 — 판정 보류"
                              f" (log={t['log']})")
        # 다음 평가에서 읽을 값을 만들어 둔다
        self._ctl_probe_due = True
        with self._lock:
            socks = [self._tenants[n]["sock"] for n in names
                     if n in self._tenants]
        for sk in socks:
            self._send(sk, "time_stats")

    def ctlcheck_status(self):
        return {"on": CTLCHECK_ON, "grace_s": CTLCHECK_GRACE_S,
                "eval_ticks": CTLCHECK_EVAL_TICKS,
                "tenants": {n: {"applied": v["applied"], "kernels": v["kernels"],
                                "age_s": round(time.time() - v["t_seen"], 1)}
                            for n, v in self._ctl.items()}}

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
            # [Exp_141 4부] ratio=적용값 · contract=계약값을 나란히 — 고착된
            #   적용값이 계약처럼 보이던 것(Exp_138 0-B)을 표면에서 가른다.
            tenants = {n: {"ratio": t["ratio"], "armed": t["armed"],
                           "contract": t.get("contract", t["ratio"]),
                           "sock": t["sock"],
                           "resource": t.get("resource", "gpu")}
                       for n, t in self._tenants.items()}
            hist = list(self._history[-20:])
        tot = max(1.0, sum(t["ratio"] for t in tenants.values() if t["armed"]))
        for n, t in tenants.items():
            t["target_share"] = round(t["ratio"] / tot, 4) if t["armed"] else None
        with self._lock:
            lease = {n: {"contract": self._tenants.get(n, {}).get("contract"),
                         "expires_in_s": round(v["expire"] - time.time(), 2)
                         if v.get("expire") else None,
                         "reason": v.get("reason", "")}
                     for n, v in self._ratio_lease.items()}
            restores = list(self._lease_restores[-10:])
            restore_total = self._lease_restore_total
        return {"tick_ms": TICK_S * 1000, "env_scale": self._env_scale,
                "ratio_lease": lease, "lease_restores": restores,
                "lease_restore_total": restore_total,
                "lease_default_s": RATIO_LEASE_DEFAULT_S,
                "env_policy": self._env_policy,
                "tenants": tenants, "recent_resets": hist,
                # [Exp_121] 재분배 관측 — 무엇을 누구에게 얼마나 줬는지(1-D)
                "redist": {"on": REDIST_ON,
                           "hungry_frac": REDIST_HUNGRY_FRAC,
                           "hold": REDIST_HOLD, "dwell": REDIST_DWELL,
                           "eval_ticks": REDIST_EVAL_TICKS,
                           "cap_ticks": REDIST_CAP_TICKS,
                           "last": self._redist_last,
                           "trace": self._redist_trace[-200:]},
                # [Exp_135 5부] 제어가 실제로 걸렸는지
                "ctlcheck": self.ctlcheck_status()}
