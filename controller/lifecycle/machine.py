"""상태머신 + make-before-break 재스폰 오케스트레이션 (Exp_26, G2).

Exp_22 run_pipeline.py(출처 md5 05a44065)의 combined 모드 상태 전이
(:111-197)를 controller 모듈로 이식. 로직 변경 최소화 — 원본 대비 diff 는
Exp_26 report §변경대장 참조 (핵심: 스폰/드레인/게이트를 주입 콜백으로 추상화,
자동 '정책 판단'은 없음 — 트리거는 수동 API 또는 구독 핸들러(Agent 영역 밖)).

상태 전이 (Exp_22 실측 검증 경로):
  NORMAL --trigger--> GATED            (게이트 on + standby 스폰 시작)
  GATED  --standby_ready--> HANDOFF    (구 테넌트 드레인 + 게이트 해제)
  HANDOFF --done--> NORMAL_PRIME       (NORMAL' — 새 구성으로 정상)
  GATED  --standby_fail/timeout--> FALLBACK_GATED  (게이트 유지 + 구 테넌트 유지)
불법 전이는 IllegalTransition 예외 (전이표 밖 요청 거부).
"""
import threading
import time

NORMAL = "NORMAL"
GATED = "GATED"
HANDOFF = "HANDOFF"
NORMAL_PRIME = "NORMAL'"
FALLBACK_GATED = "FALLBACK_GATED"

# 합법 전이표 — Exp_22 이벤트 시퀀스(go→gate_on→…→handoff→normal_prime,
# respawn_failed/timeout→FALLBACK_GATED)에서 유도
LEGAL = {
    NORMAL:         {GATED},
    GATED:          {HANDOFF, FALLBACK_GATED},
    HANDOFF:        {NORMAL_PRIME},
    NORMAL_PRIME:   {GATED},          # 재트리거 허용 (다음 사이클)
    FALLBACK_GATED: {GATED, NORMAL},  # 재시도 또는 수동 복구
}


class IllegalTransition(Exception):
    pass


class StateMachine:
    def __init__(self, audit=None):
        self.state = NORMAL
        self.audit = audit if audit is not None else []
        self._lock = threading.Lock()

    def to(self, new_state, **detail):
        with self._lock:
            if new_state not in LEGAL.get(self.state, set()):
                raise IllegalTransition(f"{self.state} -> {new_state} 불허")
            ev = {"t": round(time.time(), 3), "from": self.state,
                  "to": new_state, **detail}
            self.state = new_state
            self.audit.append(ev)
            return ev


class SwapOrchestrator:
    """make-before-break 재스폰 (Exp_22 combined: respawn_start→step1→handoff).

    인프라 주입 (controller 가 정책·워크로드에 비종속이도록):
      gate_on(old)/gate_off(old) : 시간 게이트 (보통 feeder.arm/set_ratios/release)
      spawn_standby()->handle    : 새 구성 스폰 (BLESS_LIMIT_PCT 는 호출측이
                                   ratio 엔진 decide()로 결정 — audit 에 rule 기록)
      standby_ready(handle)->bool: 이음매 신호 (Exp_22: step1 마커 = 첫 정상 step)
      standby_dead(handle)->bool : 조기 사망 감지 (폴백 트리거)
      drain_old()                : 구 테넌트 드레인 (Exp_22: stop 파일)
    """

    def __init__(self, sm, gate_on, gate_off, spawn_standby, standby_ready,
                 standby_dead, drain_old, ready_timeout_s=60.0):
        # ready_timeout 60s: Exp_22 run_pipeline.py:178 deadline 동일
        self.sm = sm
        self.gate_on = gate_on
        self.gate_off = gate_off
        self.spawn_standby = spawn_standby
        self.standby_ready = standby_ready
        self.standby_dead = standby_dead
        self.drain_old = drain_old
        self.ready_timeout_s = ready_timeout_s
        self.result = None

    def run(self, poll_s=0.05, decision_audit=None):
        """1 사이클 실행. Exp_22 controller() 이식 — 반환: 최종 상태."""
        self.sm.to(GATED, detail="gate_on + standby 스폰",
                   decision=decision_audit)
        self.gate_on()
        handle = self.spawn_standby()
        t_spawn = time.time()
        deadline = t_spawn + self.ready_timeout_s
        while not self.standby_ready(handle):
            if self.standby_dead(handle):
                # 폴백: 게이트 유지 + 구 테넌트 유지 (Exp_22 :181-184)
                self.sm.to(FALLBACK_GATED, reason="standby 조기 종료")
                self.result = {"final": FALLBACK_GATED,
                               "reason": "respawn_failed"}
                return FALLBACK_GATED
            if time.time() > deadline:
                self.sm.to(FALLBACK_GATED, reason="standby ready 타임아웃")
                self.result = {"final": FALLBACK_GATED,
                               "reason": "respawn_timeout"}
                return FALLBACK_GATED
            time.sleep(poll_s)
        t_ready = time.time()
        self.sm.to(HANDOFF, detail="구 테넌트 드레인 + 게이트 해제")
        self.drain_old()
        self.gate_off()
        self.sm.to(NORMAL_PRIME)
        self.result = {"final": NORMAL_PRIME,
                       "standby_spawn_s": round(t_ready - t_spawn, 3)}
        return NORMAL_PRIME
