# overcommit.py
# 오버커밋 모드 전환 및 idle 테넌트 처리
#
# FREE  ↔  OVERCOMMIT 전환 규칙:
#   virtual_sm_total  >  physical_sm_total  →  OVERCOMMIT (시간 분할 개입)
#   virtual_sm_total  ≤  physical_sm_total  →  FREE       (게이팅 없음)
#
# Idle 처리 (Work-conserving):
#   GATE_WAITING 상태가 N초 이상 지속되는 테넌트는 virtual_sm_total에서 제외
#   → 나머지 테넌트가 남은 자원을 더 오래 쓸 수 있음

import time
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'shm'))
from prism_shm import MAX_TENANTS, MODE_FREE, MODE_OVERCOMMIT, MODE_PROFILING, GATE_WAITING


class OvercommitManager:
    IDLE_THRESHOLD_S = 2.0   # 이 시간 이상 WAITING이면 idle로 간주

    def __init__(self, shm):
        self.shm = shm
        self._idle_since: dict[int, float] = {}   # tenant_idx → 첫 WAITING 시점

    # ── 모드 갱신 (외부에서 주기적으로 호출) ──────────────────────────────────
    def check_and_update(self) -> int:
        """
        현재 virtual_sm_total vs physical 비교 후 mode 갱신.
        PROFILING 중에는 개입하지 않음.
        반환: 새 mode 값
        """
        shm = self.shm
        if shm.mode == MODE_PROFILING:
            return shm.mode

        new_mode = (MODE_OVERCOMMIT
                    if shm.virtual_sm_total > shm.physical_sm_total
                    else MODE_FREE)
        shm.mode = new_mode
        return new_mode

    # ── Idle 감지 (Work-conserving) ───────────────────────────────────────────
    def tick_idle_check(self):
        """
        주기적으로 호출. GATE_WAITING 상태가 장시간 지속되는 테넌트를
        active=0으로 전환 → virtual_sm_total 재계산.
        """
        shm = self.shm
        now = time.monotonic()

        for i in range(MAX_TENANTS):
            if shm.alloc[i].tenant_id == b"":
                continue

            state = shm.tenants[i].gate_state

            if state == GATE_WAITING:
                if i not in self._idle_since:
                    self._idle_since[i] = now
                elif now - self._idle_since[i] >= self.IDLE_THRESHOLD_S:
                    self._on_tenant_idle(i)
            else:
                if i in self._idle_since:
                    # 다시 활성화됨
                    del self._idle_since[i]
                    self._on_tenant_active(i)

    # ── Idle / Active 전환 ────────────────────────────────────────────────────
    def _on_tenant_idle(self, tenant_idx: int):
        """
        테넌트가 idle 상태로 진입.
        virtual_sm_total에서 해당 몫 제외 → 오버커밋 재평가.
        """
        shm = self.shm
        i   = tenant_idx

        if not shm.tenants[i].active:
            return  # 이미 비활성

        shm.tenants[i].active = 0
        shm.virtual_sm_total     -= shm.alloc[i].virtual_sm
        shm.virtual_mem_total_mb -= shm.alloc[i].virtual_mem_mb
        self.check_and_update()

    def _on_tenant_active(self, tenant_idx: int):
        """
        idle이었던 테넌트가 다시 커널 제출을 시작.
        virtual_sm_total에 복귀 → 오버커밋 재평가.
        """
        shm = self.shm
        i   = tenant_idx

        if shm.tenants[i].active:
            return  # 이미 활성

        shm.tenants[i].active = 1
        shm.virtual_sm_total     += shm.alloc[i].virtual_sm
        shm.virtual_mem_total_mb += shm.alloc[i].virtual_mem_mb
        self.check_and_update()

    # ── Profiling 모드 진입/탈출 ──────────────────────────────────────────────
    def enter_profiling(self, tenant_idx: int, iter_count: int = 5):
        """신규 테넌트 solo 프로파일링 시작. 기존 테넌트는 killer 경계에서 hold."""
        shm = self.shm
        shm.profiling_tenant_idx  = tenant_idx
        shm.profiling_iter_remain = iter_count
        shm.mode = MODE_PROFILING

    def exit_profiling(self):
        """프로파일링 완료. 이전 모드로 복귀."""
        shm = self.shm
        shm.profiling_tenant_idx  = -1
        shm.profiling_iter_remain = 0
        self.check_and_update()   # FREE or OVERCOMMIT으로 복귀
