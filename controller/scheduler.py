# scheduler.py
# 중앙 스케줄러 — policy writer only
# critical path 없음: 각 테넌트가 shm을 직접 읽어 자율 동작
# 이 클래스는 policy 갱신 + stats 조회만 담당

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'shm'))
from prism_shm import MAX_TENANTS, open_shm


class Scheduler:
    def __init__(self, shm):
        self.shm = shm

    # ── Policy 갱신 ───────────────────────────────────────────────────────────
    def set_weights(self, weights: list[float]):
        """테넌트 시간 가중치 설정. 합산이 1.0이 아니어도 round_manager가 정규화."""
        shm = self.shm
        for i, w in enumerate(weights[:MAX_TENANTS]):
            shm.policy.weights[i] = w
        shm.policy.version += 1

    def set_weight(self, tenant_idx: int, weight: float):
        self.shm.policy.weights[tenant_idx] = weight
        self.shm.policy.version += 1

    def set_priority(self, tenant_idx: int, priority: int):
        """priority: 0=LOW, 1=MED, 2=HIGH

        [Exp_27 deprecated] 이 shm 필드는 runtime 소비처가 없고(Exp_23 §2.2),
        등급 어휘도 libbless(0=URGENT,1=NORMAL,2=BACKGROUND)와 상충한다.
        정본 경로는 POST /priority (controller/priority.py — 소켓 발신,
        libbless priority_set). 기존 호환을 위해 write 는 유지만 한다.
        """
        self.shm.policy.priority[tenant_idx] = priority
        self.shm.policy.version += 1

    def set_round_duration(self, duration_us: int):
        """라운드 길이 설정 (마이크로초). 기본 100ms = 100_000."""
        self.shm.policy.round_duration_us = duration_us
        self.shm.policy.version += 1

    def set_equal_weights(self):
        """모든 활성 테넌트에 동일 가중치 적용."""
        shm = self.shm
        active = [i for i in range(MAX_TENANTS) if shm.alloc[i].tenant_id != b""]
        if not active:
            return
        w = 1.0 / len(active)
        for i in active:
            shm.policy.weights[i] = w
        for i in range(MAX_TENANTS):
            if i not in active:
                shm.policy.weights[i] = 0.0
        shm.policy.version += 1

    # ── Stats 조회 ────────────────────────────────────────────────────────────
    def get_stats(self) -> list[dict]:
        shm = self.shm
        stats = []
        for i in range(MAX_TENANTS):
            if shm.alloc[i].tenant_id == b"":
                continue
            t = shm.tenants[i]
            stats.append({
                "idx":          i,
                "tenant_id":    shm.alloc[i].tenant_id.decode(),
                "active":       bool(t.active),
                "gate_state":   t.gate_state,
                "round_id":     t.round_id,
                "time_remain_us": t.time_remain_us,
                "exec_us":      t.total_exec_us,
                "wait_us":      t.total_wait_us,
                "killer_count": t.killer_count,
                "mem_mb":       t.mem_used_bytes // (1024 * 1024),
                "weight":       shm.policy.weights[i],
            })
        return stats

    def get_mode(self) -> str:
        m = self.shm.mode
        return {0: "FREE", 1: "OVERCOMMIT", 2: "PROFILING"}.get(m, f"UNKNOWN({m})")

    def get_policy_version(self) -> int:
        return self.shm.policy.version
