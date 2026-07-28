# registry.py
# 테넌트 등록 / 해제 및 virtual resource 총량 관리

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'shm'))
from prism_shm import (MAX_TENANTS, MODE_FREE, MODE_OVERCOMMIT,
                       MODE_PROFILING, TENANT_ID_LEN)


class Registry:
    def __init__(self, shm):
        self.shm = shm

    # ── 테넌트 등록 ───────────────────────────────────────────────────────────
    def register(self, tenant_id: str,
                 virtual_sm: int,
                 virtual_mem_mb: int,
                 weight: float = 1.0) -> int:
        """
        빈 슬롯에 테넌트 등록 후 슬롯 인덱스 반환.
        동일 tenant_id 슬롯이 있으면 재사용 (crash 후 재시작 지원).
        """
        shm = self.shm

        encoded = tenant_id.encode()
        if len(encoded) >= TENANT_ID_LEN:
            # 조용한 절단 금지 (Exp_41 O-2): 초과는 명시적 거부
            raise ValueError(
                f"tenant_id {tenant_id!r} 길이 {len(encoded)}B ≥ 필드 폭 "
                f"{TENANT_ID_LEN}B — 등록 거부 (절단 금지, Exp_41)")
        # 1. 동일 tenant_id 슬롯 우선 탐색
        idx = -1
        for i in range(MAX_TENANTS):
            if shm.alloc[i].tenant_id == encoded:
                idx = i
                break

        # 2. 완전히 빈 슬롯 탐색
        if idx < 0:
            for i in range(MAX_TENANTS):
                if not shm.tenants[i].active and shm.alloc[i].tenant_id == b"":
                    idx = i
                    break

        if idx < 0:
            raise RuntimeError(f"슬롯 부족: MAX_TENANTS={MAX_TENANTS}")

        # TenantAlloc 기록
        shm.alloc[idx].virtual_sm     = virtual_sm
        shm.alloc[idx].virtual_mem_mb = virtual_mem_mb
        shm.alloc[idx].weight         = weight
        shm.alloc[idx].mps_pct        = self._calc_mps_pct(virtual_sm)
        shm.alloc[idx].tenant_id      = encoded

        # 전체 합산 업데이트
        shm.virtual_sm_total      += virtual_sm
        shm.virtual_mem_total_mb  += virtual_mem_mb

        # tenant_count: 활성 슬롯 최대 인덱스+1
        shm.tenant_count = max(shm.tenant_count, idx + 1)

        # policy weight 동기화
        shm.policy.weights[idx] = weight
        shm.policy.version += 1

        self._update_mode()
        return idx

    # ── 테넌트 해제 ───────────────────────────────────────────────────────────
    def deregister(self, tenant_idx: int):
        """테넌트 슬롯 해제, virtual 총량 감소."""
        shm = self.shm
        i   = tenant_idx

        shm.virtual_sm_total     -= shm.alloc[i].virtual_sm
        shm.virtual_mem_total_mb -= shm.alloc[i].virtual_mem_mb

        shm.alloc[i].virtual_sm     = 0
        shm.alloc[i].virtual_mem_mb = 0
        shm.alloc[i].mps_pct        = 0
        shm.alloc[i].weight         = 0.0
        shm.alloc[i].tenant_id      = b""

        shm.tenants[i].active = 0
        shm.policy.weights[i] = 0.0
        shm.policy.version   += 1

        # tenant_count 재계산
        count = 0
        for j in range(MAX_TENANTS):
            if shm.alloc[j].tenant_id != b"":
                count = j + 1
        shm.tenant_count = count

        self._update_mode()

    # ── 오버커밋 모드 업데이트 ─────────────────────────────────────────────────
    def _update_mode(self):
        shm = self.shm
        # PROFILING 모드 중에는 모드 변경 보류
        if shm.mode == MODE_PROFILING:
            return
        if shm.virtual_sm_total > shm.physical_sm_total:
            shm.mode = MODE_OVERCOMMIT
        else:
            shm.mode = MODE_FREE

    # ── MPS % 계산 ────────────────────────────────────────────────────────────
    def _calc_mps_pct(self, virtual_sm: int) -> int:
        physical = self.shm.physical_sm_total
        if physical <= 0:
            return 100
        return min(100, int(virtual_sm / physical * 100))

    # ── tenant_id로 슬롯 탐색 ────────────────────────────────────────────────
    def find_slot_by_tenant_id(self, tenant_id: str) -> int:
        """
        tenant_id 문자열로 alloc 슬롯 인덱스 반환.
        찾지 못하면 -1 반환.
        Device Plugin의 /deregister_by_id 호출 경로에서 사용.
        """
        encoded = tenant_id.encode()
        if len(encoded) >= TENANT_ID_LEN:
            return -1   # 필드 폭 초과 ID 는 존재할 수 없음 (등록이 거부되므로)
        for i in range(MAX_TENANTS):
            if self.shm.alloc[i].tenant_id == encoded:
                return i
        return -1

    # ── 현재 등록 목록 ────────────────────────────────────────────────────────
    def list_tenants(self) -> list[dict]:
        shm = self.shm
        result = []
        for i in range(MAX_TENANTS):
            if shm.alloc[i].tenant_id == b"":
                continue
            result.append({
                "idx":          i,
                "tenant_id":    shm.alloc[i].tenant_id.decode(),
                "virtual_sm":   shm.alloc[i].virtual_sm,
                "virtual_mem_mb": shm.alloc[i].virtual_mem_mb,
                "mps_pct":      shm.alloc[i].mps_pct,
                "weight":       shm.alloc[i].weight,
                "active":       bool(shm.tenants[i].active),
            })
        return result
