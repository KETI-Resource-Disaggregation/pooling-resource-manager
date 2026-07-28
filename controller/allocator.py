# allocator.py
# 물리 자원 할당 관리
# - MPS 데몬 시작/중지
# - 테넌트별 MPS % 환경변수 출력 (런처가 이 값으로 프로세스 시작)
# - policy.weights를 virtual_sm 비율 기반으로 재계산

import subprocess
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'shm'))
from prism_shm import MAX_TENANTS


class Allocator:
    def __init__(self, shm):
        self.shm = shm

    # ── MPS 데몬 ──────────────────────────────────────────────────────────────
    def setup_mps(self) -> bool:
        """
        CUDA MPS 데몬 시작.
        반환: 성공 여부
        MPS가 이미 실행 중이면 무시.
        """
        try:
            # 이미 실행 중인지 확인
            r = subprocess.run(
                ["nvidia-cuda-mps-control", "get_server_list"],
                capture_output=True, timeout=3
            )
            if r.returncode == 0:
                return True  # 이미 실행 중
        except FileNotFoundError:
            print("[allocator] nvidia-cuda-mps-control 없음, MPS 비활성")
            return False
        except Exception:
            pass

        try:
            subprocess.run(
                ["nvidia-cuda-mps-control", "-d"],
                check=True, timeout=5
            )
            print("[allocator] MPS 데몬 시작 완료")
            return True
        except Exception as e:
            print(f"[allocator] MPS 데몬 시작 실패: {e}")
            return False

    def stop_mps(self):
        """MPS 데몬 중지 (controller 종료 시)."""
        try:
            subprocess.run(
                ["bash", "-c", "echo quit | nvidia-cuda-mps-control"],
                timeout=5
            )
            print("[allocator] MPS 데몬 중지")
        except Exception:
            pass

    # ── MPS % 계산 ────────────────────────────────────────────────────────────
    def get_mps_pct(self, virtual_sm: int) -> int:
        """CUDA_MPS_ACTIVE_THREAD_PERCENTAGE 값 계산."""
        physical = self.shm.physical_sm_total
        if physical <= 0:
            return 100
        return min(100, int(virtual_sm / physical * 100))

    def get_env_for_tenant(self, tenant_idx: int) -> dict[str, str]:
        """
        테넌트 프로세스 실행 시 설정해야 할 환경변수 반환.
        런처(prism_controller)가 이 dict를 subprocess env에 주입.
        """
        shm  = self.shm
        alloc = shm.alloc[tenant_idx]
        tid   = alloc.tenant_id.decode()
        gid   = "default"  # group_id는 controller가 알고 있음

        return {
            "PRISM_TENANT":               tid,
            "PRISM_GROUP":                gid,
            "CUDA_MPS_ACTIVE_THREAD_PERCENTAGE": str(alloc.mps_pct),
        }

    # ── policy.weights 재계산 ─────────────────────────────────────────────────
    def recompute_weights(self):
        """
        virtual_sm 비율 기반으로 policy.weights를 재계산하고 version bump.
        등록 / 해제가 일어날 때마다 registry가 호출.

        NOTE: registry.register()에서 이미 weight를 직접 설정하므로
        이 메서드는 나중에 일괄 정규화가 필요할 때 사용.
        """
        shm = self.shm
        total_sm = 0.0
        for i in range(MAX_TENANTS):
            if shm.alloc[i].tenant_id != b"":
                total_sm += shm.alloc[i].virtual_sm

        if total_sm <= 0:
            return

        for i in range(MAX_TENANTS):
            if shm.alloc[i].tenant_id != b"":
                shm.policy.weights[i] = shm.alloc[i].virtual_sm / total_sm
            else:
                shm.policy.weights[i] = 0.0

        shm.policy.version += 1

    # ── 슬라이스 계산 (정보 제공용) ─────────────────────────────────────────
    def compute_slice_us(self) -> dict[int, int]:
        """
        각 테넌트의 예상 slice_us 계산 (round_manager와 동일 공식).
        반환: {tenant_idx: slice_us}
        """
        shm = self.shm
        rd  = shm.policy.round_duration_us or 100_000
        total_w = sum(
            shm.policy.weights[i]
            for i in range(MAX_TENANTS)
            if shm.alloc[i].tenant_id != b""
        )
        if total_w <= 0:
            total_w = 1.0

        result = {}
        for i in range(MAX_TENANTS):
            if shm.alloc[i].tenant_id != b"":
                result[i] = int(rd * shm.policy.weights[i] / total_w)
        return result
