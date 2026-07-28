# prism_shm.py
# Python ctypes binding for prism_shm.h
# Used by: controller, monitor
#
# 레이아웃은 prism_shm.h와 정확히 일치해야 함.
# ctypes.Structure.from_buffer() → mmap 위에 직접 매핑되므로
# 필드 순서/크기/패딩을 C 컴파일러 결과와 동일하게 유지.

import ctypes
import mmap
import os
import subprocess

MAX_TENANTS = 8
SHM_MAGIC   = 0x50525332  # "PRS2" — 레이아웃 v2 (Exp_41)
SHM_MAGIC_V1 = 0x5052534D  # "PRSM" — 구 레이아웃 감지용
SHM_LAYOUT_VERSION = 2
TENANT_ID_LEN = 48

# GateState
GATE_RUNNING       = 0
GATE_KILLER_ACTIVE = 1
GATE_WAITING       = 2

# SchedMode
MODE_FREE       = 0
MODE_OVERCOMMIT = 1
MODE_PROFILING  = 2

# ── C 구조체 레이아웃 ──────────────────────────────────────────────────────────
# TenantState: 56 bytes
class TenantState(ctypes.Structure):
    _fields_ = [
        ("time_remain_us",  ctypes.c_int64),   # +0  (8)
        ("gate_state",      ctypes.c_int32),   # +8  (4)
        ("round_id",        ctypes.c_int32),   # +12 (4)
        ("active",          ctypes.c_int32),   # +16 (4)
        ("_pad0",           ctypes.c_int32),   # +20 (4)
        ("total_exec_us",   ctypes.c_int64),   # +24 (8)
        ("total_wait_us",   ctypes.c_int64),   # +32 (8)
        ("killer_count",    ctypes.c_int64),   # +40 (8)
        ("mem_used_bytes",  ctypes.c_int64),   # +48 (8)
    ]                                          # total: 56


# TenantAlloc: 64 bytes (v2 — Exp_41 tenant_id 16→48)
class TenantAlloc(ctypes.Structure):
    _fields_ = [
        ("virtual_sm",      ctypes.c_int32),   # +0  (4)
        ("virtual_mem_mb",  ctypes.c_int32),   # +4  (4)
        ("mps_pct",         ctypes.c_int32),   # +8  (4)
        ("weight",          ctypes.c_float),   # +12 (4)
        ("tenant_id",       ctypes.c_char * TENANT_ID_LEN), # +16 (48)
    ]                                          # total: 64


# SchedulerPolicy: 80 bytes
class SchedulerPolicy(ctypes.Structure):
    _fields_ = [
        ("weights",           ctypes.c_float * MAX_TENANTS),  # +0  (32)
        ("round_duration_us", ctypes.c_int64),                # +32 (8)
        ("priority",          ctypes.c_int32 * MAX_TENANTS),  # +40 (32)
        ("version",           ctypes.c_uint64),               # +72 (8)
    ]                                                         # total: 80


# PrismSharedState
# pthread_mutex_t on Linux x86_64 = 40 bytes
class PrismSharedState(ctypes.Structure):
    _fields_ = [
        ("magic",                 ctypes.c_uint32),         # +0   (4)
        ("tenant_count",          ctypes.c_int32),          # +4   (4)
        ("physical_sm_total",     ctypes.c_int32),          # +8   (4)
        ("layout_version",        ctypes.c_int32),          # +12  (4) v2 (was _pad1)
        ("physical_mem_mb",       ctypes.c_int64),          # +16  (8)
        ("virtual_sm_total",      ctypes.c_int32),          # +24  (4)
        ("_pad2",                 ctypes.c_int32),          # +28  (4)
        ("virtual_mem_total_mb",  ctypes.c_int64),          # +32  (8)
        ("mode",                  ctypes.c_int32),          # +40  (4)
        ("profiling_tenant_idx",  ctypes.c_int32),          # +44  (4)
        ("profiling_iter_remain", ctypes.c_int32),          # +48  (4)
        ("round_trigger_lock",    ctypes.c_int32),          # +52  (4)  CAS lock
        ("policy",                SchedulerPolicy),         # +56  (80)
        ("alloc",                 TenantAlloc * MAX_TENANTS),  # +136 (512, v2)
        ("tenants",               TenantState * MAX_TENANTS),  # +648 (448) → ends at +1096
        ("killer_policy_version", ctypes.c_uint64),          # +1096 (8)
        ("policy_mutex",          ctypes.c_byte * 40),       # +1104 (40)
    ]                                                        # total: 1144 (v2)


# ── SHM 유틸 ──────────────────────────────────────────────────────────────────

def _shm_path(group_id: str) -> str:
    """POSIX shm → Linux에서 /dev/shm/prism_{group_id}"""
    return f"/dev/shm/prism_{group_id}"


def create_shm(group_id: str,
               physical_sm: int,
               physical_mem_mb: int) -> tuple["PrismSharedState", mmap.mmap]:
    """
    SHM 생성 및 초기화 (controller 전용).
    반환: (PrismSharedState, mmap 객체)
    mmap을 닫으면 매핑이 해제되므로 호출자가 생존 동안 보관해야 함.
    """
    path = _shm_path(group_id)
    size = ctypes.sizeof(PrismSharedState)

    fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_TRUNC, 0o666)
    os.ftruncate(fd, size)
    mm = mmap.mmap(fd, size)
    os.close(fd)

    shm = PrismSharedState.from_buffer(mm)
    # 초기화
    shm.magic              = SHM_MAGIC
    shm.layout_version     = SHM_LAYOUT_VERSION
    shm.tenant_count       = 0
    shm.physical_sm_total  = physical_sm
    shm.physical_mem_mb    = physical_mem_mb
    shm.virtual_sm_total   = 0
    shm.virtual_mem_total_mb = 0
    shm.mode               = MODE_FREE
    shm.profiling_tenant_idx  = -1
    shm.profiling_iter_remain = 0
    shm.round_trigger_lock = 0
    shm.killer_policy_version = 0
    shm.policy.round_duration_us = 100_000   # 기본 100ms
    shm.policy.version = 0
    for i in range(MAX_TENANTS):
        shm.policy.weights[i] = 1.0
        shm.policy.priority[i] = 0

    # pthread_mutex 초기화
    _pthread_mutex_init(shm)

    return shm, mm


def _pthread_mutex_init(shm: "PrismSharedState") -> None:
    """policy_mutex를 pthread_mutex_init으로 초기화."""
    try:
        libpthread = ctypes.CDLL("libpthread.so.0", use_errno=True)
        mutex_addr = ctypes.addressof(shm.policy_mutex)
        libpthread.pthread_mutex_init(ctypes.c_void_p(mutex_addr), None)
    except Exception:
        pass  # pthread 없는 환경(macOS 등)에서는 무시


def open_shm(group_id: str) -> tuple["PrismSharedState", mmap.mmap]:
    """
    기존 SHM에 읽기/쓰기 연결 (controller 재시작, schedctl, monitor 용).
    반환: (PrismSharedState, mmap 객체)
    """
    path = _shm_path(group_id)
    size = ctypes.sizeof(PrismSharedState)
    fd = os.open(path, os.O_RDWR)
    mm = mmap.mmap(fd, size)
    os.close(fd)
    shm = PrismSharedState.from_buffer(mm)
    if shm.magic != SHM_MAGIC:
        found = shm.magic
        del shm          # from_buffer 참조 해제 후 mmap 닫기
        mm.close()
        if found == SHM_MAGIC_V1:
            raise RuntimeError(
                f"SHM {path} 는 구 레이아웃(v1, magic=PRSM) — 새 코드는 v2 "
                f"(tenant_id 48B, Exp_41) 필요. 구 SHM 제거/재생성 후 재시도. "
                f"조용한 오독 방지를 위해 연결 거부.")
        raise RuntimeError(
            f"SHM {path} magic 불일치: 0x{found:08X} (기대 0x{SHM_MAGIC:08X})")
    return shm, mm


def get_gpu_info() -> tuple[int, int]:
    """
    nvidia-smi로 GPU SM 수와 총 메모리(MB) 조회.
    반환: (sm_count, mem_mb)
    실패 시 (0, 0)
    """
    try:
        # SM 수: nvidia-smi --query-gpu=compute_cap 에서 CC 읽은 뒤 알려진 매핑 사용
        # 더 직접적인 방법: nvidia-smi --query-gpu=multiprocessorcount (지원 버전 한정)
        result = subprocess.run(
            ["nvidia-smi", "--query-gpu=name,memory.total",
             "--format=csv,noheader,nounits"],
            capture_output=True, text=True, timeout=5
        )
        if result.returncode != 0:
            return 0, 0

        line = result.stdout.strip().split("\n")[0]
        parts = [p.strip() for p in line.split(",")]
        mem_mb = int(parts[1]) if len(parts) >= 2 else 0

        # SM 수는 pynvml 또는 CUDA 런타임으로 정확히 알 수 있으나,
        # 여기서는 nvidia-smi deviceQuery 결과를 파싱 시도 후 폴백
        sm_count = _get_sm_count()
        return sm_count, mem_mb

    except Exception:
        return 0, 0


def _get_sm_count() -> int:
    """GPU SM(Streaming Multiprocessor) 수 조회."""
    # 1순위: torch.cuda (가장 정확)
    try:
        import torch
        if torch.cuda.is_available():
            return torch.cuda.get_device_properties(0).multi_processor_count
    except Exception:
        pass

    # 2순위: pynvml — nvmlDeviceGetNumGpuCores는 CUDA 코어 수이므로 사용 불가
    #   대신 compute capability 기반 SM 수 추정 테이블 사용
    try:
        import pynvml
        pynvml.nvmlInit()
        handle = pynvml.nvmlDeviceGetHandleByIndex(0)
        major, minor = pynvml.nvmlDeviceGetCudaComputeCapability(handle)
        pynvml.nvmlShutdown()
        # CC별 대표 SM 수 (보수적 하한값 사용)
        cc_sm_table = {
            (7, 0): 80,   # V100
            (7, 5): 68,   # T4
            (8, 0): 108,  # A100
            (8, 6): 84,   # A6000/3090
            (8, 9): 76,   # L40/4090
            (9, 0): 132,  # H100
        }
        return cc_sm_table.get((major, minor), 80)
    except Exception:
        pass

    return 80  # 폴백
