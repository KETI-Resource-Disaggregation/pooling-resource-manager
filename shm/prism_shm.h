// prism_shm.h
// Shared memory schema - single source of truth for C++ and Python
// Used by: runtime (direct include), controller/monitor (via prism_shm.py ctypes)
//
// 주의: 모든 atomic 접근은 __atomic_* 빌트인으로 명시 수행.
//   C11 _Atomic 키워드는 C++에서 지원 안 되므로 plain type으로 선언.
//   Python ctypes 바인딩과 레이아웃 일치를 위해 구조체 멤버 순서/크기 유지.

#pragma once
#include <stdint.h>
#include <pthread.h>

#define MAX_TENANTS     8
#define SHM_MAGIC       0x50525332   // "PRS2" — 레이아웃 v2 (Exp_41: tenant_id 16→48)
#define SHM_MAGIC_V1    0x5052534D   // "PRSM" — 구 레이아웃 감지·명시적 거부용
#define SHM_LAYOUT_VERSION 2
#define TENANT_ID_LEN   48           // 현 최대 ID 17자("keti-gpu15-lsu178") ×2 이상 (Exp_41 §Phase1)
#define SHM_PATH_FMT    "/prism_%s"  // /dev/shm/prism_{group_id}

// ── Gate state ────────────────────────────────────────────────────────────────
typedef enum {
    GATE_RUNNING       = 0,   // normal execution
    GATE_KILLER_ACTIVE = 1,   // killer op in flight (peers may wait)
    GATE_WAITING       = 2,   // time slice exhausted, waiting for new round
} GateState;

// ── Overcommit mode ───────────────────────────────────────────────────────────
typedef enum {
    MODE_FREE          = 0,   // virtual_total <= physical → no gating
    MODE_OVERCOMMIT    = 1,   // virtual_total >  physical → time-slice active
    MODE_PROFILING     = 2,   // new tenant solo profiling → others hold at killer boundary
} SchedMode;

// ── Per-tenant runtime state (written by tenant process) ─────────────────────
// 모든 필드는 __atomic_* 빌트인으로 접근
typedef struct {
    int64_t  time_remain_us;   // remaining time in current slice
    int32_t  gate_state;       // GateState
    int32_t  round_id;         // last completed round (for sync)
    int32_t  active;           // 1 = running, 0 = idle/exited
    int32_t  _pad0;            // alignment

    // stats (read by monitor)
    int64_t  total_exec_us;
    int64_t  total_wait_us;
    int64_t  killer_count;
    int64_t  mem_used_bytes;
} TenantState;

// ── Per-tenant resource allocation (written by controller) ───────────────────
typedef struct {
    int32_t  virtual_sm;          // requested virtual SMs
    int32_t  virtual_mem_mb;      // requested virtual memory (MB)
    int32_t  mps_pct;             // MPS % set at tenant launch
    float    weight;              // scheduling weight (priority)
    char     tenant_id[TENANT_ID_LEN]; // human-readable ID — 등록 경로는 절단 금지·초과 시 에러 (Exp_41 O-2)
} TenantAlloc;

// ── Scheduler policy (written by central scheduler, read by runtime) ──────────
typedef struct {
    float    weights[MAX_TENANTS];       // time share weights
    int64_t  round_duration_us;          // total round length (default 100ms)
    int32_t  priority[MAX_TENANTS];      // 0=LOW 1=MED 2=HIGH
    uint64_t version;                    // bump on any policy change
} SchedulerPolicy;

// ── Global shared state ───────────────────────────────────────────────────────
typedef struct {
    uint32_t         magic;               // SHM_MAGIC
    int32_t          tenant_count;
    int32_t          physical_sm_total;   // GPU total SMs
    int32_t          layout_version;      // SHM_LAYOUT_VERSION (was _pad1 — 오프셋 불변, Exp_41)
    int64_t          physical_mem_mb;     // GPU total memory (MB)
    int32_t          virtual_sm_total;    // Σ virtual_sm (active tenants) — controller writes
    int32_t          _pad2;               // alignment
    int64_t          virtual_mem_total_mb;
    int32_t          mode;               // SchedMode — __atomic_* 접근

    // profiling mode fields (written by controller)
    int32_t          profiling_tenant_idx;   // tenant being profiled (-1 = none)
    int32_t          profiling_iter_remain;  // iterations left in profiling phase

    // round trigger CAS lock (0=free, 1=in-progress)
    // 여러 테넌트가 동시에 start_new_round를 호출하는 TOCTOU 방지
    int32_t          round_trigger_lock;  // was _pad3

    SchedulerPolicy  policy;             // central scheduler writes
    TenantAlloc      alloc[MAX_TENANTS]; // controller writes at tenant launch
    TenantState      tenants[MAX_TENANTS];

    // hot-reload signal: profiler가 bump → runtime이 다음 라운드에 killer policy 재로드
    // 위치: tenants[8] 종료(+840) → 8-byte align OK
    uint64_t         killer_policy_version;   // +840 (8)

    pthread_mutex_t  policy_mutex;            // +848 protect policy writes
} PrismSharedState;
