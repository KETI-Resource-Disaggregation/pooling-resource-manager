"""비율 결정 엔진 v1 — LSU 요청 → (space_ratio, time_ratio) 분해 (Exp_25, G1).

역할: required_ratio r(= 요청 LSU / 디바이스 LSU capacity)과 외부 분류
결과를 받아 s×t ≥ r 을 만족하는 (space_ratio, time_ratio)를 규칙 기반으로
결정한다. v1 = 규칙 + residual 클리핑까지. v2(성능모델 스코어링)·v3(bandit)는
Exp_25 report §4 설계 메모 참조 — 여기 구현하지 않음.

원칙 (수치 출처는 각 상수 주석 — 임의 상수 금지):
  · 분류는 항상 외부 입력. 엔진이 트레이스에서 재유도하지 않는다 —
    b≥4 에서 decode 가 COMPUTE 로 반전(Exp_17 §3)하므로 분류 시점의
    재분류 책임은 kernel_class/phase_online 쪽에 있다.
  · 비대칭 축 (Exp_21): space 는 스폰 시점에만 결정 가능(CUDA exec affinity 는
    런타임 재설정 불가 — Exp_20 §4경로 규명), time 은 런타임 조정 가능
    (time_add 소켓). 결과 구조체의 space_axis/time_axis 필드로 구분.
  · 적용 시 BLESS_LIMIT_PCT 를 항상 명시 설정 (bless_limit_pct() 헬퍼) —
    미설정이면 libbless.cpp:468 의 암묵 50% 캡으로 떨어진다 (Exp_23 §2.3).
"""

import math

# 후보 이산화 격자 — v1 잠정값 (Exp_25 스펙 §참고: SM 12개 단위 등
# 디바이스 정합 이산화는 v2 논점). 0.1 단위 10개.
GRID = [round(0.1 * k, 1) for k in range(1, 11)]

# 부동소수 경계 비교 여유 (0.1 격자 산술 오차 흡수용 — 물리 의미 없음)
EPS = 1e-9

# 축 선호 근거:
#   COMPUTE_BOUND → 큰 s: Exp_9 §2 — compute 워크로드 처리량이 SM 비율에
#     선형 (SM 여유가 곧 처리량). Exp_15 §2-3: prefill(COMPUTE) 우대 70/30 이
#     50/50 대비 +1.7%p (OC 1.222 vs 1.205).
#   MEMORY_BOUND → 작은 s: Exp_9 §2 — memory-bound 는 SM 증가 이득 체감
#     (대역폭 병목). 남는 SM 을 compute 파트너에 양보하는 방향.
#   MIXED/UNKNOWN/저신뢰 → 중립: √r (s=t 균형점 — s×t=r 곡선상 두 축을
#     같은 비율로 쓰는 점. 선호 근거가 없을 때의 무편향 기본값).
CLASS_COMPUTE = "COMPUTE_BOUND"
CLASS_MEMORY = "MEMORY_BOUND"

# 런치 실링 (Exp_7 §3): 절대 grant rate 가 ~180 launch/s 에 근접하면 시간
# 분할이 0.5 로 포화. v1 은 검사하지 않음 — 자리만 예약 (스펙 §작업2).
LAUNCH_CEILING_PER_S = 180   # Exp_7 실측 근사치. v1 미사용 (warnings 자리)

# BLESS_LIMIT_PCT 적용 규칙:
#   libbless.cpp:469 의 getenv 가드는 0<v<100 만 유효 — 100 은 배제되어
#   암묵 50% 로 폴백한다 (Exp_23 §2.3 코드 확인). full 은 99 로 매핑
#   (Exp_21 실험 관례: run_gen.py min(pct,99)).
BLESS_PCT_MAX = 99
BLESS_PCT_MIN = 1


def decide(required_ratio, workload_class, free_sm_ratio=1.0,
           free_time_ratio=1.0, provisional=False, confidence="HIGH"):
    """(space_ratio, time_ratio) 결정 — v1 규칙.

    입력:
      required_ratio   r = 요청 LSU / 디바이스 LSU capacity (0 < r ≤ 1)
      workload_class   kernel_class 산출 분류 (외부 입력 — 재유도 금지)
      free_sm_ratio    호출자가 전달하는 residual (공간 축)
      free_time_ratio  호출자가 전달하는 residual (시간 축)
      provisional      catalog measured=false 디바이스면 True (전파)
      confidence       분류 신뢰도 ("LOW" 면 중립 선호로 강등)
    반환 dict (설명 가능 구조 — 결정 근거가 로그로 남음):
      feasible, space_ratio, time_ratio, effective_ratio(=s×t),
      space_axis("spawn-time"), time_axis("runtime-adjustable"),
      candidates_evaluated, rule_applied, provisional, reason,
      warnings{launch_ceiling: None}   # Exp_7 자리 예약, v1 미검사
    """
    base = {
        "space_ratio": None, "time_ratio": None, "effective_ratio": None,
        # 비대칭 축 (Exp_21): space=스폰 파라미터(재스폰 필요), time=런타임 조정
        "space_axis": "spawn-time (BLESS_LIMIT_PCT — 런타임 재설정 불가, Exp_20/21)",
        "time_axis": "runtime-adjustable (time_add 소켓, Exp_16/18)",
        "candidates_evaluated": 0, "rule_applied": None,
        "provisional": bool(provisional), "reason": None,
        "warnings": {"launch_ceiling": None},   # Exp_7 ~180 launch/s — v1 미검사
    }

    r = float(required_ratio)
    if not (0.0 < r <= 1.0 + EPS):
        base.update(feasible=False,
                    reason=f"required_ratio={r} 범위 밖 (0<r≤1) — "
                           f"단일 디바이스 capacity 초과 요청")
        return base
    r = min(r, 1.0)

    # 1) 후보 이산화 + residual 클리핑: s ≥ r (→ t=r/s ≤ 1), s ≤ free_sm,
    #    t ≤ free_time 동시 만족 후보만 생존
    cands = []
    for s in GRID:
        if s + EPS < r or s > free_sm_ratio + EPS:
            continue
        t = r / s
        if t > 1.0 + EPS or t > free_time_ratio + EPS:
            continue
        # 6자리 '올림' — 반올림 내림이 생기면 s×t < r 로 불변식이 깨진다
        # (test_09 곡선 수학이 회귀 감시)
        t_q = min(1.0, math.ceil(t * 1e6) / 1e6)
        cands.append((s, t_q))
    base["candidates_evaluated"] = len(cands)

    if not cands:
        base.update(feasible=False,
                    reason=f"생존 후보 0개 — r={r}, free_sm={free_sm_ratio}, "
                           f"free_time={free_time_ratio} 제약에서 s×t≥r 불가")
        return base

    # 2) 축 선호
    neutral = confidence == "LOW" or workload_class not in (CLASS_COMPUTE,
                                                            CLASS_MEMORY)
    if neutral:
        target = math.sqrt(r)
        best_d = min(abs(s - target) for s, _ in cands)
        pref = [c for c in cands if abs(c[0] - target) <= best_d + EPS]
        rule = (f"중립 √r={target:.3f} 근방 (class={workload_class}, "
                f"conf={confidence})")
    elif workload_class == CLASS_COMPUTE:
        s_max = max(s for s, _ in cands)
        pref = [c for c in cands if abs(c[0] - s_max) <= EPS]
        rule = "COMPUTE→큰 s (Exp_9 선형, Exp_15 70/30 우대)"
    else:
        s_min = min(s for s, _ in cands)
        pref = [c for c in cands if abs(c[0] - s_min) <= EPS]
        rule = "MEMORY→작은 s (Exp_9 이득 체감)"

    # 3) 동률 시 best-fit: 배치 후 잔여 (free_sm−s)×(free_time−t) 최소
    #    (온라인 bin-packing best-fit — 잔여 직사각형이 작을수록 다음 요청
    #    수용에 유리한 조밀 배치)
    if len(pref) > 1:
        pref.sort(key=lambda c: (max(0.0, free_sm_ratio - c[0])
                                 * max(0.0, free_time_ratio - c[1])))
        rule += " + best-fit 잔여 최소"
    s, t = pref[0]

    # residual 클리핑이 선호를 눌렀는지 기록 (설명 가능성)
    clipped = []
    if workload_class == CLASS_COMPUTE and s < 1.0 - EPS:
        clipped.append(f"free_sm={free_sm_ratio}")
    if t + EPS >= free_time_ratio and free_time_ratio < 1.0:
        clipped.append(f"free_time={free_time_ratio}")

    base.update(feasible=True, space_ratio=s, time_ratio=t,
                effective_ratio=round(s * t, 6),
                rule_applied=rule + (f" (클리핑: {', '.join(clipped)})"
                                     if clipped else ""),
                reason=f"r={r} → s={s}, t={t} (s×t={s*t:.3f} ≥ r)")
    return base


def decide_for_device(lsu_request, device_name, catalog, workload_class,
                      free_sm_ratio=1.0, free_time_ratio=1.0,
                      confidence="HIGH"):
    """catalog.json 엔트리에서 r 과 provisional 을 유도해 decide() 호출.

    catalog: lsu/catalog.json 파싱 dict. measured=false 디바이스(예: Warboy)는
    provisional=True 를 결과에 전파 (스펙 §작업2 입력 규약).
    """
    dev = catalog["devices"].get(device_name)
    if dev is None:
        return {**decide(0, workload_class), "feasible": False,
                "reason": f"catalog 에 없는 디바이스: {device_name!r} — "
                          f"default_lsu 추정은 v1 범위 밖 (임의값 금지)"}
    capacity = dev["lsu"]
    return decide(lsu_request / capacity, workload_class,
                  free_sm_ratio, free_time_ratio,
                  provisional=not dev.get("measured", False),
                  confidence=confidence)


def bless_limit_pct(space_ratio):
    """space_ratio → BLESS_LIMIT_PCT 값 (항상 명시 설정할 것).

    미설정 시 libbless.cpp:468 암묵 50% 캡 (Exp_23 §2.3).
    100 은 getenv 가드(0<v<100)에서 배제 → full 은 99 (Exp_21 관례).
    """
    pct = int(round(space_ratio * 100))
    return max(BLESS_PCT_MIN, min(BLESS_PCT_MAX, pct))
