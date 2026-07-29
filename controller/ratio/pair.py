"""페어 봉투 정책 — 이종 페어 완화 규약 opt-in (Exp_40, Exp_39b 근거).

배경 (Exp_39b 실측): 현행 완전배분 규약(Σ(s×t)=1 → 페어에서 s=1, t=r 유일해)은
이종 페어(COMPUTE×MEMORY)에서 양쪽 시간 게이트를 강제해 OC 0.722 로 순차 실행보다
손해. 공격자(COMPUTE)만 게이트하고 피해자(MEMORY)의 시간 게이트를 해제하면
OC 1.089 (+0.367 = 이득의 사실상 전부, C−U). 공간 절단은 성능 이득이 아니라
(P−C = −0.030) 상한 강제 수단.

정책 3종 (기본값 strict — opt-in 아니면 기존 동작 무변경):
  strict         : 현행 규약 그대로 — 각 테넌트 decide(r, cls, free_time=r)
                   (Exp_38 페어 규약. 완전 배분에서 s=1, t=r 유일해)
  relaxed_hetero : 이종 페어 한정 — COMPUTE: s=1.0·t=r(게이트), MEMORY: s=1.0·
                   t=1.0(게이트 해제 = Σt>1 허용). Exp_39b C 팔.
  capped_hetero  : relaxed + MEMORY 에 공간 상한 s=r 부여(초과 점유 구조적 차단,
                   OC 비용 −0.03). MPS 필수 (O-3: 무-MPS 에서 s 는 무음 no-op).
                   Exp_39b P 팔.

가드 (전부 strict/relaxed 폴백 + 사유 기록 — 무리한 적용 금지):
  · 2-tenant 가 아니면 → strict
  · 분류가 {COMPUTE, MEMORY} 로 갈리지 않으면(동종/UNCERTAIN 포함) → strict
  · confidence 가 HIGH 가 아닌 테넌트가 있으면 → strict (Exp_37 오분류 회피)
  · capped_hetero 인데 mps_running 이 아니면 → relaxed_hetero 로 폴백 (O-3)

일반화 경계 (Exp_40 §제약): 2-tenant 이종 페어 한정 v1. 실측 검증은
prefill×decode 1조합 (Exp_39b). 3+ tenant 는 정의하지 않고 strict 폴백.
"""

from .engine import (CLASS_COMPUTE, CLASS_MEMORY, EPS, bless_limit_pct,
                     decide)
from .pair_predict import predict_pair

POLICIES = ("strict", "relaxed_hetero", "capped_hetero")


def _strict_member(req):
    """현행 페어 규약 1인분 — Exp_38 run_pair38.py 의 decide 호출 그대로."""
    d = decide(req["r"], req["workload_class"], free_sm_ratio=1.0,
               free_time_ratio=req["r"],
               confidence=req.get("confidence", "HIGH"))
    if not d["feasible"]:
        return None
    return {"space_ratio": d["space_ratio"], "time_ratio": d["time_ratio"],
            "limit_pct": bless_limit_pct(d["space_ratio"]),
            "gate": d["time_ratio"] < 1.0 - EPS,
            "rule_applied": d["rule_applied"]}


def decide_pair(requests, policy="strict", mps_running=False, self_pair_oc=None):
    """2-tenant 페어의 (s, t, gate) 일괄 결정.

    입력:
      requests    [{name, r, workload_class, confidence?, device_fill?}, ...]
                  r = 계약 몫 (Σr ≤ 1 — 완전/부분 배분)
                  device_fill = 분류 v2 공간 지표 (선택 — Exp_48/49 페어링 예측용)
      policy      POLICIES 중 하나 (기본 strict = 기존 동작)
      mps_running 노드 MPS 데몬 상태 (capped_hetero 전제조건, O-3)
      self_pair_oc M측 자기페어 실측 OC (선택 — 공간 경합 앵커, pair_predict 참조)

    반환 dict:
      feasible, applied_policy(실제 적용된 정책), requested_policy,
      fallback_reason(폴백 시), warnings[], members{name: {space_ratio,
      time_ratio, limit_pct, gate, rule_applied}},
      pairing_prediction — ★보조 출력(Exp_49): 예상 조합 특성·OC 앵커·근거.
        본 함수의 s/t/gate 결정에 일절 관여하지 않는다 (Exp_39b 결론 보호 —
        이득 원천은 봉투 완화이며 예측은 배치·정책 선택의 입력일 뿐).
        지표 부재 시 available=False (v1 단독 동작 = 기존 무변경).
    """
    out = {"feasible": False, "requested_policy": policy,
           "applied_policy": None, "fallback_reason": None,
           "warnings": [], "members": {},
           "pairing_prediction": predict_pair(requests,
                                              self_pair_oc=self_pair_oc)}
    if policy not in POLICIES:
        out["fallback_reason"] = f"알 수 없는 정책 {policy!r} — {POLICIES} 중 하나"
        return out

    rs = [float(q["r"]) for q in requests]
    if any(r <= 0 for r in rs) or sum(rs) > 1.0 + EPS:
        out["fallback_reason"] = (f"계약 몫 위반 — r 들={rs}, Σr={sum(rs):.4f}"
                                  f" (0<r, Σr≤1 필요)")
        return out

    applied = policy
    reason = None
    if policy != "strict":
        classes = sorted(q["workload_class"] for q in requests)
        confs = [q.get("confidence", "HIGH") for q in requests]
        if len(requests) != 2:
            applied, reason = "strict", (f"{len(requests)}-tenant — "
                                         f"완화 규약은 2-tenant 한정 (v1)")
        elif classes != sorted([CLASS_COMPUTE, CLASS_MEMORY]):
            applied, reason = "strict", (f"이종 페어 아님 (classes={classes})"
                                         " — 동종/UNCERTAIN 은 strict")
        elif any(c != "HIGH" for c in confs):
            applied, reason = "strict", (f"분류 신뢰도 부족 (conf={confs})"
                                         " — Exp_37 오분류 회피 가드")
        elif policy == "capped_hetero" and not mps_running:
            applied = "relaxed_hetero"
            reason = ("MPS 미기동 — s 상한은 무음 no-op 이 되므로 (O-3) "
                      "relaxed_hetero 로 폴백")
            out["warnings"].append("capped_hetero 요청이나 MPS 부재 — "
                                   "공간 상한 미적용")

    out["applied_policy"] = applied
    out["fallback_reason"] = reason

    if applied == "strict":
        for q in requests:
            m = _strict_member(q)
            if m is None:
                out["fallback_reason"] = (f"{q['name']}: strict decide "
                                          f"INFEASIBLE (r={q['r']})")
                return out
            out["members"][q["name"]] = m
        out["feasible"] = True
        return out

    # relaxed_hetero / capped_hetero — 이종 2-tenant 확정 상태
    for q in requests:
        if q["workload_class"] == CLASS_COMPUTE:
            # 공격자: 시간 게이트가 곧 격리 (Exp_39b C: prefill 0.424 ≈ 계약)
            out["members"][q["name"]] = {
                "space_ratio": 1.0, "time_ratio": round(float(q["r"]), 6),
                "limit_pct": bless_limit_pct(1.0), "gate": True,
                "rule_applied": "완화 규약: COMPUTE=공격자 → t=r 게이트 유지"}
        else:
            s = 1.0 if applied == "relaxed_hetero" else round(float(q["r"]), 6)
            rule = ("완화 규약: MEMORY=피해자 → 시간 게이트 해제 "
                    "(Σt>1, Exp_39b C−U=+0.367)")
            if applied == "capped_hetero":
                rule = ("완화+상한: MEMORY → 게이트 해제 + s=r 공간 상한 "
                        "(계약 초과 차단, 비용 −0.03 — Exp_39b P)")
            out["members"][q["name"]] = {
                "space_ratio": s, "time_ratio": 1.0,
                "limit_pct": bless_limit_pct(s), "gate": False,
                "rule_applied": rule}
    out["feasible"] = True
    return out
