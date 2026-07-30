"""페어링 예측 — 분류 v2 공간 지표 기반 조합 특성·OC 앵커 (Exp_48/49).

★용도 한계 (Exp_39b 결론 보호): 이 예측은 **비율 결정자가 아니다**. 이득의 원천은
봉투 완화(decide_pair 정책)이며, 본 모듈은 봉투 정책 선택·배치 판단에 쓰는
**보조 입력**(조합 특성 카테고리 + 참고 앵커)만 낸다.

규칙 (Exp_56 개정 — 곱(wall_fill) 단일 축):
  공간 경합(hetero_space_contended) ⇔ M측 **wall_fill = device_fill ×
  kernel_density ≥ WALL_FILL_HI** — "벽시계 전체에서 SM 을 실제로 붙잡고 있는
  비율". 미만이면 이종 상보 — 공간이 비거나(fill 낮음) 시간이 비거나(launch-bound)
  파트너가 들어갈 틈이 있다.

  ★Exp_49 의 2D AND(fill>0.60 AND density≥0.5)에서 개정한 이유(실측):
  Exp_56 이 빈 구간에 새 케이스 4종을 넣자 AND 규칙이 2건 오판했다 —
  densenet121(fill 0.695·dens 0.509 로 둘 다 임계 초과 → 경합 예측이나 실측
  OC 1.326 = 상보), convnext_tiny(fill 0.584 로 미달 → 상보 예측이나 실측
  1.082 = 경합). 두 축을 각각 임계로 자르면 "곱은 작은데 둘 다 아슬하게 넘는"
  경우와 그 반대를 구분하지 못한다. 곱 기준으로 바꾸면 전 9케이스 중 오판이
  2건 → **1건**(convnext_tiny)으로 줄고 서열이 거의 단조가 된다.

  ★실측 근거 (9케이스 = Exp_49 5 + Exp_56 4, 전부 bert 파트너 MPS 50/50 ×3회):
    wall_fill 오름차순 → OC: 0.016→1.591(gpt2m), 0.180→1.546(regnet_y_400mf),
    0.230→1.462(mobilenet), 0.354→1.326(densenet121), **0.366→1.082(convnext_tiny
    — 유일한 이상치)**, 0.398→1.212(decode), 0.415→1.089(resnet50 학습),
    0.560→1.092(wide_resnet50_2), 0.593→1.059(vgg16).
  임계 도출: 이상치 convnext_tiny 를 제외하면 상보군 최대 0.398(decode)와
    경합군 최소 0.415(resnet50 학습) 사이가 비어 있고, 그 **중간점 ≈0.41** 을
    채택. 실측 극값 사이에서만 잡았고 케이스 맞춤 조정은 없다.
  ★convnext_tiny 오판 — Exp_58 에서 원인 규명·해소: 오판의 근원은 파트너 간섭이
    아니라 **자기 유지율**(SM 절반에서 혼자서도 ret50 0.716 — 같은 wall_fill 대의
    densenet121 은 1.003). MEMORY 상반(fill≈1.0)과 COMPUTE 하반(fill≈0.17)의
    이중구조가 평균 fill 을 희석해 wall_fill 이 이 손실을 못 본다. convnext_small/
    base 페어 실측(1.085/1.079)으로 계열 전체 경합 확인 → CM_MIXED 보수 가드로
    반영(아래 상수 주석). 대역폭 가설은 부분 근거만(convnext×decode 상호 간섭
    0.476 최대) — CUPTI Metrics 고비용 경로는 불채택(가드로 실용 해소).

앵커 (Exp_15/17 실측 — ★카테고리 대표값일 뿐 수치 이전성은 조합 의존):
  이종 상보 1.205 는 하한 성격 — 신규 조합 실측 1.462~1.591 로 상회 가능
  (Exp_17 "이득 크기는 조합 의존" 재확인). 신뢰 산출물은 category, expected_oc 는 참고.
지표 부재 시: available=False — 호출측은 v1 단독 동작(기존 무변경, 기본값).
"""

from .engine import CLASS_COMPUTE, CLASS_MEMORY

# 앵커 (실측 출처 고정)
ANCHOR_HETERO = 1.205            # Exp_15 이종 (참고 하한 — 조합 의존, 상회 실측 존재)
ANCHOR_HOMO_C = 0.945            # Exp_15 동종-C
ANCHOR_HOMO_M = 1.152            # Exp_15 동종-M
ANCHOR_CONTENDED_FLOOR = 0.945   # 공간 경합 보수 하한 (실측 1.059~1.089 를 하회 — 안전)

# 임계 (Exp_56 실측 — 근거는 모듈 docstring). 9케이스 중 오판 1건.
WALL_FILL_HI = 0.41              # 경합 판정: fill × density (벽시계 SM 점유율)
# MIXED 경계 보수 가드 (Exp_58): cm_ratio 가 이 창에 있으면서 wall_fill 이
# 임계 미만이면 경합으로 보수 취급. 근거 실측 3건 — convnext_tiny/small/base
# (cm 0.491/0.542/0.597, wall_fill 0.366/0.341/0.320 전부 임계 미만인데 실측 OC
# 1.082/1.085/1.079 전부 경합). 기전: MEMORY 상반(LayerNorm/GELU, fill≈1.0)과
# COMPUTE 하반(smem 무거운 GEMM, fill≈0.17)의 이중구조가 평균 fill 을 희석해
# wall_fill 이 자기 유지율 손실(ret50 0.716~0.726)을 담지 못한다 (Exp_58 §1).
# 창 도출: 하단 0.45 = 상보-정답 최대 cm(mobilenet 0.414)과 오판 최소
# cm(convnext_tiny 0.491)의 중간점. 상단 0.66 = 오판 최대 cm(convnext_base
# 0.597)과 그 위 최근접 케이스(decode b=2, 0.727)의 중간점 — 단 상단 이웃은
# 페어 미실측이라 잠정. 정답 케이스 12건 중 창 안은 0건 → 기존 판정 무영향.
CM_MIXED_LO = 0.45
CM_MIXED_HI = 0.66
# 하위호환 참고값 (Exp_49 2D AND — 개정 전 규칙, 문서/테스트 대조용)
FILL_HI = 0.60
DENSITY_MIN = 0.5


def predict_pair(requests, self_pair_oc=None):
    """2-tenant 조합 특성 예측.

    requests: [{name, workload_class, confidence?, device_fill?, kernel_density?}, ...]
      device_fill / kernel_density — 분류 v2 지표 (fill 부재 시 available=False)
      density 부재 시: 보수 취급(경합 가정 — OC 과소추정 방향이 배치 안전)
    self_pair_oc — M측 자기페어 실측 OC (있으면 contended 앵커로 사용 — 비순환)

    반환: {available, category, expected_oc, basis, fallback_reason}
    """
    out = {"available": False, "category": None, "expected_oc": None,
           "basis": None, "fallback_reason": None}
    if len(requests) != 2:
        out["fallback_reason"] = f"{len(requests)}-tenant — 예측은 2-tenant 한정"
        return out
    classes = sorted(q["workload_class"] for q in requests)
    if any(q.get("confidence", "HIGH") != "HIGH" for q in requests):
        out["fallback_reason"] = "분류 신뢰도 부족 — 예측 보류 (Exp_37 가드 계승)"
        return out

    if classes == [CLASS_COMPUTE, CLASS_COMPUTE]:
        out.update(available=True, category="homo_c", expected_oc=ANCHOR_HOMO_C,
                   basis="동종 C×C — Exp_15 앵커 0.945")
        return out
    if classes == [CLASS_MEMORY, CLASS_MEMORY]:
        out.update(available=True, category="homo_m", expected_oc=ANCHOR_HOMO_M,
                   basis="동종 M×M — Exp_15 앵커 1.152")
        return out
    if classes != sorted([CLASS_COMPUTE, CLASS_MEMORY]):
        out["fallback_reason"] = f"UNCERTAIN 포함 조합 (classes={classes}) — 예측 보류"
        return out

    mem = next(q for q in requests if q["workload_class"] == CLASS_MEMORY)
    fill = mem.get("device_fill")
    if fill is None:
        out["fallback_reason"] = ("M측 device_fill 부재 — v1 단독 동작 "
                                  "(하위호환 기본값)")
        return out
    try:
        fill = float(fill)
    except (TypeError, ValueError):
        out["fallback_reason"] = f"device_fill 형식 오류: {fill!r}"
        return out
    if not (0.0 <= fill <= 1.0):
        out["fallback_reason"] = f"device_fill 범위 밖: {fill}"
        return out

    density = mem.get("kernel_density")
    density_note = ""
    if density is None:
        density = 1.0        # 보수 취급: 밀도 미지 → 상한 가정 (경합 쪽 = OC 과소추정)
        density_note = " [density 부재 — 보수 취급]"
    else:
        density = float(density)
        if not (0.0 <= density <= 1.0):
            out["fallback_reason"] = f"kernel_density 범위 밖: {density}"
            return out

    wall_fill = fill * density
    contended = wall_fill >= WALL_FILL_HI

    # MIXED 경계 보수 가드 (Exp_58) — cm_ratio 는 선택 입력(부재 시 기존 동작).
    if not contended:
        cm = mem.get("cm_ratio")
        if cm is not None:
            try:
                cm = float(cm)
            except (TypeError, ValueError):
                cm = None
        if cm is not None and CM_MIXED_LO <= cm <= CM_MIXED_HI:
            anchor = (float(self_pair_oc) if self_pair_oc is not None
                      else ANCHOR_CONTENDED_FLOOR)
            out.update(
                available=True, category="hetero_space_contended",
                expected_oc=anchor,
                basis=f"이종 + M측 wall_fill {wall_fill:.3f} < {WALL_FILL_HI} "
                      f"이나 cm_ratio {cm:.3f} ∈ [{CM_MIXED_LO}, {CM_MIXED_HI}] "
                      f"MIXED 경계 — 보수 경합 취급 (Exp_58: convnext 계열 3건 "
                      f"실측 전부 경합 1.079~1.085, 이중구조가 wall_fill 을 "
                      f"희석){density_note}")
            return out

    if contended:
        anchor = (float(self_pair_oc) if self_pair_oc is not None
                  else ANCHOR_CONTENDED_FLOOR)
        src = ("M측 자기페어 실측" if self_pair_oc is not None
               else "보수 하한(동종 앵커 최소)")
        out.update(available=True, category="hetero_space_contended",
                   expected_oc=anchor,
                   basis=f"이종이나 M측 wall_fill {wall_fill:.3f}"
                         f"(={fill:.3f}×{density:.3f}) ≥ {WALL_FILL_HI} — 벽시계 "
                         f"SM 점유(Exp_56 곱 규칙), 앵커={src}{density_note}")
        return out
    why = ("공간 여유" if fill <= 0.6 else "launch-bound(시간 여유)")
    out.update(available=True, category="hetero_complementary",
               expected_oc=ANCHOR_HETERO,
               basis=f"이종 + M측 wall_fill {wall_fill:.3f}"
                     f"(={fill:.3f}×{density:.3f}) < {WALL_FILL_HI} — {why}로 "
                     f"파트너 진입 여지 (Exp_56 곱 규칙), 앵커=Exp_15 참고 하한"
                     f"{density_note}")
    return out
