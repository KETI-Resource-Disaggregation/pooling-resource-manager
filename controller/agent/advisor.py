#!/usr/bin/env python3
"""[Exp_145] AI Agent — 관측 · 해석 · 권고 (K20, 2차년도 범위)

**몫·배율·모드를 바꾸지 않는다.** 관측(/metrics·/capacity) → 규칙 판정 → 근거 문장까지다.
적용 경로는 구조만 두고(Applier, 기본 꺼짐) 이번에는 호출하지 않는다.

설계 선 (Exp_145 2-C):
  · 판정 = 규칙   — 같은 입력에 같은 답이 나와야 시험·시연에서 쓸 수 있고 근거를 댈 수 있다
  · 문장 = LLM    — 생성 실패해도 권고는 템플릿으로 나온다(폴백)

입력은 이미 있는 것만 쓴다 — 새 관측 경로를 만들지 않는다(Exp_140 /metrics 11계열, /capacity).

usage:
  advisor.py --goal throughput            # 1회 판정 후 종료
  advisor.py --goal latency --watch       # 주기 판정(기본 15s — Prometheus 스크레이프 주기 승계)
  advisor.py --goal placement --llm off   # 템플릿만
"""
import argparse
import json
import os
import subprocess
import sys
import time
import urllib.request

# ── 판정 임계: 전부 실측 출처가 있다. 임의 상수 금지(Exp_145 2-B) ──────────
TH = {
    # 간섭 구역 임계 — /capacity interference_hint 와 같은 값(Exp_80)
    "zone_transition": 1.00,
    "zone_contended": 1.40,
    # 배율 1.35 라이브 기준선(Exp_142 §1): 활용률 76.2%, 개별 유지율 62.6%, p95 1.77x
    "util_live_pct": 76.2,
    "retention_live": 0.626,
    "p95_ratio_live": 1.77,
    # ④ 개입 효과(Exp_89·92): 유지율 0.477 → 0.743
    "fb_retention_off": 0.477,
    "fb_retention_on": 0.743,
    # Colocation 이득(Exp_39b) — ★구 230 하네스. 232 재측정 전(Exp_139 5-C)
    "coloc_gain_2": (0.72, 1.09),
    # 소형 요청 LSU 하한(Exp_142 §4): 배율 1.35에서 30u 실패 추정, 45u 안전
    "lsu_min_safe": 45,
    "lsu_min_fail_est": 30,
}

# 조절 축 → 적용 등급 (Exp_145 0부, 코드 확인분)
AXES = {
    "time_share":  ("즉시",   "/feeder/ratios — 임대 기반(Exp_141)"),
    "priority":    ("즉시",   "HTTP /priority — 시간 게이트 미연계(C-3)"),
    "feedback":    ("즉시",   "④ closed_loop 프로세스 기동·종료"),
    "gate_layout": ("즉시",   "class 선언 → relaxedDecision 2초 폴링(Exp_138 0-A)"),
    "overcommit":  ("재기동", "카탈로그 + DS 재기동(Exp_142·143)"),
    "mps_space":   ("재기동", "스폰 시 고정 — 런타임 재설정 REJECT(Exp_139 K10)"),
    "redist":      ("재기동", "KRAKEN_REDIST env, 모듈 로드 시 1회 읽기"),
    "placement":   ("외부",   "스케줄러 소관 — 우리가 적용할 수 없다"),
}

CTRL = os.environ.get("KRAKEN_CTRL", "http://127.0.0.1:8090")
# 수집 주기 = Prometheus 전역 기본 스크레이프 주기 15s 승계(Exp_140 2-E 스크레이프 예시).
# 임의 상수가 아니다 — 같은 소스를 같은 주기로 본다.
POLL_S = float(os.environ.get("KRAKEN_AGENT_POLL_S", "15"))


class ObserveError(Exception):
    """수집 실패 — 판정을 멈춘다. 옛 상태로 권고하지 않는다(Exp_145 1부)."""


def _get(path, parse):
    try:
        with urllib.request.urlopen(CTRL + path, timeout=5) as r:
            if r.status != 200:
                raise ObserveError("%s HTTP %s" % (path, r.status))
            return parse(r.read().decode())
    except ObserveError:
        raise
    except Exception as e:
        raise ObserveError("%s 수집 실패: %r" % (path, e))


def parse_metrics(text):
    """Prometheus text → {계열: [(labels, value)]}. 형식 오류는 수집 실패로 올린다."""
    out = {}
    for ln in text.splitlines():
        ln = ln.strip()
        if not ln or ln.startswith("#"):
            continue
        try:
            head, val = ln.rsplit(" ", 1)
            name, _, lbl = head.partition("{")
            labels = {}
            for part in lbl.rstrip("}").split(","):
                if "=" in part:
                    k, _, v = part.partition("=")
                    labels[k.strip()] = v.strip().strip('"')
            out.setdefault(name, []).append((labels, float(val)))
        except Exception as e:
            raise ObserveError("metrics 형식 오류: %r (%s)" % (e, ln[:60]))
    if not out:
        raise ObserveError("metrics 응답이 비어 있다")
    return out


def observe():
    """현재 상태. 값이 없는 항목은 None — 0으로 채우지 않는다(Exp_140·141 규칙)."""
    m = _get("/metrics", parse_metrics)
    cap = _get("/capacity", json.loads)
    dev = cap["devices"][0]
    c = dev["capacity"]

    def one(name):
        v = m.get(name)
        return v[0][1] if v else None

    slices = []
    for s in dev.get("slices", []):
        slices.append({
            "tenant": s["tenant"],
            "contract": s.get("contract_time_ratio"),      # Exp_141
            "applied": s.get("time_ratio"),
            "mem_mb": s.get("mem_mb"),
            "armed": s.get("armed"),
            "lsu_req": round((s.get("time_ratio") or 0) * c["physical_lsu"]),
        })
    # 지켜줄 작업 선언 — watcher 선기록 class 파일(Exp_110 R-1)이 /metrics 라벨로 온다
    cls = {}
    for labels, _ in m.get("kraken_slice_compute_pct", []):
        if labels.get("class"):
            cls[labels["tenant"]] = labels["class"]
    for s in slices:
        s["class"] = cls.get(s["tenant"])                  # 선언 없으면 None

    st = {
        "ts": round(time.time(), 3),
        "node": cap["node"],
        "physical_lsu": c["physical_lsu"],
        "advertised_lsu": c["advertised_lsu"],
        "overcommit_factor": c["overcommit_factor"],
        "allocation_ratio": c["allocation_ratio"],
        "interference_zone": one("kraken_node_interference_zone"),
        "npu_cores_available": one("kraken_node_npu_cores_available"),
        "slices": slices,
        "n_slices": len(slices),
        "declared": {s["tenant"]: s["class"] for s in slices if s["class"]},
        "has_victim": any(s["class"] == "memory" for s in slices),
        "has_aggressor": any(s["class"] == "compute" for s in slices),
        # 활용률은 측정값이라 관측으로 얻을 수 없다 — 지어내지 않는다(Exp_142 분모 규약)
        "utilization_pct": None,
    }
    return st


# ── 2부 판정 (규칙) ─────────────────────────────────────────────────────────
STRATEGIES = {
    "contract":   "계약 준수",
    "throughput": "총량 우선",
    "latency":    "지연 보호",
    "placement":  "배치 권고",
}


def decide(st, goal):
    """상태 × 목표 → (전략, 근거 리스트, 조치 리스트). 근거 없으면 계약 준수."""
    ev, act = [], []
    ar = st["allocation_ratio"]
    zone = st["interference_zone"]
    hetero = st["has_victim"] and st["has_aggressor"]

    if st["n_slices"] == 0:
        ev.append(("배치 없음", "슬라이스 0개 — 조절할 대상이 없다"))
        return "contract", ev, act

    if goal == "placement":
        # 우리가 적용할 수 없는 축 — 권고 자체가 산출물(Exp_145 전략 4)
        if hetero:
            ev.append(("이종 조합 성립",
                       "연산·대역폭 선언이 함께 있음 → Colocation 이득 구간 "
                       "(0.72→1.09, Exp_39b · ★구 230 하네스, 232 재측정 전)"))
            act.append(("이 조합을 같은 노드에 유지 권고", AXES["placement"]))
        else:
            ev.append(("동종 조합",
                       "같은 성격끼리 묶여 있음 → 이종 조합으로 섞으면 이득 구간에 들어간다"))
            act.append(("이종 작업과 섞어 배치 권고", AXES["placement"]))
        small = [s for s in st["slices"]
                 if s["lsu_req"] and s["lsu_req"] < TH["lsu_min_safe"]]
        if small:
            ev.append(("소형 요청 존재",
                       "요청 %s LSU — 배율 %.2f에서 %du 미만은 메모리 몫 부족으로 "
                       "적재 실패 위험(30u 실패 실측, 45u 안전 — Exp_142 §4)"
                       % ([s["lsu_req"] for s in small], st["overcommit_factor"],
                          TH["lsu_min_safe"])))
            act.append(("소형 요청의 LSU 하한을 %du로 안내" % TH["lsu_min_safe"],
                        AXES["placement"]))
        return "placement", ev, act

    if zone is not None and zone >= 2:
        ev.append(("간섭 구역 2",
                   "총 할당률 %.3f ≥ %.2f — 경합 구간(Exp_80 임계)"
                   % (ar, TH["zone_contended"])))
    elif zone == 1:
        ev.append(("간섭 구역 1",
                   "총 할당률 %.3f — 전이 구간(%.2f~%.2f, Exp_80)"
                   % (ar, TH["zone_transition"], TH["zone_contended"])))
    else:
        ev.append(("간섭 구역 0",
                   "총 할당률 %.3f < %.2f — 경합 없음" % (ar, TH["zone_transition"])))

    if goal == "latency":
        if st["has_victim"]:
            ev.append(("지켜줄 작업 선언 있음",
                       "class=memory 선언: %s"
                       % [t for t, c in st["declared"].items() if c == "memory"]))
            ev.append(("④ 개입 효과 실측",
                       "유지율 %.3f → %.3f (Exp_89·92, Σ149 중량 victim)"
                       % (TH["fb_retention_off"], TH["fb_retention_on"])))
            act.append(("④ Feedback 가동 — 지켜줄 작업의 지연을 보고 aggressor를 조인다",
                        AXES["feedback"]))
            act.append(("지켜줄 작업에 우선순위 상향", AXES["priority"]))
            if hetero:
                act.append(("이종 조합이므로 게이트 배치는 그대로 두면 victim이 풀린다"
                            " (relaxedDecision 자동)", AXES["gate_layout"]))
            return "latency", ev, act
        ev.append(("지켜줄 작업 선언 없음",
                   "class=memory 선언이 없어 ④가 보호 대상을 정할 수 없다 "
                   "(라벨 폴백은 하네스 obs 경로 — K18 소관)"))
        return "contract", ev, act

    if goal == "throughput":
        if hetero:
            ev.append(("이종 조합 성립",
                       "연산·대역폭이 함께 있음 — 게이트 완화 구간 "
                       "(2개 조합 %.2f→%.2f, Exp_39b · ★구 230 하네스)"
                       % TH["coloc_gain_2"]))
            act.append(("게이트 완화 유지 — relaxedDecision이 memory 쪽을 푼다(자동)",
                        AXES["gate_layout"]))
            act.append(("④ Feedback 끔 — 총량 목표에서는 개입이 총처리를 깎는다",
                        AXES["feedback"]))
            return "throughput", ev, act
        if zone is not None and zone >= 2:
            ev.append(("동종 + 경합 구간",
                       "게이트를 풀 이종 근거가 없다. 배율 %.2f에서 개별 유지율 "
                       "%.1f%%·p95 %.2f×가 이미 대가(Exp_142 §4)"
                       % (st["overcommit_factor"], TH["retention_live"] * 100,
                          TH["p95_ratio_live"])))
            return "contract", ev, act
        ev.append(("동종 조합·경합 전 구간",
                   "총 할당률 %.3f (구역 %s) — 완화할 이종 근거가 없다. "
                   "총량 우선의 유일한 런타임 수단이 게이트 완화인데 그것은 "
                   "이종 선언에서만 성립한다(Exp_138 0-A relaxedDecision)"
                   % (ar, "없음" if zone is None else int(zone))))
        return "contract", ev, act

    if goal == "fair":
        drift = [s for s in st["slices"]
                 if s["contract"] is not None and s["applied"] is not None
                 and abs(s["applied"] - s["contract"]) > 1e-6]
        if drift:
            ev.append(("계약값·적용값 불일치",
                       "%s — 적용값이 계약과 다르다(④·적응형 개입 중, 임대 만료 시 "
                       "자동 복귀 — Exp_141)"
                       % [(s["tenant"], s["applied"], s["contract"]) for s in drift]))
            act.append(("개입 해제 시 계약값 복귀 확인 — 임대가 이미 보장한다",
                        AXES["time_share"]))
        else:
            ev.append(("계약 이행 중", "모든 슬라이스의 적용값 = 계약값"))
        return "contract", ev, act

    return "contract", ev, act


# ── 3부 근거 문장 ───────────────────────────────────────────────────────────
def render_template(st, strat, ev, act):
    """LLM 없이도 나오는 권고 — 이것이 정본이고 LLM은 다듬기만 한다."""
    lines = [
        "[권고] %s (목표 기준 판정)" % STRATEGIES[strat],
        "  상태: 슬라이스 %d개 · 총 할당률 %.3f · 간섭 구역 %s · 배율 %.2f(광고 %d LSU)"
        % (st["n_slices"], st["allocation_ratio"],
           "없음" if st["interference_zone"] is None else int(st["interference_zone"]),
           st["overcommit_factor"], st["advertised_lsu"]),
    ]
    for k, v in ev:
        lines.append("  근거: %s — %s" % (k, v))
    if act:
        for a, (grade, how) in act:
            lines.append("  조치: %s [적용 등급: %s · %s]" % (a, grade, how))
    else:
        lines.append("  조치: 없음 — 현 상태 유지가 계약 준수다")
    return "\n".join(lines)


def render_llm(text, model_dir, threads, cores):
    """문장만 LLM. 판정은 넘기지 않는다. 실패하면 None → 템플릿이 정본으로 남는다."""
    script = os.path.join(os.path.dirname(os.path.abspath(__file__)), "llm_phrase.py")
    cmd = []
    if cores:
        cmd += ["taskset", "-c", cores]        # 코어 고정 — 워크로드 발사율 보호
    cmd += [sys.executable, script, "--model", model_dir, "--threads", str(threads)]
    env = dict(os.environ)
    env["CUDA_VISIBLE_DEVICES"] = ""            # ★GPU 미사용 강제 — 240 LSU 풀 불간섭
    env["HF_HUB_OFFLINE"] = "1"
    t0 = time.monotonic()
    try:
        p = subprocess.run(cmd, input=text, capture_output=True, text=True,
                           timeout=120, env=env)
    except Exception as e:
        return None, 0.0, "실행 실패: %r" % (e,)
    dt = time.monotonic() - t0
    if p.returncode != 0:
        return None, dt, (p.stderr or "").strip()[-200:]
    return p.stdout.strip(), dt, ""


# ── 4부 적용 경로 자리 (구조만 — 호출하지 않는다) ───────────────────────────
class Applier:
    """[Exp_145 4부] 적용 자리. **기본 꺼짐이고 이번 Exp에서 켜지 않는다.**

    나중에 승인 단을 끼울 자리: approve(rec) → apply(rec) 순서가 자연스럽게 들어간다.
    붙일 때의 전제(Exp_141): 시간 몫 적용은 /feeder/ratios 임대라 **Agent가 죽으면
    계약값으로 자동 복귀**한다(kill -9 → 2.41s 실측). 그래서 적용을 붙여도 고착이
    남지 않는다 — 이 전제가 깨지면 적용을 붙이면 안 된다.
    """

    def __init__(self, enabled=False):
        self.enabled = enabled          # 기본 꺼짐
        self.applied = []

    def approve(self, rec):
        """사람 승인 자리 — 미구현(2차년도 범위 밖)."""
        raise NotImplementedError("승인 단 미구현 — 2차년도는 권고까지다")

    def apply(self, rec):
        """적용 자리 — 미구현. 등급이 '즉시'인 조치만 대상이 된다."""
        raise NotImplementedError("적용 미구현 — 2차년도는 권고까지다(Exp_145)")


def advise(goal, llm_mode, model_dir, threads, cores):
    st = observe()                       # 실패하면 ObserveError → 호출자가 멈춘다
    strat, ev, act = decide(st, goal)
    template = render_template(st, strat, ev, act)
    rec = {"ts": st["ts"], "goal": goal, "strategy": strat,
           "strategy_ko": STRATEGIES[strat],
           "evidence": [{"key": k, "detail": v} for k, v in ev],
           "actions": [{"what": a, "grade": g, "how": h} for a, (g, h) in act],
           "state": st, "text": template, "text_source": "template",
           "llm_ms": None, "llm_error": None}
    if llm_mode == "on":
        out, dt, err = render_llm(template, model_dir, threads, cores)
        rec["llm_ms"] = round(dt * 1000, 1)
        if out:
            rec["text"] = out
            rec["text_source"] = "llm"
        else:
            rec["llm_error"] = err or "생성 실패"
    return rec


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--goal", default="throughput",
                    choices=["throughput", "latency", "fair", "placement"])
    ap.add_argument("--watch", action="store_true")
    ap.add_argument("--interval", type=float, default=POLL_S)
    ap.add_argument("--llm", default="off", choices=["on", "off"])
    ap.add_argument("--model", default=os.environ.get(
        "KRAKEN_AGENT_MODEL", "Qwen/Qwen3-1.7B"))
    ap.add_argument("--threads", type=int, default=8)
    ap.add_argument("--cores", default="40-47",
                    help="taskset 코어 고정 범위 — 워크로드 발사율 보호(Exp_145 3-B)")
    ap.add_argument("--log", default="")
    a = ap.parse_args()

    lg = open(a.log, "a") if a.log else None

    def emit(rec):
        print(rec["text"], flush=True)
        if lg:                            # 판정 이력 — 언제 무엇을 왜 권고했는지
            lg.write(json.dumps(rec, ensure_ascii=False) + "\n")
            lg.flush()

    while True:
        try:
            emit(advise(a.goal, a.llm, a.model, a.threads, a.cores))
        except ObserveError as e:
            # [T-5] 조용히 넘기지 않는다. 옛 상태로 권고하지도 않는다.
            msg = "[권고 중단] 관측 수집 실패: %s — 판정하지 않는다(옛 상태로 권고 금지)" % e
            print(msg, flush=True)
            if lg:
                lg.write(json.dumps({"ts": round(time.time(), 3),
                                     "error": str(e), "halted": True},
                                    ensure_ascii=False) + "\n")
                lg.flush()
            if not a.watch:
                return 2
        if not a.watch:
            return 0
        time.sleep(a.interval)


if __name__ == "__main__":
    sys.exit(main())
