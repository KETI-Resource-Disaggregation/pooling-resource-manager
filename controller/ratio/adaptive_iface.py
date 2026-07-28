#!/usr/bin/env python3
"""Exp_19 Phase 3 — 적응형 루프 연결 인터페이스 (조회 데모, 폐루프 제어 아님).

경로: phase_online 이벤트(transition) → co-located 테넌트 라벨 조합 키 →
adaptive_map.json 조회 → 권고 {sm_split, time_control, expected_oc}.
실제 재설정(MPS pct / time_add weight)은 Track 1-1 — 여기선 조회·출력까지만.
"""
import json, os

D = os.path.dirname(os.path.abspath(__file__))
MAP = json.load(open(os.path.join(D, "adaptive_map.json")))


def lookup(labels):
    """labels: co-located 테넌트들의 현재 국면 라벨 리스트 → 권고 dict."""
    key = "+".join(sorted(labels, key=lambda x: ["PREFILL", "DECODE_BATCHED",
                                                 "DECODE", "MIXED", "IDLE"].index(x)
                          if x in ("PREFILL", "DECODE_BATCHED", "DECODE", "MIXED", "IDLE") else 9))
    ent = MAP["pairs"].get(key)
    if ent is None:
        return {"key": key, "known": False,
                "action": "미실측 조합 — 보수적 50/50 + 재분류 유지",
                "basis": MAP["basis"]}
    return {"key": key, "known": True, "oc_eff": ent["oc_eff"],
            "recommend": ent["recommend"],
            "time_control": ent.get("time_control"),
            "basis": MAP["basis"], "exp": ent["exp"]}


if __name__ == "__main__":
    for labels in (["DECODE", "PREFILL"], ["PREFILL", "DECODE_BATCHED"],
                   ["PREFILL", "DECODE", "DECODE"], ["DECODE", "DECODE"],
                   ["MIXED", "DECODE"]):
        r = lookup(labels)
        print(labels, "->", r["key"], "|",
              r.get("recommend", {}).get("sm_split") if r["known"] else r["action"])
