"""residual 장부 v1 — Overcommit 하 (s,t) 배치 잔여 관리 (Exp_26 작업 3).

채택 규칙 (Exp_26 스펙 §작업3 최소 규칙 제안을 채택 — 근거는 report §3):
  free_time = max(0, 1 − Σ time_ratio_i)
      · in-envelope 피더(Σbudget=TICK)와 일치 — 시간 축은 진짜 소모 자원
  free_sm   = 1 − max(s_i of 게이트 미보유(ungated) 테넌트) ; 전원 gated 면 1.0
      · 시간 게이트가 직렬화하는 한 gated 테넌트의 공간은 순간에 하나만
        활성 → 공간 중첩 허용, 단순 차감하지 않음 (Exp_16 시간×SM 직교)
      · ungated 테넌트는 상시 공간 점유 — max 기준으로 보수적 예약
        (합산이 아닌 max: 스펙 제안 채택. Σ로 하면 gated 공존까지 과잉 차단)
  순간 공간 점유 보고치 = max(s_i of gated) — "동시 활성 게이트 보유 테넌트의
      max(s)" (스펙 제안 그대로: 게이트 하 순간 활성은 1개뿐이므로)
이보다 정교한 규칙(시구간별 packing 등)은 v2 논점 — 구현하지 않음.

회피 권고 처리 (v1): adaptive_map 이 회피(sm_split=None)를 권고해도 배치를
거부하지 않는다 — warnings 에 기록 + 감사 로그 (거부 정책은 기관 협의
사항, Exp_29 안건).
"""
import threading
import time


class ResidualLedger:
    def __init__(self):
        self._lock = threading.Lock()
        self._tenants = {}   # name -> {s, t, gated}
        self.audit = []      # 배치/회수/경고 감사 로그

    def _log(self, kind, **kw):
        self.audit.append({"t": round(time.time(), 3), "kind": kind, **kw})

    def view(self):
        """엔진 decide() 에 넘길 (free_sm_ratio, free_time_ratio)."""
        with self._lock:
            t_sum = sum(v["t"] for v in self._tenants.values())
            ungated = [v["s"] for v in self._tenants.values() if not v["gated"]]
            gated = [v["s"] for v in self._tenants.values() if v["gated"]]
            return {
                "free_time_ratio": max(0.0, round(1.0 - t_sum, 6)),
                "free_sm_ratio": (round(1.0 - max(ungated), 6) if ungated
                                  else 1.0),
                "instant_sm_report": max(gated) if gated else 0.0,
                "tenants": {n: dict(v) for n, v in self._tenants.items()},
            }

    def place(self, name, space_ratio, time_ratio, gated=True,
              pair_recommendation=None):
        """배치 기록. 회피 권고는 거부 아닌 warnings (v1 — Exp_29 안건)."""
        warnings = []
        if pair_recommendation is not None:
            rec = pair_recommendation.get("recommend") or {}
            if pair_recommendation.get("known") and rec.get("sm_split") is None:
                warnings.append(
                    f"adaptive_map 회피 권고 조합({pair_recommendation.get('key')})"
                    f" — v1 은 거부하지 않고 기록만 (Exp_29 협의 안건)")
        with self._lock:
            self._tenants[name] = {"s": float(space_ratio),
                                   "t": float(time_ratio),
                                   "gated": bool(gated)}
        self._log("place", tenant=name, s=space_ratio, t=time_ratio,
                  gated=gated, warnings=warnings)
        return warnings

    def update(self, name, space_ratio=None, time_ratio=None, gated=None):
        with self._lock:
            v = self._tenants[name]
            if space_ratio is not None:
                v["s"] = float(space_ratio)
            if time_ratio is not None:
                v["t"] = float(time_ratio)
            if gated is not None:
                v["gated"] = bool(gated)
        self._log("update", tenant=name, s=space_ratio, t=time_ratio,
                  gated=gated)

    def remove(self, name):
        with self._lock:
            self._tenants.pop(name, None)
        self._log("remove", tenant=name)
