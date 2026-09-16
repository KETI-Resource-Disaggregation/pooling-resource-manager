#!/usr/bin/env python3
# kraken_controller.py
# KRAKEN 컨트롤러 메인 진입점
#
# 역할:
#   1. SHM 생성 및 GPU 정보 초기화
#   2. HTTP API (기본 포트 8090) 제공
#   3. 테넌트 등록/해제, policy 제어, stats 조회
#   4. 백그라운드 idle 감지 루프 실행
#
# API:
#   POST /register        {"tenant_id", "virtual_sm", "virtual_mem_mb", "weight"}
#   POST /deregister      {"tenant_idx"}
#   GET  /status          전체 SHM 상태
#   GET  /tenants         등록된 테넌트 목록
#   POST /policy/weights  {"weights": [0.5, 0.5]}
#   POST /policy/round    {"duration_us": 100000}
#   POST /policy/equal    모든 테넌트 동일 가중치
#   POST /profiling/enter {"tenant_idx", "iter_count"}
#   POST /profiling/exit  {}
#
# 실행:
#   python3 kraken_controller.py --group default --port 8090

import argparse
import json
import signal
import sys
import threading
import time
import os
from http.server import BaseHTTPRequestHandler, HTTPServer

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'shm'))
from kraken_shm import create_shm, get_gpu_info, MAX_TENANTS

from registry   import Registry
from allocator  import Allocator
from overcommit import OvercommitManager
from scheduler  import Scheduler
import loop_api   # [Exp_26] 폐루프 배선 (feeder/lifecycle 라우트)

# ── 전역 상태 ─────────────────────────────────────────────────────────────────
_shm     = None
_mm      = None   # mmap 객체 (GC 방지용 참조 유지)
_registry = None
_allocator = None
_overcommit = None
_scheduler  = None
_group_id   = "default"


# ── HTTP 핸들러 ───────────────────────────────────────────────────────────────
class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass   # 기본 로그 억제 (필요 시 활성화)

    def _send_json(self, data: dict, status: int = 200):
        body = json.dumps(data).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_text(self, text: str, status: int = 200,
                   ctype: str = "text/plain; version=0.0.4; charset=utf-8"):
        # [Exp_140 2부] Prometheus text exposition 용
        body = text.encode()
        self.send_response(status)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_json(self) -> dict:
        length = int(self.headers.get("Content-Length", 0))
        return json.loads(self.rfile.read(length)) if length else {}

    def do_GET(self):
        # [Exp_26] 폐루프 라우트 위임 ([Exp_27] /priority/, [Exp_28] /booster/ 추가)
        if self.path.startswith(("/feeder/", "/lifecycle/", "/events/", "/priority/",
                                 "/booster/", "/scanner/")):   # [Exp_126]
            return loop_api.handle_get(self, self.path)
        if self.path == "/status":
            shm = _shm
            self._send_json({
                "group_id":          _group_id,
                "mode":              _scheduler.get_mode(),
                "policy_version":    _scheduler.get_policy_version(),
                "physical_sm":       shm.physical_sm_total,
                "physical_mem_mb":   shm.physical_mem_mb,
                "virtual_sm_total":  shm.virtual_sm_total,
                "virtual_mem_total_mb": shm.virtual_mem_total_mb,
                "round_duration_us": shm.policy.round_duration_us,
                "tenant_count":      shm.tenant_count,
            })
        elif self.path == "/tenants":
            self._send_json({"tenants": _registry.list_tenants()})
        elif self.path == "/stats":
            self._send_json({"stats": _scheduler.get_stats()})
        elif self.path.split("?")[0].rstrip("/") == "/capacity":
            # [Exp_82] A-2 규격: 가용 용량 조회. shim(:8091) 흡수 — 팟 내 catalog(factor)
            # + physical(shm) + feeder(slices) 조합. K8s allocatable 대신 factor로 광고 산출.
            self._send_json(_capacity_response())
        elif self.path.split("?")[0].rstrip("/") == "/metrics":
            # [Exp_140 2부] A-4·v1.3 38장 실서빙 — /capacity 와 같은 소스.
            # Exp_139까지 이 경로는 404 였고 kraken_* 는 미가동 스텁 전용이었다.
            self._send_text(_metrics_response())
        else:
            self._send_json({"error": "not found"}, 404)

    def do_POST(self):
        body = self._read_json()

        # [Exp_26] 폐루프 라우트 위임 ([Exp_27] /priority,/urgent, [Exp_28]
        # /booster 추가 — 기존 /policy/priority(shm write, deprecated)와 별개)
        if self.path.startswith(("/feeder/", "/lifecycle/", "/events/", "/subscribe",
                                 "/priority", "/urgent", "/booster/",
                                 "/scanner/")):   # [Exp_126]
            return loop_api.handle_post(self, self.path, body)

        if self.path == "/register":
            try:
                tenant_id    = body["tenant_id"]
                virtual_sm   = int(body.get("virtual_sm", 40))
                virtual_mem  = int(body.get("virtual_mem_mb", 4096))
                weight       = float(body.get("weight", 1.0))
                idx = _registry.register(tenant_id, virtual_sm, virtual_mem, weight)
                env = _allocator.get_env_for_tenant(idx)
                self._send_json({"tenant_idx": idx, "env": env})
                print(f"[controller] 등록: {tenant_id} slot={idx} "
                      f"sm={virtual_sm} mem={virtual_mem}MB "
                      f"mode={_scheduler.get_mode()}")
            except Exception as e:
                self._send_json({"error": str(e)}, 400)

        elif self.path == "/deregister":
            try:
                idx = int(body["tenant_idx"])
                _registry.deregister(idx)
                self._send_json({"ok": True})
                print(f"[controller] 해제: slot={idx} mode={_scheduler.get_mode()}")
            except Exception as e:
                self._send_json({"error": str(e)}, 400)

        elif self.path == "/deregister_by_id":
            # Called by the Device Plugin pod watcher when a pod terminates.
            # Looks up the slot by tenant_id string (= device ID) and deregisters it.
            try:
                tenant_id = body["tenant_id"]
                idx = _registry.find_slot_by_tenant_id(tenant_id)
                if idx < 0:
                    self._send_json({"error": f"tenant_id not found: {tenant_id}"}, 404)
                    return
                _registry.deregister(idx)
                self._send_json({"ok": True, "tenant_idx": idx})
                print(f"[controller] 해제(by_id): {tenant_id} slot={idx} "
                      f"mode={_scheduler.get_mode()}")
            except Exception as e:
                self._send_json({"error": str(e)}, 400)

        elif self.path == "/policy/weights":
            weights = body.get("weights", [])
            _scheduler.set_weights(weights)
            # [Exp_107 T-3] weight 를 feeder 까지 배선한다.
            #   Exp_106 N-1: SHM policy.weights 에는 기록됐으나 게이트를 움직이는
            #   feeder 가 그 값을 보지 않아 weight 2:1/4:1 에서도 처리량 비가 1.00 이었다.
            #   ★SHM 판독 책임은 여기(controller)에 둔다 — feeder 는 순수 분배기로 남긴다.
            wmap, unmapped = {}, 0
            try:
                shm = _scheduler.shm
                for i, wv in enumerate(weights[:shm.tenant_count]):
                    tid = shm.alloc[i].tenant_id.decode().rstrip("\x00")
                    if tid:
                        wmap[tid] = float(wv)
                    else:
                        unmapped += 1
                loop_api._feeder.set_weights(wmap)
            except Exception as e:
                # [Exp_107 T-5] 조용한 폴백 금지
                print(f"[controller][경고] weight→feeder 배선 실패 err={e!r} "
                      f"→ feeder 는 LSU 비례만 적용한다(weight 무시).", flush=True)
            if unmapped:
                print(f"[controller][경고] weight {unmapped}건이 tenant_id 미매핑 — 무시됨.", flush=True)
            self._send_json({"ok": True, "version": _scheduler.get_policy_version(),
                             "feeder_weights": wmap})

        elif self.path == "/policy/round":
            dur = int(body.get("duration_us", 100_000))
            _scheduler.set_round_duration(dur)
            self._send_json({"ok": True})

        elif self.path == "/policy/equal":
            _scheduler.set_equal_weights()
            self._send_json({"ok": True})

        elif self.path == "/policy/priority":
            idx = int(body["tenant_idx"])
            pri = int(body["priority"])
            _scheduler.set_priority(idx, pri)
            self._send_json({"ok": True})

        elif self.path == "/profiling/enter":
            idx   = int(body.get("tenant_idx", 0))
            iters = int(body.get("iter_count", 5))
            _overcommit.enter_profiling(idx, iters)
            self._send_json({"ok": True})
            print(f"[controller] PROFILING 모드: slot={idx} iters={iters}")

        elif self.path == "/profiling/exit":
            _overcommit.exit_profiling()
            self._send_json({"ok": True, "mode": _scheduler.get_mode()})
            print(f"[controller] PROFILING 종료 → mode={_scheduler.get_mode()}")

        else:
            self._send_json({"error": "not found"}, 404)


# ── [Exp_82] A-2 가용 용량 (shim :8091 흡수) ─────────────────────────────────
_CATALOG_PATH = os.environ.get("KRAKEN_CATALOG", "/etc/kraken/catalog.json")
_CAT_WARNED = False   # [Exp_107 T-5] 폴백 경고 1회만
_FEEDER_WARNED = False
_NODE_WARNED = False


def _node_name():
    """[Exp_107 T-5] 노드명 폴백 고지.
    KRAKEN_NODE 미설정 시 개발 노드명으로 폴백한다 — 다른 노드에 배포하면
    /capacity 가 잘못된 노드를 가리키는데 아무 신호가 없다(Exp_103 E-3 하드코딩 10건).
    """
    # [Exp_140] KRAKEN_NODE_NAME(fieldRef, Exp_138 2-B 5)도 본다 — DS 가 이미
    # 넣어주는 값이라 폴백 경고 없이 어느 노드서든 맞는 이름이 나온다.
    v = os.environ.get("KRAKEN_NODE") or os.environ.get("KRAKEN_NODE_NAME")
    if v:
        return v
    global _NODE_WARNED
    if not _NODE_WARNED:
        _NODE_WARNED = True
        print("[controller][경고] KRAKEN_NODE 미설정 → 기본값 'gpu-npu-server-02' 사용. "
              "다른 노드라면 /capacity 의 node 필드가 틀린다.", flush=True)
    return "gpu-npu-server-02"


def _capacity_response():
    """A-2 규격 응답. 팟 내 catalog(factor·physical_lsu) + shm + feeder(slices) 조합."""
    phys_lsu, factor, measured = 178, 1.0, True
    try:
        cat = json.load(open(_CATALOG_PATH))
        factor = float(cat.get("overcommit_factor", 1.0))
        devs = [d for d in cat.get("devices", {}).values() if d.get("measured")]
        if devs:
            phys_lsu = max(d["lsu"] for d in devs)
    except Exception as e:
        # [Exp_107 T-5] 조용한 폴백 금지 — 무엇을 못 읽었고 무엇으로 대체했는지 남긴다.
        #   Exp_106 N-2: controller 에 catalog 가 미마운트인데 아무 경고 없이 178/1.0 으로
        #   폴백해, allocatable(214)과 /capacity(178)가 어긋난 것을 아무도 몰랐다.
        global _CAT_WARNED
        if not _CAT_WARNED:
            _CAT_WARNED = True
            print(f"[controller][경고] catalog 읽기 실패 path={_CATALOG_PATH} err={e!r} "
                  f"→ 기본값 폴백(physical_lsu=178, overcommit_factor=1.0). "
                  f"/capacity 가 실제 광고와 어긋날 수 있다.", flush=True)
    adv_lsu = round(phys_lsu * factor)
    try:
        tenants = loop_api._feeder.status().get("tenants", {})
    except Exception as e:
        # [Exp_107 T-5] 조용한 폴백 금지 — 이게 비면 allocated_lsu·slices 가 0 으로
        #   보고되어 "아무도 안 쓰는 중"처럼 보인다. 실제 배치와 어긋난다.
        global _FEEDER_WARNED
        if not _FEEDER_WARNED:
            _FEEDER_WARNED = True
            print(f"[controller][경고] feeder status 조회 실패 err={e!r} "
                  f"→ tenants={{}} 폴백. /capacity 의 allocated_lsu·slices 가 "
                  f"실제 배치와 어긋난다.", flush=True)
        tenants = {}
    allocated = sum(round(v.get("ratio", 0) * phys_lsu) for v in tenants.values())
    ratio = round(allocated / phys_lsu, 3) if phys_lsu else 0.0
    shm = _shm
    mem_cap_mb = getattr(shm, "physical_mem_mb", 97887)
    # [Exp_140 2-B] mem_mb — Allocate 의 sliceSpec() 과 같은 식으로 파생:
    #   virtualMemMB = MemoryMB × units ÷ advertised (정수 나눗셈, plugin.go:253).
    #   units = round(ratio × physical). 실주입값(BLESS_MEM_QUOTA_MB)과 같은 산식.
    # [Exp_141 4부] time_ratio=적용값(기존 의미 유지) 옆에 contract_time_ratio=
    #   계약값(등록 시점 몫). ④·적응형이 내린 값과 계약을 구분해 내보낸다 —
    #   고착값이 계약처럼 고려대·오케스트로에 나가던 것(Exp_138 0-B)의 노출 분리.
    #   36장 규격 갱신 제안 대상(신설 필드).
    slices = [{"tenant": k, "compute_pct": round(v.get("ratio", 0) * 100, 1),
               "time_ratio": round(v.get("ratio", 0), 3),
               "contract_time_ratio": round(v.get("contract", v.get("ratio", 0)), 3),
               "mem_mb": (int(mem_cap_mb * round(v.get("ratio", 0) * phys_lsu)
                              // adv_lsu) if adv_lsu else 0),
               "armed": v.get("armed", False)}
              for k, v in tenants.items()]
    return {
        "schema_version": "1.0", "node": _node_name(),
        "devices": [{
            "uuid": _group_id, "kind": "gpu",
            "model": "NVIDIA RTX PRO 6000 Blackwell Server Edition",
            "capacity": {"physical_lsu": phys_lsu, "advertised_lsu": adv_lsu,
                         "overcommit_factor": factor, "allocated_lsu": allocated,
                         "allocation_ratio": ratio, "lsu_measured": measured},
            "memory": {"capacity_mb": getattr(shm, "physical_mem_mb", 97887),
                       "quota_divisor": "advertised_lsu"},
            "slices": slices,
            "interference_hint": {"current_sigma_pct": ratio, "no_interference_below": 1.00,
                                  "interference_above": 1.40,
                                  "note": "100~140%는 victim 무게 의존 전이(Exp_80). 경량 저지연 victim ~130%까지 안전"},
        }],
        "npu": {"available_cores": 8,
                "request_rule": {"allowed_blocks": [1, 2, 4, 8], "alignment": "power_of_two_contiguous"},
                "lsu_measured": False},
    }


# ── [Exp_140 2부] /metrics — Prometheus text exposition ──────────────────────
# 이름·라벨은 A-4(docs/interface/A4_orchestro_metrics_events.md)·v1.3 38장·
# 스텁(integration_demo/orchestro_stub/kraken_exporter.py)을 승계한다. 새로 짓지 않는다.
#
# 내보내지 않는 것 (T-5: 없는 값을 0 으로 채우지 않는다):
#   - kraken_slice_retention_ratio — ④(closed_loop) 미가동 시 값이 없다. ④는 독립
#     CLI 라 controller 로 값이 오지 않는다(배선 자체가 없음). 부재로 둔다.
#   - kraken_remote_* — RemotePool CR 이 없거나 조회 실패면 부재로 둔다.
#   - slice 의 pod·class 라벨 — watcher 선기록(podref/class)이 없으면 그 라벨을
#     뺀다(빈 문자열 금지).
_REMOTE_WARNED = False


def _prom_escape(v):
    return str(v).replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def _slice_extra(tenant_id):
    """<sockDir>/<devID>/{class,podref} — watcher 선기록(Exp_110 R-1·Exp_126 4-B).

    controller 는 파드명을 모른다(feeder 는 devID 만 앎). watcher 가 hostPath 에
    남긴 podref("<ns>/<name>/<container>")를 읽어 pod 라벨을 채운다. 없으면 뺀다.
    """
    base = os.environ.get("KRAKEN_BLESS_SOCK_DIR", "/var/lib/kraken/socks")
    cls, pod = "", ""
    try:
        c = open(os.path.join(base, tenant_id, "class")).read().strip()
        if c in ("compute", "memory"):
            cls = c
    except Exception:
        pass
    try:
        parts = open(os.path.join(base, tenant_id, "podref")).read().strip().split("/")
        if len(parts) == 3:
            pod = parts[1]
    except Exception:
        pass
    return cls, pod


def _remote_pools():
    """RemotePool CR 목록 — SA 토큰으로 API 직독(스텁의 kubectl 대체, RBAC Exp_140).
    실패·부재 시 None — 호출자는 kraken_remote_* 를 내보내지 않는다."""
    import ssl
    import urllib.request
    sa = "/var/run/secrets/kubernetes.io/serviceaccount"
    host = os.environ.get("KUBERNETES_SERVICE_HOST", "")
    if not host:
        return None
    try:
        tok = open(os.path.join(sa, "token")).read().strip()
        ctx = ssl.create_default_context(cafile=os.path.join(sa, "ca.crt"))
        url = "https://%s:%s/apis/keti.re.kr/v1alpha1/remotepools" % (
            host, os.environ.get("KUBERNETES_SERVICE_PORT", "443"))
        req = urllib.request.Request(url, headers={"Authorization": "Bearer " + tok})
        with urllib.request.urlopen(req, timeout=3, context=ctx) as r:
            return json.load(r).get("items", [])
    except Exception as e:
        global _REMOTE_WARNED
        if not _REMOTE_WARNED:
            _REMOTE_WARNED = True
            print(f"[controller][경고] RemotePool 조회 실패 err={e!r} — "
                  f"kraken_remote_* 계열은 내보내지 않는다(0 아님, 부재). "
                  f"RBAC(remotepools get/list)·CRD 존재를 확인하라.", flush=True)
        return None


def _metrics_response():
    cap = _capacity_response()
    d = cap["devices"][0]
    c = d["capacity"]
    node = _prom_escape(cap["node"])
    L = 'node="%s",device="%s",location="local"' % (node, _prom_escape(d["uuid"]))
    ratio = c["allocation_ratio"]
    hint = d["interference_hint"]
    # zone 판정 주체 = 이 exporter. 임계는 /capacity interference_hint 와 동일 상수
    # (Exp_80: <1.00 무간섭 / 1.00~1.40 전이 / >=1.40 간섭). 스텁 render() 승계.
    zone = 2 if ratio >= hint["interference_above"] else (
        1 if ratio >= hint["no_interference_below"] else 0)

    out = []

    def fam(name, help_, series):
        if not series:
            return                      # 계열 자체가 비면 HELP/TYPE 도 내지 않는다
        out.append("# HELP %s %s" % (name, help_))
        out.append("# TYPE %s gauge" % name)
        out.extend(series)

    fam("kraken_node_physical_lsu", "물리 논리 용량(실측 LSU)",
        ["kraken_node_physical_lsu{%s} %s" % (L, c["physical_lsu"])])
    adv_series = ["kraken_node_advertised_lsu{%s} %s" % (L, c["advertised_lsu"])]
    fam("kraken_node_overcommit_factor", "오버커밋 배율(catalog overcommit_factor)",
        ["kraken_node_overcommit_factor{%s} %s" % (L, c["overcommit_factor"])])
    fam("kraken_node_allocation_ratio", "총 할당률(=Σpct, 간섭 축)",
        ["kraken_node_allocation_ratio{%s} %s" % (L, ratio)])
    fam("kraken_node_interference_zone",
        "0=무간섭(<100%) 1=전이 2=간섭(>=140%) — 임계는 interference_hint(Exp_80) 동일",
        ["kraken_node_interference_zone{%s} %s" % (L, zone)])
    fam("kraken_node_npu_cores_available", "NPU 가용 코어(/capacity npu 와 동일 소스)",
        ['kraken_node_npu_cores_available{node="%s"} %s'
         % (node, cap["npu"]["available_cores"])])

    pct_s, mem_s, tr_s, ctr_s, armed_s, mode_s = [], [], [], [], [], []
    for s in d.get("slices", []):
        cls, pod = _slice_extra(s["tenant"])
        SL = L + ',tenant="%s"' % _prom_escape(s["tenant"])
        if pod:
            SL += ',pod="%s"' % _prom_escape(pod)
        if cls:
            SL += ',class="%s"' % cls
        pct_s.append("kraken_slice_compute_pct{%s} %s" % (SL, s["compute_pct"]))
        mem_s.append("kraken_slice_mem_mb{%s} %s" % (SL, s["mem_mb"]))
        tr_s.append("kraken_slice_time_ratio{%s} %s" % (SL, s["time_ratio"]))
        # [Exp_141 4부] 계약값 — time_ratio(적용값)의 기존 의미는 그대로 두고
        # 새 계열로 분리(38장 갱신 제안). 이름은 kraken_slice_* 규칙 승계.
        if "contract_time_ratio" in s:
            ctr_s.append("kraken_slice_contract_time_ratio{%s} %s"
                         % (SL, s["contract_time_ratio"]))
        armed_s.append("kraken_slice_armed{%s} %s" % (SL, 1 if s["armed"] else 0))
        # mode 값 = feeder 의 슬라이스 실상태(armed/released). 38장의 relaxed|strict|off
        # 는 슬라이스 단위 실체가 없어 쓰지 않는다(지시서 2-C·report §4 — 38장 갱신 제안).
        mode_s.append('kraken_slice_control_mode{%s,mode="%s"} 1'
                      % (SL, "armed" if s["armed"] else "released"))
    fam("kraken_slice_compute_pct", "슬라이스 연산 몫(%)", pct_s)
    fam("kraken_slice_mem_mb", "슬라이스 메모리 쿼터(MB, sliceSpec 동일 산식)", mem_s)
    fam("kraken_slice_time_ratio", "슬라이스 시간 몫 — 현재 적용값", tr_s)
    fam("kraken_slice_contract_time_ratio",
        "슬라이스 시간 몫 — 계약값(등록 시점, Exp_141)", ctr_s)
    fam("kraken_slice_armed", "1=제어 걸림(armed) 0=풀림(released)", armed_s)
    fam("kraken_slice_control_mode", "슬라이스 제어 상태(mode=armed|released)", mode_s)

    pools = _remote_pools()
    leased_s, phase_s = [], []
    if pools:
        for p in pools:
            dev = p.get("spec", {}).get("device", {})
            st = p.get("status", {})
            RL = ('node="%s",device="%s",location="remote",provider="%s"'
                  % (node, _prom_escape(dev.get("uuid", "")),
                     _prom_escape(p.get("spec", {}).get("providerHost", ""))))
            adv_series.append("kraken_node_advertised_lsu{%s} %s"
                              % (RL, st.get("advertisedLsu", 0)))
            leased_s.append("kraken_remote_leased_lsu{%s} %s"
                            % (RL, st.get("leasedLsu", 0)))
            if st.get("phase"):
                phase_s.append('kraken_remote_pool_phase{%s,phase="%s"} 1'
                               % (RL, _prom_escape(st["phase"])))
    fam("kraken_node_advertised_lsu", "스케줄링에 노출한 LSU(local/remote)", adv_series)
    fam("kraken_remote_leased_lsu", "원격 풀 lease 중 LSU", leased_s)
    fam("kraken_remote_pool_phase", "RemotePool phase(라벨 phase, 값 1)", phase_s)

    return "\n".join(out) + "\n"


# ── 백그라운드 루프 ───────────────────────────────────────────────────────────
def _idle_check_loop(stop_event: threading.Event):
    while not stop_event.is_set():
        try:
            _overcommit.tick_idle_check()
        except Exception:
            pass
        stop_event.wait(1.0)   # 1초 간격


# ── 진입점 ────────────────────────────────────────────────────────────────────
def main():
    global _shm, _mm, _registry, _allocator, _overcommit, _scheduler, _group_id

    parser = argparse.ArgumentParser(prog="kraken_controller")
    parser.add_argument("--group",      default="default",  help="그룹 ID")
    parser.add_argument("--port",       type=int, default=8090)
    parser.add_argument("--sm",         type=int, default=0,
                        help="물리 SM 수 (0=자동 감지)")
    parser.add_argument("--mem",        type=int, default=0,
                        help="물리 메모리(MB) (0=자동 감지)")
    parser.add_argument("--no-mps",     action="store_true",
                        help="MPS 데몬 자동 시작 억제")
    args = parser.parse_args()

    _group_id = args.group

    # GPU 정보 감지
    detected_sm, detected_mem = get_gpu_info()
    physical_sm  = args.sm  or detected_sm  or 80
    physical_mem = args.mem or detected_mem or 16384

    print(f"[controller] 시작: group={_group_id} GPU SM={physical_sm} mem={physical_mem}MB")

    # SHM 생성
    _shm, _mm = create_shm(_group_id, physical_sm, physical_mem)
    print(f"[controller] SHM 생성: /dev/shm/kraken_{_group_id} "
          f"({import_size()} bytes)")

    # 모듈 초기화
    _registry   = Registry(_shm)
    _allocator  = Allocator(_shm)
    _overcommit = OvercommitManager(_shm)
    _scheduler  = Scheduler(_shm)

    # MPS 시작
    if not args.no_mps:
        _allocator.setup_mps()

    # [Exp_26] 폐루프 모듈 초기화 (상주 피더 스레드 포함)
    loop_api.init()

    # 백그라운드 idle 감지
    stop_event = threading.Event()
    idle_thread = threading.Thread(target=_idle_check_loop,
                                   args=(stop_event,), daemon=True)
    idle_thread.start()

    # 종료 처리
    def shutdown(sig, frame):
        print("\n[controller] 종료 중...")
        stop_event.set()
        loop_api.stop()   # [Exp_26]
        if not args.no_mps:
            _allocator.stop_mps()
        sys.exit(0)

    signal.signal(signal.SIGINT,  shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    # HTTP 서버
    server = HTTPServer(("0.0.0.0", args.port), Handler)
    print(f"[controller] HTTP API: http://0.0.0.0:{args.port}")
    print(f"[controller] 준비 완료. Ctrl+C로 종료.")
    server.serve_forever()


def import_size():
    import ctypes
    from kraken_shm import KrakenSharedState
    return ctypes.sizeof(KrakenSharedState)


if __name__ == "__main__":
    main()
