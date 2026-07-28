#!/usr/bin/env python3
# prism_controller.py
# PRISM 컨트롤러 메인 진입점
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
#   python3 prism_controller.py --group default --port 8090

import argparse
import json
import signal
import sys
import threading
import time
import os
from http.server import BaseHTTPRequestHandler, HTTPServer

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'shm'))
from prism_shm import create_shm, get_gpu_info, MAX_TENANTS

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

    def _read_json(self) -> dict:
        length = int(self.headers.get("Content-Length", 0))
        return json.loads(self.rfile.read(length)) if length else {}

    def do_GET(self):
        # [Exp_26] 폐루프 라우트 위임 ([Exp_27] /priority/, [Exp_28] /booster/ 추가)
        if self.path.startswith(("/feeder/", "/lifecycle/", "/priority/",
                                 "/booster/")):
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
        else:
            self._send_json({"error": "not found"}, 404)

    def do_POST(self):
        body = self._read_json()

        # [Exp_26] 폐루프 라우트 위임 ([Exp_27] /priority,/urgent, [Exp_28]
        # /booster 추가 — 기존 /policy/priority(shm write, deprecated)와 별개)
        if self.path.startswith(("/feeder/", "/lifecycle/", "/subscribe",
                                 "/priority", "/urgent", "/booster/")):
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
            self._send_json({"ok": True, "version": _scheduler.get_policy_version()})

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

    parser = argparse.ArgumentParser(prog="prism_controller")
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
    print(f"[controller] SHM 생성: /dev/shm/prism_{_group_id} "
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
    from prism_shm import PrismSharedState
    return ctypes.sizeof(PrismSharedState)


if __name__ == "__main__":
    main()
