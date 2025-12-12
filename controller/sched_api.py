#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
HTTP API → scheduler 제어 + 테넌트 런처
- 표준 라이브러리만 사용 (http.server + Threading)
- 인증: X-API-Key (환경변수 API_KEY 있으면 필수)
- 스케줄러 제어: SET_EQUAL/SET_WEIGHTS/SET_SHARE/SET_TICK/SET_CREDITS/BOOST/BOOST_OFF
- 테넌트 런칭: controller/run_multi.py (권장), controller/run_train.py(레거시) 실행
- 테넌트 관리: 목록/상세/정지/강제종료, blessctl 메시지 전달
- 로그: 프로세스 stdout/stderr를 파일로 저장 후 tail 조회

환경:
  BLESS_MASTER   : 스케줄러 소켓 경로 (스케줄러와 동일하게)
  API_HOST       : 0.0.0.0 (기본)
  API_PORT       : 8080 (기본)
  API_KEY        : 비어있으면 인증 없음
  LD_PRELOAD     : 미지정 시 프로젝트 루트의 libbless/libbless.so를 자동 세팅
"""

import os, sys, json, time, socket, re, signal, shlex
import subprocess as sp
from http.server import BaseHTTPRequestHandler, HTTPServer
from socketserver import ThreadingMixIn
from pathlib import Path
from typing import Dict, Any

# ----------------- 설정 -----------------
ROOT = Path(__file__).resolve().parents[1]      # 프로젝트 루트
MASTER = os.environ.get("BLESS_MASTER", "/tmp/bless-master.sock")
API_KEY = os.environ.get("API_KEY", "")

# 기본 러너 경로
RUN_MULTI = ROOT / "controller" / "run_multi.py"
RUN_TRAIN = ROOT / "controller" / "run_train.py"
LIB_BLESS = ROOT / "libbless" / "libbless.so"

# 아카이브 경로
LOG_DIR = ROOT / "archive" / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

# 프로세스 레지스트리: tenant_id -> dict(pid, popen, started, log, csv_hint, runner, cmd)
PROCS: Dict[str, Dict[str, Any]] = {}

TENANT_RE = re.compile(r"^[A-Za-z0-9_\-]+$")

# --------------- 공용 유틸 ----------------
def _require_api_key(handler: BaseHTTPRequestHandler) -> bool:
    if not API_KEY:
        return False
    return handler.headers.get("X-API-Key","") != API_KEY

def _read_json(handler: BaseHTTPRequestHandler):
    try:
        n = int(handler.headers.get("Content-Length","0"))
        raw = handler.rfile.read(n) if n>0 else b"{}"
        return json.loads(raw.decode("utf-8") or "{}")
    except Exception:
        return None

def _json(handler: BaseHTTPRequestHandler, obj, code=200):
    data = json.dumps(obj).encode("utf-8")
    handler.send_response(code)
    handler.send_header("Content-Type","application/json; charset=utf-8")
    handler.send_header("Access-Control-Allow-Origin", "*")
    handler.send_header("Content-Length", str(len(data)))
    handler.end_headers()
    handler.wfile.write(data)

def _err(handler, msg="bad request", code=400):
    _json(handler, {"ok": False, "error": msg}, code)

def _ok(handler, payload=None):
    _json(handler, {"ok": True, **(payload or {})}, 200)

def _send_master(msg: str):
    s = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    try:
        s.sendto(msg.encode("utf-8"), MASTER)
    finally:
        s.close()

def _bless_sock(pid: int) -> str:
    return f"/tmp/bless-{pid}.sock"

def _send_bless(pid: int, msg: str) -> bool:
    path = _bless_sock(pid)
    try:
        s = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
        s.sendto(msg.encode("utf-8"), path)
        s.close()
        return True
    except OSError:
        return False

def _ensure_ld_preload(env: Dict[str,str]) -> Dict[str,str]:
    env = dict(env)
    if "LD_PRELOAD" not in env or not env["LD_PRELOAD"]:
        env["LD_PRELOAD"] = str(LIB_BLESS)
    return env

def _now() -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S")

def _popen_status(p: sp.Popen) -> str:
    rc = p.poll()
    if rc is None: return "running"
    return f"exited({rc})"

# --------------- 런처 ----------------
def launch_tenant(tenant: str, spec: Dict[str, Any]) -> Dict[str, Any]:
    """
    spec 예시:
      {
        "runner": "multi" | "train",     # 기본 multi
        "domain": "nlp"|"cv",            # multi 전용
        "model": "gpt2"/"bert"/"resnet50"/"vgg16"/...,
        "steps": 200,
        # NLP
        "batch": 8, "seq": 256, "vocab": 4096,
        "d_model": 768, "n_layer": 6, "n_head": 12, "ff_mult": 4,
        # CV
        "imgsz": 224, "classes": 1000, "amp": True, "optim": "sgd"|"adamw",
        # 공통
        "runner_name": "A",  # CSV 파일명 prefix로 사용됨
        "python": "/usr/bin/python3"     # 기본: sys.executable
      }
    """
    assert TENANT_RE.match(tenant), "invalid tenant id"
    if tenant in PROCS and PROCS[tenant]["popen"].poll() is None:
        raise RuntimeError(f"tenant '{tenant}' already running (pid={PROCS[tenant]['pid']})")

    runner = (spec.get("runner") or "multi").lower()
    py = spec.get("python") or sys.executable

    cmd = []
    if runner == "train":
        # 레거시 GPT/BERT 러너
        model = spec.get("model") or "gpt2"
        steps = int(spec.get("steps", 200))
        batch = int(spec.get("batch", 8))
        seq   = int(spec.get("seq", 256))
        d_model = int(spec.get("d_model", 768))
        n_layer = int(spec.get("n_layer", 6))
        n_head  = int(spec.get("n_head", 12))
        ff_mult = int(spec.get("ff_mult", 4))
        cmd = [
            py, str(RUN_TRAIN),
            "--model", model,
            "--steps", str(steps),
            "--batch", str(batch),
            "--seq",   str(seq),
            "--vocab", str(int(spec.get("vocab", 4096))),
            "--d_model", str(d_model),
            "--n_layer", str(n_layer),
            "--n_head",  str(n_head),
            "--ff_mult", str(ff_mult),
        ]
    else:
        # 통합 러너 권장
        domain = (spec.get("domain") or "nlp").lower()
        model  = spec.get("model")  or ("gpt2" if domain=="nlp" else "resnet50")
        steps  = int(spec.get("steps", 200))
        cmd = [py, str(RUN_MULTI), "--domain", domain, "--model", model, "--steps", str(steps)]
        if domain == "nlp":
            for k, dv in [("batch",12),("seq",384),("vocab",4096),
                          ("d_model",768),("n_layer",6),("n_head",12),("ff_mult",4)]:
                if k in spec: cmd += [f"--{k}", str(spec[k])]
        else:
            # cv
            for k, dv in [("batch",32),("imgsz",224),("classes",1000)]:
                if k in spec: cmd += [f"--{k}", str(spec[k])]
            if spec.get("amp"): cmd += ["--amp"]
            if spec.get("optim"): cmd += ["--optim", str(spec["optim"])]

    # 환경 구성
    env = os.environ.copy()
    env["BLESS_MASTER"] = MASTER
    env["BLESS_TENANT"] = tenant
    if spec.get("runner_name"):
        env["RUNNER_NAME"] = str(spec["runner_name"])
    env = _ensure_ld_preload(env)

    # 로그 파일
    ts_tag = time.strftime("%Y%m%d-%H%M%S")
    log_path = LOG_DIR / f"{tenant}-{ts_tag}.log"
    log_f = open(log_path, "ab", buffering=0)

    # Popen
    p = sp.Popen(cmd, cwd=str(ROOT), env=env, stdout=log_f, stderr=log_f, preexec_fn=os.setsid)
    info = {
        "pid": p.pid,
        "popen": p,
        "started": _now(),
        "log": str(log_path),
        "csv_hint": f"{tenant}.csv",
        "runner": runner,
        "cmd": " ".join(shlex.quote(x) for x in cmd),
        "env_overrides": {k: env[k] for k in ["BLESS_MASTER","BLESS_TENANT","RUNNER_NAME","LD_PRELOAD"] if k in env}
    }
    PROCS[tenant] = info
    return {"tenant": tenant, **{k:v for k,v in info.items() if k!="popen"}}

def stop_tenant(tenant: str, force=False, timeout=5.0) -> Dict[str, Any]:
    if tenant not in PROCS:
        return {"ok": False, "error": "not found"}
    p: sp.Popen = PROCS[tenant]["popen"]
    if p.poll() is not None:
        return {"ok": True, "note": "already exited", "pid": p.pid}
    try:
        if force:
            os.killpg(os.getpgid(p.pid), signal.SIGKILL)
            return {"ok": True, "killed": True, "pid": p.pid}
        else:
            os.killpg(os.getpgid(p.pid), signal.SIGTERM)
            t0 = time.time()
            while time.time()-t0 < timeout:
                if p.poll() is not None: break
                time.sleep(0.1)
            if p.poll() is None:
                os.killpg(os.getpgid(p.pid), signal.SIGKILL)
                return {"ok": True, "killed_after_timeout": True, "pid": p.pid}
            return {"ok": True, "stopped": True, "pid": p.pid, "rc": p.returncode}
    finally:
        # 남은 파일 핸들 닫기 시도
        try:
            for f in [p.stdout, p.stderr]:
                if f: f.close()
        except Exception:
            pass

def list_tenants() -> Dict[str, Any]:
    out = {}
    for t,info in PROCS.items():
        p = info["popen"]
        out[t] = {k:v for k,v in info.items() if k!="popen"}
        out[t]["status"] = _popen_status(p)
    return out

def show_tenant(tenant: str) -> Dict[str, Any]:
    if tenant not in PROCS:
        return {"ok": False, "error": "not found"}
    info = dict(PROCS[tenant])
    info["status"] = _popen_status(info["popen"])
    info.pop("popen", None)
    return info

def tail_log(path: str, n: int = 200) -> str:
    p = Path(path)
    if not p.exists(): return ""
    try:
        with p.open("rb") as f:
            f.seek(0, os.SEEK_END)
            size = f.tell()
            chunk = 4096
            data = b""
            pos = size
            while pos > 0 and data.count(b"\n") <= n:
                pos = max(0, pos - chunk)
                f.seek(pos)
                data = f.read(size - pos) + data
                if pos == 0: break
            lines = data.splitlines()[-n:]
            return "\n".join(x.decode("utf-8", "replace") for x in lines)
    except Exception:
        return ""

# --------------- HTTP 핸들러 ---------------
SPEC = {
  "health": {"GET /health": {}},
  "scheduler": {
    "POST /set_equal":    {"equal": "bool"},
    "POST /set_weights":  {"weights": {"TENANT":"pct"}},
    "POST /set_share":    {"tenant":"str","pct":"0..100"},
    "POST /set_tick":     {"tick_s":"float>0"},
    "POST /set_credits":  {"credits":"int>0"},
    "POST /boost":        {"tenant":"str"},
    "POST /boost_off":    {}
  },
  "launcher": {
    "POST /launch": {
      "tenant":"A..Z/..", "runner":"multi|train (default multi)",
      # for multi:
      "domain":"nlp|cv", "model":"gpt2|bert|resnet50|vgg16|...", "steps":"int",
      # NLP: batch,seq,vocab,d_model,n_layer,n_head,ff_mult
      # CV : batch,imgsz,classes,amp(bool),optim(sgd|adamw)
      "runner_name":"optional"
    },
    "POST /stop": {"tenant":"str"},
    "POST /kill": {"tenant":"str"},
    "GET  /tenants": {},
    "GET  /tenants/<tenant>": {},
    "POST /blessctl": {"pid":"int", "msg":"str"},
    "GET  /logs/<tenant>?n=200": {}
  }
}

class ThreadingHTTPServer(ThreadingMixIn, HTTPServer):
    daemon_threads = True

class Api(BaseHTTPRequestHandler):
    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin","*")
        self.send_header("Access-Control-Allow-Methods","GET,POST,OPTIONS")
        self.send_header("Access-Control-Allow-Headers","Content-Type,X-API-Key")
        self.end_headers()

    def do_GET(self):
        if _require_api_key(self): return _err(self, "unauthorized", 401)
        if self.path == "/" or self.path == "/spec":
            return _ok(self, {"endpoints": SPEC, "master": MASTER})
        if self.path == "/health":
            return _ok(self, {"ok": True, "time": _now()})
        if self.path.startswith("/tenants"):
            parts = self.path.split("?")[0].split("/")
            if len(parts) == 2:
                return _ok(self, {"tenants": list_tenants()})
            elif len(parts) == 3 and parts[2]:
                t = parts[2]
                return _ok(self, {"tenant": show_tenant(t)})
        if self.path.startswith("/logs/"):
            parts = self.path.split("?")[0].split("/")
            if len(parts) == 3 and parts[2]:
                t = parts[2]
                n = 200
                if "?" in self.path:
                    try:
                        q = self.path.split("?",1)[1]
                        for kv in q.split("&"):
                            if kv.startswith("n="):
                                n = int(kv.split("=",1)[1])
                    except: pass
                info = PROCS.get(t)
                if not info: return _err(self, "not found", 404)
                txt = tail_log(info["log"], n=n)
                return _ok(self, {"tenant": t, "lines": n, "log": txt})
        return _err(self, "not found", 404)

    def do_POST(self):
        if _require_api_key(self): return _err(self, "unauthorized", 401)
        body = _read_json(self)
        if body is None: return _err(self, "invalid json")

        route = self.path.rstrip("/")
        try:
            # --- 스케줄러 제어 ---
            if route == "/set_equal":
                equal = bool(body.get("equal"))
                _send_master(f"SET_EQUAL {1 if equal else 0}")
                return _ok(self)
            if route == "/set_weights":
                w = body.get("weights", {})
                if not isinstance(w, dict) or not w: return _err(self, "weights dict required")
                spec = ",".join([f"{k}:{float(v)}" for k,v in w.items()])
                _send_master(f"SET_WEIGHTS {spec}")
                return _ok(self)
            if route == "/set_share":
                t = body.get("tenant"); pct = float(body.get("pct", -1))
                if not t or pct<0 or pct>100: return _err(self, "tenant & pct(0..100) required")
                _send_master(f"SET_SHARE {t} {pct}")
                return _ok(self)
            if route == "/set_tick":
                tick = float(body.get("tick_s", 0))
                if tick <= 0: return _err(self, "tick_s>0 required")
                _send_master(f"SET_TICK {tick}")
                return _ok(self)
            if route == "/set_credits":
                cred = int(body.get("credits", 0))
                if cred <= 0: return _err(self, "credits>0 required")
                _send_master(f"SET_CREDITS {cred}")
                return _ok(self)
            if route == "/boost":
                t = body.get("tenant")
                if not t: return _err(self, "tenant required")
                _send_master(f"BOOST {t}")
                return _ok(self)
            if route == "/boost_off":
                _send_master("BOOST_OFF"); return _ok(self)

            # --- 런처 ---
            if route == "/launch":
                tenant = str(body.get("tenant","")).strip()
                if not tenant or not TENANT_RE.match(tenant):
                    return _err(self, "invalid tenant id (A-Z,0-9,_,-)")
                info = launch_tenant(tenant, body)
                return _ok(self, {"launched": info})

            if route == "/stop":
                tenant = str(body.get("tenant","")).strip()
                if not tenant: return _err(self, "tenant required")
                res = stop_tenant(tenant, force=False)
                return _ok(self, res)

            if route == "/kill":
                tenant = str(body.get("tenant","")).strip()
                if not tenant: return _err(self, "tenant required")
                res = stop_tenant(tenant, force=True)
                return _ok(self, res)

            if route == "/blessctl":
                pid = int(body.get("pid", 0)); msg = body.get("msg","")
                if pid <= 0 or not msg: return _err(self, "pid>0 and msg required")
                ok = _send_bless(pid, msg)
                return _ok(self, {"sent": ok})

            return _err(self, "not found", 404)

        except Exception as e:
            return _err(self, f"server error: {e}", 500)

def main():
    host = os.environ.get("API_HOST","0.0.0.0")
    port = int(os.environ.get("API_PORT","8080"))
    httpd = ThreadingHTTPServer((host, port), Api)
    print(f"[sched_api] listen http://{host}:{port}  master={MASTER}  auth={'on' if API_KEY else 'off'}  root={ROOT}")
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()

if __name__ == "__main__":
    main()
