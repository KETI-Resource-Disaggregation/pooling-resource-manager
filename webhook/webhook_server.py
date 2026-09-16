#!/usr/bin/env python3
# KETI(우리) 프로덕션 컴포넌트 — 대체 대상 아님.
# [Exp_137] integration_demo/keti_bridge/ 에서 이리로 옮겼다. 데모 스텁이 아니라
#   제어 성립 조건을 지키는 상시 구성요소이며, 파드로 상주한다(호스트 nohup 아님).
# A-3 규격: 파드 annotation(kraken.keti.re.kr/workload-class 등) → 컨테이너 env(KRAKEN_WORKLOAD_CLASS) 변환.
# 고려대는 annotation만 달면 되고, env 주입은 KETI가 이 webhook으로 담당한다.
#
# 안전: 미지정 annotation은 아무것도 주입하지 않는다(기본값 발명 금지 — 제어계층의 대칭 기본이 그대로).
import base64, json, os, ssl, time
from http.server import BaseHTTPRequestHandler, HTTPServer

CERT = os.environ.get("KRAKEN_WH_CERT", "/etc/kraken-webhook/tls.crt")
KEY = os.environ.get("KRAKEN_WH_KEY", "/etc/kraken-webhook/tls.key")
ANN = "kraken.keti.re.kr/"
# [Exp_135 4부] env 충돌 방어 — 파드가 자기 LD_PRELOAD/PYTHONPATH 를 쓰면
#   device plugin Allocate 의 주입이 **통째로 밀린다**(파드 spec 이 이긴다, 실측).
#   그러면 libbless 가 안 붙어 시간 몫 제어·메모리 쿼터·class 선언이 사라진다.
#   Allocate 는 파드 env 를 **읽을 수 없지만** webhook 은 읽을 수 있다 → 여기서 잇는다.
#   ★순서는 우리 것을 앞에 둔다 — Exp_135 3부 실측 근거:
#     남의 .so 가 libcudart 를 직접 잡아 체인을 끊어도(o2) 우리가 앞이면 제어가 산다.
#     우리가 뒤면(o4) libbless 는 적재되고 로그도 정상인데 후킹이 안 걸린다.
LSU_RES = "keti.re.kr/lsu"
PREPEND = {
    "LD_PRELOAD": os.environ.get("KRAKEN_WH_PRELOAD",
                                 "/var/lib/kraken/lib/libbless.so"),
    # sitecustomize 는 BLESS_FAST_TEARDOWN=1 일 때만 동작하므로, 빠른 종료가
    # 꺼져 있어도 이 경로를 앞에 붙이는 것은 무해하다(찾을 파일이 없거나 무동작).
    "PYTHONPATH": os.environ.get("KRAKEN_WH_PYPATH", "/var/lib/kraken/py"),
}


def wants_lsu(c):
    """LSU 를 요청하는 컨테이너만 대상 — 나머지는 우리 제어와 무관하다."""
    lim = ((c.get("resources") or {}).get("limits") or {})
    req = ((c.get("resources") or {}).get("requests") or {})
    return LSU_RES in lim or LSU_RES in req


def env_patches(c, i):
    """[Exp_135] 이미 설정된 LD_PRELOAD/PYTHONPATH 앞에 우리 값을 잇는다.

    · 파드가 그 변수를 **안 쓰면** 아무것도 하지 않는다 — Allocate 가 넣으면 된다.
      (webhook 이 굳이 손대면 Allocate 의 기능 조합(tracer·빠른 종료)을 덮어쓴다)
    · 이미 우리 값이 들어 있으면 중복해 붙이지 않는다(재적용 안전).
    · valueFrom 으로 온 값은 문자열이 아니므로 건드리지 않는다 — 조용히 틀리게
      바꾸느니 그대로 두고 5부 탐지가 잡게 한다.
    """
    out = []
    for j, e in enumerate(c.get("env") or []):
        name = e.get("name")
        if name not in PREPEND or "value" not in e:
            continue
        ours = PREPEND[name]
        cur = e.get("value") or ""
        if not cur or ours in cur.split(":"):
            continue
        out.append({"op": "replace",
                    "path": f"/spec/containers/{i}/env/{j}/value",
                    "value": f"{ours}:{cur}"})
    return out
# A-3 pipeline-stage → workload-class 매핑 (class 명시 없을 때만)
STAGE_MAP = {"prefill": "compute", "decode": "memory"}


def resolve_class(ann):
    """A-3 규격 그대로. class 명시 우선, 없으면 stage 매핑, 둘 다 없으면 None(무주입)."""
    c = ann.get(ANN + "workload-class")
    if c in ("compute", "memory"):
        return c
    stage = ann.get(ANN + "pipeline-stage")
    return STAGE_MAP.get(stage)


def build_patch(pod):
    ann = (pod.get("metadata") or {}).get("annotations") or {}
    patches = []
    # [Exp_135 4부] env 충돌 방어 — annotation 유무와 **무관하게** 먼저 처리한다.
    #   A-3 규격(annotation→env)은 선택 기능이지만 이쪽은 제어 성립 조건이다.
    for i, c in enumerate(pod.get("spec", {}).get("containers", [])):
        if wants_lsu(c):
            patches.extend(env_patches(c, i))
    wclass = resolve_class(ann)
    if wclass is None:
        return patches                 # 미지정 → class 무주입(안전 기본값 유지)
    prio = ann.get(ANN + "priority")
    for i, c in enumerate(pod["spec"]["containers"]):
        envs = c.get("env")
        base = f"/spec/containers/{i}/env"
        # 이미 KRAKEN_WORKLOAD_CLASS env가 있으면 건너뜀(고려대가 직접 세팅한 경우 존중)
        if any(e.get("name") == "KRAKEN_WORKLOAD_CLASS" for e in (envs or [])):
            continue
        if envs is None:
            patches.append({"op": "add", "path": base, "value": []})
        patches.append({"op": "add", "path": base + "/-",
                        "value": {"name": "KRAKEN_WORKLOAD_CLASS", "value": wclass}})
        if prio:
            patches.append({"op": "add", "path": base + "/-",
                            "value": {"name": "KRAKEN_PRIORITY", "value": prio}})
    return patches


# [Exp_137 2-C] 상태 확인 — "파드가 Running" 이 "webhook 이 동작한다" 를 뜻하지 않는다.
#   실제로 심사를 받고 패치를 냈는지 세어 /healthz 로 낸다. 기동 시각을 함께 내므로
#   재시작 여부도 밖에서 보인다.
_STAT = {"started": time.time(), "reviews": 0, "patched": 0, "errors": 0}


class H(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path.split("?")[0].rstrip("/") not in ("/healthz", "/health"):
            self.send_response(404); self.end_headers(); return
        body = json.dumps({"ok": True, "uptime_s": round(time.time() - _STAT["started"], 1),
                           **{k: v for k, v in _STAT.items() if k != "started"}}).encode()
        self.send_response(200); self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body))); self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        if os.environ.get("KRAKEN_WH_REQLOG"):
            with open(os.environ["KRAKEN_WH_REQLOG"], "a") as lf:
                lf.write(f"POST path={self.path}\n")
        if self.path.split("?")[0].rstrip("/") != "/mutate":
            self.send_response(404); self.end_headers(); return
        n = int(self.headers.get("Content-Length", 0))
        review = json.loads(self.rfile.read(n) or b"{}")
        req = review.get("request", {})
        uid = req.get("uid", "")
        if os.environ.get("KRAKEN_WH_REQLOG"):
            with open(os.environ["KRAKEN_WH_REQLOG"], "a") as lf:
                obj = req.get("object", {})
                lf.write(f"REQ uid={uid} ns={req.get('namespace')} name={(obj.get('metadata') or {}).get('name')} ann={(obj.get('metadata') or {}).get('annotations')}\n")
        resp = {"uid": uid, "allowed": True}
        _STAT["reviews"] += 1
        try:
            patches = build_patch(req.get("object", {}))
            if patches:
                resp["patchType"] = "JSONPatch"
                resp["patch"] = base64.b64encode(json.dumps(patches).encode()).decode()
                _STAT["patched"] += 1
        except Exception:
            _STAT["errors"] += 1
            pass                        # 어떤 오류든 allowed=True 무패치(파드 생성 막지 않음)
        out = json.dumps({"apiVersion": "admission.k8s.io/v1", "kind": "AdmissionReview",
                          "response": resp}).encode()
        self.send_response(200); self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(out))); self.end_headers(); self.wfile.write(out)

    def log_message(self, *a):
        pass


if __name__ == "__main__":
    port = int(os.environ.get("KRAKEN_WH_PORT", "8443"))
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(CERT, KEY)
    srv = HTTPServer(("0.0.0.0", port), H)
    srv.socket = ctx.wrap_socket(srv.socket, server_side=True)
    print(f"[kraken-webhook] build={os.environ.get('KRAKEN_BUILD_STAMP', 'unstamped')}", flush=True)
    print(f"[kraken-webhook] https://0.0.0.0:{port}/mutate  ·  /healthz", flush=True)
    srv.serve_forever()
