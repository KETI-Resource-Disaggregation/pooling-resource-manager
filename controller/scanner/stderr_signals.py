"""[Exp_126 4부] libbless stderr 신호 수집 — K8s 로그 API 경유.

★왜 필요한가 (Exp_125 §3-A). libbless 는 통계를 두 곳에 낸다:
    feed.log  → total/kernels, gate_*, qmd_*      (컨트롤러가 읽을 수 있음)
    stderr    → mem/ctx/queue/syscall/sm + time_stats 의 avg_kernel·time_credit
  stderr 은 워크로드 **컨테이너 안**이라 지금까지 읽을 방법이 없었다. 그런데
  컨테이너 런타임이 stderr 을 잡으므로 **K8s 로그 API로 읽으면 libbless 를
  고치지 않고 확보된다.** (libbless 는 Exp_85 이후 무수정 계보 — 유지한다.)

경로:
  ① 소켓에 통계 명령 발화 (mem_stats/ctx_stats/... — libbless 가 stderr 에 1줄 출력)
  ② watcher 가 선기록한 <sockDir>/<devID>/podref 로 파드를 찾는다 (Exp_126 4-B)
  ③ GET /api/v1/namespaces/<ns>/pods/<name>/log?container=..&tailLines=N 파싱

★한계 (숨기지 않는다):
  - **주기 관측용이다. 고빈도 계측용이 아니다.** 매 호출이 API 왕복 + 로그 tail 이라
    비용이 feed.log 읽기와 자릿수가 다르다
  - 로그 회전(kubelet containerLogMaxSize)에 걸리면 오래된 줄이 사라진다.
    발화 직후 읽으므로 실무상 문제는 없으나 **지연되면 놓친다**
  - 파드당 스트림이므로 테넌트 수에 선형 비례한다
"""
import json
import os
import re
import socket
import ssl
import time
import urllib.request

SA = "/var/run/secrets/kubernetes.io/serviceaccount"
# 발화 → 로그 반영 대기. 컨테이너 런타임이 stderr 을 파일로 넘기는 시간.
#   libbless 는 각 통계 줄마다 fflush 하지 않으므로(stderr 은 기본 unbuffered 이나
#   런타임 경유 지연이 있다) 최소 대기를 둔다. 값은 feeder 의 occupancy 정착
#   대기(0.1s)와 같은 자릿수로 잡았다 — 새 상수를 만들지 않는다.
SETTLE_S = float(os.environ.get("KRAKEN_STDERR_SETTLE_S", "0.3"))
TAIL_LINES = int(os.environ.get("KRAKEN_STDERR_TAIL", "40"))

_WARNED = set()


def _warn(key, msg):
    """[Exp_107 T-5] 조용한 폴백 금지 — 같은 사유는 1회만."""
    if key not in _WARNED:
        _WARNED.add(key)
        print(f"[scanner][경고] {msg}", flush=True)


# libbless stderr 한 줄 → (kind, {k: v})
#   형식은 libbless.cpp 의 fprintf 그대로. 값은 전부 `k=v` 공백 구분.
_LINE = re.compile(r"^\[libbless\]\s+(\w+):\s+(.*)$")
# ★time_stats 만 형식이 다르다 — `kind:` 접두 없이 바로 k=v 가 온다
#   ("[libbless] mode=1 time_credit=... avg_kernel=..."). libbless.cpp:1023 확인.
#   형식을 맞추려 libbless 를 고치지 않는다(무수정 계보) — 파서가 흡수한다.
_BARE = re.compile(r"^\[libbless\]\s+(mode=.*)$")
_KV = re.compile(r"([A-Za-z_][A-Za-z0-9_]*)=(-?[\d.]+|\S+)")


def parse_libbless_line(line):
    """`[libbless] mem: alloc_count=3 ... quota_MB=0.00` → ("mem", {...}).

    숫자는 int/float 으로, 나머지는 문자열로 둔다. 해석 실패는 None —
    호출자가 경고를 담당한다(값을 만들지 않는다)."""
    t = line.strip()
    m = _LINE.match(t)
    if m:
        kind, rest = m.group(1), m.group(2)
    else:
        b = _BARE.match(t)
        if not b:
            return None
        kind, rest = "time", b.group(1)
    out = {}
    for k, v in _KV.findall(rest):
        try:
            out[k] = int(v)
        except ValueError:
            try:
                out[k] = float(v)
            except ValueError:
                out[k] = v
    return (kind, out) if out else None


def read_podref(sock_dir, dev_id):
    """watcher 선기록 → (ns, name, container). 없으면 None."""
    try:
        with open(os.path.join(sock_dir, dev_id, "podref")) as f:
            parts = f.read().strip().split("/")
    except OSError:
        return None
    return tuple(parts) if len(parts) == 3 else None


class StderrSignals:
    """소켓 발화 + 파드 로그 파싱. 기본 꺼짐(opt-in)."""

    # libbless 가 stderr 에만 내는 통계 명령 (Exp_125 §3-A 인벤토리)
    STDERR_ONLY = ("mem_stats", "ctx_stats", "queue_stats", "sm_stats")
    # time_stats 는 feed.log 에도 나가지만 stderr 줄에만 avg_kernel·time_credit 이 있다
    BOTH = ("time_stats",)

    def __init__(self, sock_dir, send_fn, api=None, token=None, sleep_fn=time.sleep):
        self.sock_dir = sock_dir
        self._send = send_fn
        self._sleep = sleep_fn
        self.api = api or ("https://" + os.environ.get("KUBERNETES_SERVICE_HOST", "")
                           + ":" + os.environ.get("KUBERNETES_SERVICE_PORT", "443"))
        self.token = token
        if self.token is None:
            try:
                self.token = open(os.path.join(SA, "token")).read().strip()
            except OSError:
                self.token = ""
        self._ctx = ssl.create_default_context(cafile=os.path.join(SA, "ca.crt")) \
            if os.path.exists(os.path.join(SA, "ca.crt")) else None

    # ---- K8s 로그 ----
    def pod_log(self, ns, name, container, tail=TAIL_LINES):
        if not self.token:
            _warn("no_token", "ServiceAccount 토큰을 읽지 못했다 — "
                              "stderr 신호 수집이 비활성된다")
            return None
        url = (f"{self.api}/api/v1/namespaces/{ns}/pods/{name}/log"
               f"?container={container}&tailLines={tail}")
        req = urllib.request.Request(url, headers={
            "Authorization": "Bearer " + self.token})
        try:
            with urllib.request.urlopen(req, timeout=5, context=self._ctx) as r:
                return r.read().decode("utf-8", "replace")
        except Exception as e:
            _warn(f"log_{ns}/{name}",
                  f"파드 로그 조회 실패 {ns}/{name}: {e} — "
                  f"RBAC(pods/log get) 또는 파드 상태를 확인할 것")
            return None

    # ---- 수집 ----
    def collect(self, dev_id, sock_path, kinds=None):
        """한 테넌트의 stderr 신호를 한 번 수집. 실패는 None + 경고."""
        ref = read_podref(self.sock_dir, dev_id)
        if ref is None:
            _warn(f"podref_{dev_id}",
                  f"{dev_id}: podref 선기록이 없다 — watcher 가 아직 안 썼거나 "
                  f"우리 리소스를 요청하지 않은 파드다. 이 테넌트는 건너뛴다")
            return None
        ns, name, container = ref
        cmds = list(kinds) if kinds else list(self.STDERR_ONLY) + list(self.BOTH)
        for c in cmds:
            self._send(sock_path, c)
        self._sleep(SETTLE_S)
        txt = self.pod_log(ns, name, container)
        if txt is None:
            return None
        out = {}
        for line in txt.splitlines():
            p = parse_libbless_line(line)
            if p:
                out[p[0]] = p[1]          # 같은 종류는 마지막 줄이 최신
        if not out:
            _warn(f"parse_{dev_id}",
                  f"{dev_id}: 파드 로그 {len(txt.splitlines())}줄에서 libbless 통계 "
                  f"줄을 찾지 못했다 — 발화 지연/로그 회전/형식 변경 중 하나다")
            return None
        out["_pod"] = f"{ns}/{name}/{container}"
        return out

    # ---- 4-C: EMA vs 누적평균 ----
    @staticmethod
    def kernel_size_signal(stderr_stats, feed_total_us, feed_kernels):
        """[Exp_126 4-C] 커널 크기 분포의 **대용 신호**.

        avg_kernel(EMA, stderr) 과 total/kernels(누적 평균, feed.log)의 비.
          > 1  최근 커널이 전체 평균보다 크다
          < 1  최근이 더 작다
        ★완전한 분포가 아니다. 분산이 아니라 '최근 대 전체'의 이동만 본다.
          libbless 에 분포 정보가 전무하다는 사실(Exp_125 §3-B)은 그대로다.
        """
        st = stderr_stats or {}
        ema = st.get("time", {}).get("avg_kernel") if "time" in st \
            else st.get("avg_kernel")
        if ema is None or not feed_kernels:
            return None
        cum = feed_total_us / feed_kernels
        if cum <= 0:
            return None
        return {"avg_kernel_ema_us": ema, "avg_kernel_cum_us": round(cum, 2),
                "ratio_ema_over_cum": round(ema / cum, 4)}
