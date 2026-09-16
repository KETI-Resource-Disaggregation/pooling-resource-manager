#!/usr/bin/env python3
"""[Exp_138 2부] Deep Scanner 관측 분류를 ③ 게이트 배치에 잇는 스캐너.

tracer 가 테넌트 디렉터리에 남기는 trace.<pid>.txt 를 주기 폴링하고, **표본 충분성
게이트**를 통과한 것만 분류해 같은 자리에 `obs_class` 로 쓴다. Go 와이어러
(bless_feeder.go)가 `class`(선언)와 나란히 이 파일을 읽는다.

★기본 꺼짐. 이 프로세스를 띄우지 않으면 `obs_class` 가 생기지 않고, 와이어러는
  선언만 보는 기존 동작 그대로다.

게이트 (Exp_138 1-A 실측 유도 — 임의 상수 아님):
  경과 >= GATE_S 초 AND 누적 커널 >= GATE_KERNELS 개.
  · GATE_S 3.0 — 하네스 5종 중 가장 늦은 안정 시점(decode). decode 는 표본 부족이
    아니라 **실제 국면 전이**(머리의 prefill)라 커널을 더 모아도 해결되지 않는다.
    2.0s 까지 COMPUTE_BOUND 를 HIGH 로 낸다.
  · GATE_KERNELS 180 — 관측된 오판의 최대 커널 수는 15, 정답에 닿은 최소 커널 수는
    180(prefill). 알려진 오판을 전부 막으면서 알려진 정답을 하나도 버리지 않는 하한.
  · 두 축이 다 필요하다: 커널만 쓰면 decode 기준(52,389)에서 prefill 이 37초를
    기다리고, 시간만 쓰면 발사율이 낮은 워크로드가 커널 몇 개로 통과한다.

게이트 통과 전에는 `obs_class` 를 쓰지 않는다 = 와이어러에게 UNKNOWN 과 같다.
"""
import argparse
import glob
import json
import os
import sys
import time

GATE_S = 3.0
GATE_KERNELS = 180

sys.path.insert(0, os.environ.get("KRAKEN_ROOT", "/opt/kraken"))
try:
    from profiler.kernel_class.classify import summarize
except Exception:
    sys.path.insert(0, os.path.abspath("."))
    from profiler.kernel_class.classify import summarize


def trace_stats(path):
    """(커널수, 경과초) — 전량 파싱 없이 첫/마지막 타임스탬프만 본다."""
    n = 0
    t0 = None
    t1 = None
    with open(path, "rb") as f:
        for line in f:
            if not line.startswith(b"K|"):
                continue
            n += 1
            try:
                ts = int(line.split(b"|", 3)[1])
            except Exception:
                continue
            if t0 is None:
                t0 = ts
            t1 = ts
    if t0 is None or t1 is None:
        return n, 0.0
    return n, (t1 - t0) / 1e9


def newest_trace(d):
    """워크로드 프로세스의 트레이스 = 가장 큰 파일 (Exp_131 3-3: LD_PRELOAD 가
    컨테이너의 모든 프로세스에 걸려 bash/cat 것은 0 B 로 남는다)."""
    best, bt = None, -1
    for p in glob.glob(os.path.join(d, "trace.*.txt")):
        try:
            sz = os.stat(p).st_size
        except OSError:
            continue
        if sz > bt:
            bt, best = sz, p
    return best


def scan_once(sockdir, log):
    n_written = 0
    for d in sorted(glob.glob(os.path.join(sockdir, "*"))):
        if not os.path.isdir(d):
            continue
        tenant = os.path.basename(d)
        tr = newest_trace(d)
        if tr is None:
            continue
        meta_p = os.path.join(d, "obs_meta.json")
        prev = {}
        if os.path.exists(meta_p):
            try:
                prev = json.load(open(meta_p))
            except Exception:
                prev = {}
        n, el = trace_stats(tr)
        if el < GATE_S or n < GATE_KERNELS:
            # [T-5] 조용히 넘어가지 않는다 — 왜 아직 안 나오는지 남긴다
            if prev.get("gate") != "waiting" or prev.get("n_kernels") != n:
                json.dump({"gate": "waiting", "n_kernels": n,
                           "elapsed_s": round(el, 3),
                           "need": {"s": GATE_S, "kernels": GATE_KERNELS},
                           "ts": round(time.time(), 3)},
                          open(meta_p, "w"), ensure_ascii=False)
            continue
        try:
            r = summarize(tr, tenant, workload_type=None)
        except Exception as e:
            json.dump({"gate": "error", "err": "%s: %s" % (type(e).__name__, e),
                       "ts": round(time.time(), 3)},
                      open(meta_p, "w"), ensure_ascii=False)
            continue
        cls = r["classification"]
        if prev.get("gate") == "passed" and prev.get("classification") == cls:
            continue
        json.dump({"gate": "passed", "classification": cls,
                   "confidence": r["confidence"],
                   "cm": r["compute_memory_ratio"],
                   "n_kernels": n, "elapsed_s": round(el, 3),
                   "ts": round(time.time(), 3)},
                  open(meta_p, "w"), ensure_ascii=False)
        tmp = os.path.join(d, ".obs_class.tmp")
        with open(tmp, "w") as f:
            f.write(cls)
        os.replace(tmp, os.path.join(d, "obs_class"))   # 원자적 교체
        log("%.2f\t%s\t%s\tconf=%s\tcm=%s\tn=%d\tel=%.2f"
            % (time.time(), tenant, cls, r["confidence"],
               r["compute_memory_ratio"], n, el))
        n_written += 1
    return n_written


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sock-dir", default="/var/lib/kraken/socks")
    ap.add_argument("--interval", type=float, default=2.0)
    ap.add_argument("--log", default=os.environ.get(
        "KRAKEN_SCAN_LOG", "/tmp/kraken_scan_loop.log"))
    ap.add_argument("--duration", type=float, default=0)
    a = ap.parse_args()
    lg = open(a.log, "a")

    def log(line):
        lg.write(line + "\n")
        lg.flush()
        print("[scan] " + line, flush=True)

    log("# START sock=%s gate=%ss/%dkernels" % (a.sock_dir, GATE_S, GATE_KERNELS))
    t0 = time.time()
    try:
        while True:
            scan_once(a.sock_dir, log)
            if a.duration and time.time() - t0 > a.duration:
                break
            time.sleep(a.interval)
    finally:
        log("# END")
        lg.close()


if __name__ == "__main__":
    main()
