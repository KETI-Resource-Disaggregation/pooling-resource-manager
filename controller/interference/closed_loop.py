#!/usr/bin/env python3
"""[Exp_89] 폐루프 간섭 제어 데몬 (관측 → 판단 → 조절).

설계 원칙:
  - 평상시 = 제어 없음(aggressor release = off 거동, 처리량 손실 0).
  - victim p95 지연이 baseline×k_intervene 를 넘을 때만 aggressor 를 단계적으로 조인다.
  - work-conserving: 조이는 것은 aggressor 크레딧 스로틀이나, "간섭 감지 시에만" 조이므로
    victim 이 slack 을 소비 → GPU 유휴 없음(WC-in-effect). NONE 에선 aggressor 무제한.
  - 히스테리시스: k_intervene(조임) > k_release(풂), 해제는 release_hold 초 안정 후에만 → 진동 방지.
  - 안전 기본값: victim 관측 부재/stale → 보수적 단계로 하강(WC 유지).

센서: SYNC/obs_<name>.json (worker PRISM_OBS=1 리포터, 롤링 p95). 파일 기반(네트워킹 불요).
액추에이터: controller :8090 /feeder/{arm,release,ratios} (런타임 가변, 10ms 틱 반영).
  ★Go 오토-와이어러(bless_feeder.go)는 델타 트리거(LSU/조합 변경 시에만) → 안정 파드셋에선 충돌 없음.

on/off = 이 데몬 기동/종료(롤백). 라이브 광고·플러그인 무수정.
"""
import argparse, glob, json, os, time, urllib.request


def _post(url, body):
    data = json.dumps(body).encode()
    req = urllib.request.Request(url, data=data,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=2) as r:
            return r.status < 300
    except Exception as e:
        print(f"[loop] POST 실패 {url}: {e}", flush=True)
        return False


def emit_event(reason, msg, warn=False):
    """K8s Event 발행(오케스트로 A-4). 실패는 무시(측정 방해 금지)."""
    ev = {
        "apiVersion": "v1", "kind": "Event",
        "metadata": {"generateName": "prism-loop-"},
        "involvedObject": {"kind": "Node", "name": "gpu-npu-server-02"},
        "reason": reason, "message": msg,
        "type": "Warning" if warn else "Normal",
        "source": {"component": "prism-closed-loop"},
    }
    import subprocess
    try:
        subprocess.run(["kubectl", "create", "-f", "-"], input=json.dumps(ev).encode(),
                       timeout=5, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except Exception:
        pass


def read_obs(sync_dir, stale_s):
    """obs_*.json 을 읽어 victim(memory)/aggressor(compute) 로 분류. 신선한 것만."""
    now = time.time()
    vic, agg = None, None
    for p in glob.glob(os.path.join(sync_dir, "obs_*.json")):
        try:
            with open(p) as f:
                o = json.load(f)
        except Exception:
            continue
        if now - o.get("ts", 0) > stale_s:
            continue
        if o.get("class") == "memory":
            vic = o
        elif o.get("class") == "compute":
            agg = o
    return vic, agg


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sync-dir", required=True)
    ap.add_argument("--feeder-url", default="http://localhost:8090")
    ap.add_argument("--victim-p95-base", type=float, required=True,
                    help="victim solo p95(ms) 기준선 — 유지율·개입 임계 계산의 분모")
    ap.add_argument("--interval", type=float, default=0.5)
    ap.add_argument("--k-intervene", type=float, default=1.5)   # baseline×이 값 초과 → 조임
    ap.add_argument("--k-release", type=float, default=1.2)     # baseline×이 값 미만 → 풂 후보
    ap.add_argument("--intervene-hold", type=float, default=2.0,
                    help="조이기 전 초과가 지속돼야 하는 시간(s) — 롤링 p95 순간 스파이크 무시")
    ap.add_argument("--release-hold", type=float, default=3.0)  # 풀기 전 안정 유지 시간(s)
    ap.add_argument("--min-dwell", type=float, default=1.0)     # 상태 전환 후 최소 체류(s)
    ap.add_argument("--steps", default="1.0,0.7,0.5,0.4")       # ratio 사다리(0=NONE/release)
    ap.add_argument("--stale", type=float, default=3.0)
    ap.add_argument("--safe-level", type=int, default=1)        # 관측 부재 시 보수 단계
    ap.add_argument("--log", default="/root/exp89_loop.log")
    ap.add_argument("--tag", default="")
    ap.add_argument("--emit-events", action="store_true")
    ap.add_argument("--duration", type=float, default=0, help=">0 이면 그 초 뒤 자동 종료")
    a = ap.parse_args()

    steps = [float(x) for x in a.steps.split(",")]
    base = a.victim_p95_base
    lg = open(a.log, "a")
    def log(*x):
        line = "\t".join(str(i) for i in x)
        lg.write(line + "\n"); lg.flush()
        print("[loop] " + line, flush=True)

    log(f"# START tag={a.tag} base_p95={base}ms k_int={a.k_intervene} k_rel={a.k_release} "
        f"hold={a.release_hold} steps={steps} sync={a.sync_dir}")

    level = 0                 # 0 = NONE(release)
    last_change = 0.0
    below_since = None
    above_since = None        # p95 가 개입 임계 초과로 지속된 시각(스파이크 무시용)
    transitions = 0
    last_agg = None
    t_start = time.time()

    def actuate(lvl, agg_tenant, reason):
        if lvl == 0:
            _post(f"{a.feeder_url}/feeder/release", {"tenant": agg_tenant})
        else:
            _post(f"{a.feeder_url}/feeder/arm", {"tenant": agg_tenant})
            _post(f"{a.feeder_url}/feeder/ratios",
                  {"ratios": {agg_tenant: steps[lvl]}, "reason": reason})

    try:
        while True:
            time.sleep(a.interval)
            if a.duration and time.time() - t_start > a.duration:
                break
            now = time.time()
            vic, agg = read_obs(a.sync_dir, a.stale)
            newly = False
            if agg:
                t = agg.get("tenant")
                newly = (t != last_agg)
                last_agg = t
            if not last_agg:
                continue  # aggressor 미등장 — 대상 없음
            # 초기화: aggressor 를 능동 release → "평상시=제어없음=off"(Go 와이어러 arm 무효화).
            # 이게 없으면 미개입 상태에서도 와이어러의 고정 arm 이 남아 손실이 생긴다.
            if newly and level == 0:
                actuate(0, last_agg, "init-release(NONE=off)")
                log(f"{now:.2f}", "-", "-", 0, steps[0], transitions, "INIT_RELEASE")

            # 안전 기본값: victim 관측 부재/stale → 보수 단계로(WC 유지)
            if not vic or vic.get("p95_ms") is None:
                if level != a.safe_level and (now - last_change) >= a.min_dwell:
                    old = level; level = a.safe_level; last_change = now; transitions += 1
                    actuate(level, last_agg, f"safe-fallback(no-obs) {old}->{level}")
                    log(f"{now:.2f}", "NA", "NA", level, steps[level], transitions, "SAFE_FALLBACK")
                continue

            p95 = vic["p95_ms"]
            r = p95 / base
            act = "hold"
            # 개입: 임계 초과가 intervene_hold 초 이상 지속돼야 조임(순간 스파이크 무시)
            if r > a.k_intervene:
                if above_since is None:
                    above_since = now
            else:
                above_since = None
            sustained = above_since is not None and (now - above_since) >= a.intervene_hold
            if sustained and level < len(steps) - 1 and (now - last_change) >= a.min_dwell:
                old = level; level += 1; last_change = now; below_since = None
                above_since = now; transitions += 1        # 다음 단계도 재확인 요구
                actuate(level, last_agg, f"intervene r={r:.2f} {old}->{level}")
                act = "TIGHTEN"
                if a.emit_events and old == 0:
                    emit_event("InterferenceDetected",
                               f"victim p95 {p95:.1f}ms = {r:.2f}x baseline — tighten aggressor to {steps[level]}", warn=True)
                if a.emit_events:
                    emit_event("ControlModeSwitched", f"level {old}->{level} ratio={steps[level]}")
            elif r < a.k_release:
                if below_since is None:
                    below_since = now
                elif (now - below_since) >= a.release_hold and level > 0 and (now - last_change) >= a.min_dwell:
                    old = level; level -= 1; last_change = now; below_since = now; transitions += 1
                    actuate(level, last_agg, f"release r={r:.2f} {old}->{level}")
                    act = "LOOSEN"
                    if a.emit_events:
                        emit_event("ControlModeSwitched", f"level {old}->{level} ratio={steps[level]}")
            else:
                below_since = None  # 중간 구간 = 유지(진동 방지 데드밴드)

            log(f"{now:.2f}", f"{p95:.1f}", f"{r:.2f}", level, steps[level], transitions, act)
    finally:
        # 종료 시 aggressor 원복(release) — 다음 실험 오염 방지
        if last_agg:
            _post(f"{a.feeder_url}/feeder/release", {"tenant": last_agg})
        log(f"# END transitions={transitions}")
        lg.close()


if __name__ == "__main__":
    main()
