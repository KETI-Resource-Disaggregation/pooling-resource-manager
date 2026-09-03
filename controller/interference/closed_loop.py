#!/usr/bin/env python3
"""[Exp_89] 폐루프 간섭 제어 데몬 (관측 → 판단 → 조절).

설계 원칙:
  - 평상시 = 제어 없음(aggressor release = off 거동, 처리량 손실 0).
  - victim p95 지연이 baseline×k_intervene 를 넘을 때만 aggressor 를 단계적으로 조인다.
  - work-conserving: 조이는 것은 aggressor 크레딧 스로틀이나, "간섭 감지 시에만" 조이므로
    victim 이 slack 을 소비 → GPU 유휴 없음(WC-in-effect). NONE 에선 aggressor 무제한.
  - 히스테리시스: k_intervene(조임) > k_release(풂), 해제는 release_hold 초 안정 후에만 → 진동 방지.
  - 안전 기본값: victim 관측 부재/stale → 보수적 단계로 하강(WC 유지).

센서: SYNC/obs_<name>.json (worker KRAKEN_OBS=1 리포터, 롤링 p95). 파일 기반(네트워킹 불요).
액추에이터: controller :8090 /feeder/{arm,release,ratios} (런타임 가변, 10ms 틱 반영).
  ★Go 오토-와이어러(bless_feeder.go)는 델타 트리거(LSU/조합 변경 시에만) → 안정 파드셋에선 충돌 없음.

on/off = 이 데몬 기동/종료(롤백). 라이브 광고·플러그인 무수정.
"""
import argparse, glob, json, os, time, urllib.request


_EV_WARNED = False


# [Exp_116 2부] 대칭 판정 임계 — ★임의 상수가 아니라 실측 분포에서 세웠다.
#   대칭 사례(양쪽이 똑같이 아픈 상황) p95 비: 1.000 / 1.005 / 1.007  (Exp_115 3부 n=3)
#   비대칭 사례(한쪽만 아픈 상황)   p95 비: 2.196 / 2.214            (Exp_113 M-8, Exp_114 3부)
#   두 군 사이(1.007 ~ 2.196)가 완전히 비어 있다. 기하 중간점 1.487 → 1.5 로 둔다.
#   ★의미: 양쪽이 비슷하게 아프면 **조일 대상이 없다**. 한쪽을 조여도 손실 이전이지
#     해소가 아니다(Exp_115 3-C 실측: 총합 -16.7%, 서빙 측 -25.0%).
#     자원 종류(대역폭이냐 SM이냐)를 알 필요가 없다는 것이 이 신호의 장점이다.
SYMMETRY_RATIO = 1.5

# [Exp_116 2부] 대칭일 때의 중간안 — 완전 생략이 아니다.
#   3-C 에서 1단계가 **적극적으로 해를 끼쳤으므로** 오탐 비용이 낮지 않다.
#   한 칸만 내려보고 개선이 없으면 즉시 3단계로 승격한다(6초 → 2초).
SYM_TRY_STEPS = 1          # 대칭 판정 시 내려볼 사다리 칸 수
SYM_IMPROVE = 0.05         # '개선' 기준: victim p95 가 5% 이상 낮아져야 한다.
#   근거: Exp_115 3부의 회차 간 p95 편차가 83.5~84.9ms(1.7%)였다. 잡음의 약 3배를
#   개선 문턱으로 두면 우연한 변동을 개선으로 오판하지 않는다.

_WARNED = set()


def _warn_once(key, msg):
    """[Exp_107 T-5] 조용한 폴백 금지 — 같은 사유는 1회만."""
    if key not in _WARNED:
        _WARNED.add(key)
        print(f"[loop][경고] {msg}", flush=True)


def _get(url, timeout=2):
    """[Exp_113 M-8] GET 헬퍼. ★없어서 2단계(actuate_space→_sock_of)가
    `NameError: name '_get' is not defined` 로 죽었다 — 1단계 사다리를 끝까지 내린 뒤
    2단계를 시도하는 순간 폐루프 전체가 종료되어 **3단계(재배치 추천)에 영원히
    도달하지 못했다**. Exp_109 Q-3 에서 2단계를 얹을 때 빠뜨린 것이다."""
    try:
        with urllib.request.urlopen(url, timeout=timeout) as r:
            return json.load(r)
    except Exception as e:
        print(f"[loop] GET 실패 {url}: {e}", flush=True)
        return None


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
        "metadata": {"generateName": "kraken-loop-"},
        "involvedObject": {"kind": "Node", "name": "gpu-npu-server-02"},
        "reason": reason, "message": msg,
        "type": "Warning" if warn else "Normal",
        "source": {"component": "kraken-closed-loop"},
    }
    import subprocess
    try:
        subprocess.run(["kubectl", "create", "-f", "-"], input=json.dumps(ev).encode(),
                       timeout=5, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except Exception as e:
        # [Exp_107 T-5] 이벤트 발행 실패는 제어 자체를 막지 않으나, 조용하면
        #   "제어가 돈 흔적이 없다"로 오인된다. 1회만 알린다.
        global _EV_WARNED
        if not _EV_WARNED:
            _EV_WARNED = True
            print(f"[loop][경고] K8s 이벤트 발행 실패 err={e!r} → 제어는 계속하되 "
                  f"이벤트 기록이 남지 않는다.", flush=True)


def read_obs(sync_dir, stale_s, base_p95=None):
    """obs_*.json 을 읽어 victim/aggressor 로 분류. 신선한 것만.

    1차 기준은 class 라벨(memory=victim, compute=aggressor)이며 기존 동작이다.

    ★[Exp_115 1-B] class 만으로는 실제로 아픈 쪽을 못 잡는 경우가 있다.
      · 이종 라벨(memory+compute)이면 Go 와이어러의 relaxedDecision 이 memory 를
        **게이트에서 풀어준다**(armed=False). 그래서 라벨상 victim 은 오히려 자유롭고,
        실측 저하는 게이트에 묶인 compute 쪽에 몰린다(Exp_114 3부: memory 25.71 /
        compute 10.52 sps). 폐루프는 이미 자유로운 쪽을 지키러 간다.
      · 반대로 실제 간섭이 크게 나는 조합은 **동종 대역폭 쌍**인데(Exp_114 2-B: 각 ~61%),
        동종이면 class 가 같아 aggressor 를 찾지 못하고 개입 자체가 불가능하다.

      → 라벨로 쌍이 성립하지 않을 때만 **실측 저하율**로 대체 판정한다.
        base_p95 대비 p95 비가 큰 쪽을 victim, 작은 쪽을 aggressor 로 둔다.
        라벨이 정상적으로 쌍을 이루면 기존 경로 그대로다(동작 무변경).
    """
    now = time.time()
    vic, agg = None, None
    fresh = []
    for p in glob.glob(os.path.join(sync_dir, "obs_*.json")):
        try:
            with open(p) as f:
                o = json.load(f)
        except Exception:
            continue
        if now - o.get("ts", 0) > stale_s:
            continue
        fresh.append(o)
        if o.get("class") == "memory":
            vic = o
        elif o.get("class") == "compute":
            agg = o
    if vic is not None and agg is not None:
        return vic, agg
    # 라벨로 쌍이 안 서면 실측 저하율로 대체 판정 (T-5: 조용히 넘어가지 않는다)
    cand = [o for o in fresh if o.get("p95_ms")]
    if len(cand) >= 2:
        cand.sort(key=lambda o: o["p95_ms"], reverse=True)
        _warn_once("obs_fallback",
                   f"class 라벨로 victim/aggressor 쌍이 서지 않는다 "
                   f"(labels={[o.get('class') for o in fresh]}) → 실측 p95 로 대체 판정: "
                   f"victim={cand[0].get('name')} p95={cand[0]['p95_ms']:.1f}ms / "
                   f"aggressor={cand[-1].get('name')} p95={cand[-1]['p95_ms']:.1f}ms")
        return cand[0], cand[-1]
    return vic, agg


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sync-dir", required=True)
    ap.add_argument("--feeder-url", default="http://localhost:8090")
    ap.add_argument("--victim-p95-base", type=float, default=None,
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
    ap.add_argument("--log", default=os.environ.get("KRAKEN_LOOP_LOG", "/tmp/kraken_closed_loop.log"))
    ap.add_argument("--tag", default="")
    ap.add_argument("--emit-events", action="store_true")
    ap.add_argument("--duration", type=float, default=0, help=">0 이면 그 초 뒤 자동 종료")
    a = ap.parse_args()

    steps = [float(x) for x in a.steps.split(",")]
    # [Exp_118 A] ★기준선을 외부에서 받지 않고 **개입 전(level 0) 관측으로 스스로 잡는다.**
    #   외부 주입(--victim-p95-base)은 두 번 사고를 냈다:
    #     Exp_115 3-A — 가짜 단독(89:1)으로 기준선이 2배 부풀어 측정 전체 무효
    #     Exp_117    — 하네스가 값을 하드코딩(810.7)
    #   자기 교정으로 바꾸면 단독 측정 회차 자체가 불필요해지고 두 사고의 원인이
    #   구조적으로 사라진다. 외부 워크로드에 기준선을 미리 재둘 필요도 없다(이식성).
    #   ★한계: 폐루프가 **이미 경합이 시작된 뒤** 붙으면 기준선이 이미 저하된 값이다.
    #     붙는 시점을 제어할 수 있느냐가 갈림길이므로, 기준을 언제·얼마로 잡았는지
    #     반드시 로그에 남긴다(아래 BASE_LOCK).
    #   --victim-p95-base 를 주면 그 값을 쓴다(구 동작 호환·대조용).
    base = a.victim_p95_base
    self_base = {}          # tenant/name → 자기 기준 p95
    base_locked = [False]
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

    # ── [Exp_108 D-3] 단계 승격 ────────────────────────────────────────────
    # ★단계 순서: 계획서는 SM 재조정(공간)을 1단계, 시간 재조정을 2단계로 두었으나
    #   **현재 구현(시간 우선)을 유지하고 계획서 쪽을 고치는 것이 맞다.** 근거 둘:
    #     ① 공간 축은 런타임 재설정이 불가하다. space_mode=mps_env 는 프로세스 단위
    #        환경변수라 libbless 가 reconf_sm/set_limit_pct 를 REJECT 한다
    #        ("런타임 공간 재설정 불가 — 재스폰으로 처리할 것", libbless.cpp:995).
    #        해소 수단이 재스폰뿐이면 그것은 '해소'가 아니라 '재배치'다 → 3단계에 속한다.
    #     ② 봉투 정책 결론상 이기종 쌍의 이득은 메모리 측을 **시간 게이트에서 풀어주는**
    #        데서 나왔고 공간 상한 자체는 비용이었다. 공간을 먼저 조이는 순서는 효과 면에서도
    #        근거가 없다.
    #   → 1단계 시간 재조정 → (해소 안 되면) 2단계 SM 재조정은 **현 구조에서 불가** →
    #     3단계 재배치 추천으로 승격한다.
    #
    # 승격 기준(임의 상수 금지):
    #   PROMOTE_AFTER_S — 1단계 사다리를 끝까지 내렸는데도 저하가 지속된 시간.
    #     steps 마지막 칸까지 가는 데 min_dwell×(len(steps)-1) 이 걸리므로, 그 두 배를
    #     기다린 뒤에도 안 풀리면 시간 축으로는 못 푼다고 본다.
    PROMOTE_AFTER_S = max(4.0, a.min_dwell * (len(steps) - 1) * 2)
    promoted_at = [0.0]      # 3단계 발행 시각(재발행 억제)
    # [Exp_118 B] ★재배치 추천 발행 후에는 시간 축 조정을 멈춘다.
    #   3단계 이벤트가 `TimeAxisExhausted` 라고 명시해놓고 계속 조이는 것은 앞뒤가 안 맞고,
    #   Exp_117 에서 victim 역전(off 0.91 → on 0.79)의 유력 원인으로 지목됐다
    #   (승격 후 TIGHTEN 4회가 더 돌았다).
    #   ★상태 처리: 그 시점의 사다리 위치를 **유지**한다(원복하지 않는다).
    #     이유 — 원복은 aggressor 몫을 갑자기 되돌려 또 한 번 급변을 만든다. 승격은
    #     "여기서 더 못 한다"는 선언이지 "제어를 취소한다"가 아니므로 현 상태 동결이 맞다.
    handed_off = [False]     # 상위(재배치)로 넘긴 뒤 = 시간 축 동결
    # [Exp_118 A 수정] ★기준을 잡은 그 틱에는 대칭을 판정하지 않는다.
    #   자기 기준으로 나누므로 BASE_LOCK 시점의 저하율은 정의상 1.000 이고,
    #   같은 틱에 판정하면 **언제나 대칭**이 된다(1차 재실행에서 전 회차 sym=1.000).
    #   기준 확정 후 저하가 실제로 벌어질 시간을 준 뒤 판정한다.
    SYM_DECIDE_AFTER_S = 3.0   # = intervene_hold 기본값과 같게. 개입 판정과 같은 시점에 확정
    base_lock_t = [None]
    REEMIT_S = 30.0          # 같은 추천을 반복 발행하지 않는 최소 간격

    def emit_relocation(agg_tenant, vic_tenant, usage_note):
        """3단계 — 재배치 '추천'. 파드를 옮기지 않는다. 오케스트로 수집 경로와
        고려대 스케줄링 계층이 소비 대상이다."""
        now = time.time()
        if now - promoted_at[0] < REEMIT_S:
            return False
        promoted_at[0] = now
        payload = {
            "kind": "RelocationRecommendation",
            "reason": "TimeAxisExhausted",
            "detail": (f"시간 축 사다리를 끝까지 내렸으나 저하가 {PROMOTE_AFTER_S:.0f}s 이상 "
                       f"지속됨. 공간 축은 런타임 재설정 불가(mps_env)라 같은 노드에서 "
                       f"더 줄 수 있는 것이 없다."),
            "aggressor": agg_tenant, "victim": vic_tenant,
            "observation": usage_note,
            "recommend": {"action": "reschedule", "target": agg_tenant,
                          "to": "other-node-or-respawn-with-lower-space-share",
                          "note": "재스폰 시 BLESS_LIMIT_PCT 를 낮춰 스폰하면 공간 축이 반영된다"},
            "ts": now,
        }
        log(f"{now:.2f}", "PROMOTE-3", agg_tenant, json.dumps(payload, ensure_ascii=False)[:200])
        ok = _post(f"{a.feeder_url}/events/relocation", payload)
        if not ok:
            # [Exp_107 T-5] 발행 실패를 조용히 넘기지 않는다
            print(f"[loop][경고] 재배치 추천 발행 실패 — 수신자가 이 신호를 받지 못한다. "
                  f"payload={json.dumps(payload, ensure_ascii=False)[:160]}", flush=True)
        return True

    # ── [Exp_109 Q-3] 2단계 — 공간(QMD TPC 마스크) 재조정 ──────────────────
    # Exp_108 에서 "현 구조상 불가"로 판정했던 것을 QMD 로 실현한다.
    #   mps_env 는 프로세스 단위 env 라 런타임 재설정을 거부하지만(libbless.cpp),
    #   QMD 는 커널 런치마다 마스크를 얹으므로 실행 중 변경이 된다(Exp_109 Q-2 실측:
    #   3회 연속 변경, 무중단, 안정화 ~2초).
    #
    # ★순서는 Exp_108 판단을 유지한다 — 1단계 시간, 2단계 공간.
    #   근거는 그대로다: 공간 조정은 봉투 정책상 이득의 원천이 아니었고(P−C = −0.030),
    #   시간 축이 더 싸고 되돌리기 쉽다.
    #
    # 승격 기준(임의 상수 금지):
    #   SPACE_AFTER_S — 시간 사다리를 끝까지 내린 뒤에도 저하가 지속된 시간.
    #     3단계(재배치)의 PROMOTE_AFTER_S 보다 **먼저** 와야 하므로 그 절반으로 둔다.
    #     즉 시간(1) → 공간(2) → 재배치(3) 순으로 자연히 승격된다.
    #   SPACE_STEPS — 공간 사다리. 시간 사다리(steps)와 같은 모양으로 두어
    #     두 축이 같은 속도로 움직이게 한다. pct 이므로 100 을 곱한다.
    SPACE_AFTER_S = PROMOTE_AFTER_S / 2.0
    SPACE_STEPS = [int(x * 100) for x in steps]      # 예: [100, 70, 50, 40] %
    space_level = [0]
    space_last = [0.0]
    # [Exp_116 2부] 대칭 판정 상태
    sym_tried = [False]        # 대칭 경로에서 사다리를 한 칸 내려봤는가
    sym_p95_at_try = [None]    # 그 시점 victim p95 (개선 판정 기준)
    sym_promoted = [False]
    # [Exp_116 2부 수정] ★대칭 판정은 **개입 전에 한 번 확정하고 고정**한다.
    #   매 틱 재판정하면 안 된다 — 사다리를 내리는 순간 한쪽만 조여지므로
    #   대칭성이 개입 자신 때문에 깨지고, 그 다음 틱에 '비대칭'으로 판정돼
    #   대칭 경로를 이탈한다(실측: sym 분포 min 1.000 / 중앙값 2.13 / max 3.14, n=332×3).
    sym_locked = [None]        # None=미확정, True/False=확정
    SPACE_DWELL = max(2.0, a.min_dwell * 2)          # 공간은 시간보다 느리게 움직인다

    def _sock_of(tenant):
        """feeder status 에서 해당 테넌트의 libbless 소켓 경로를 얻는다."""
        st = _get(f"{a.feeder_url}/feeder/status")
        if not st:
            return None
        return (st.get("tenants", {}).get(tenant) or {}).get("sock")

    def actuate_space(lvl, agg_tenant, reason):
        """2단계 — aggressor 의 TPC 몫을 줄인다. QMD 모드에서만 유효하다."""
        sock = _sock_of(agg_tenant)
        if not sock:
            print(f"[loop][경고] 2단계: {agg_tenant} 소켓을 찾지 못해 공간 재조정을 "
                  f"건너뛴다(시간 축만 적용된 상태로 남는다)", flush=True)
            return False
        pct = SPACE_STEPS[min(lvl, len(SPACE_STEPS) - 1)]
        try:
            import socket as _s
            c = _s.socket(_s.AF_UNIX, _s.SOCK_DGRAM)
            c.settimeout(1.0)
            c.sendto(f"qmd_pct {pct}".encode(), sock)
            c.close()
        except Exception as e:
            # [Exp_107 T-5] 조용한 폴백 금지
            print(f"[loop][경고] 2단계 전송 실패 tenant={agg_tenant} sock={sock} "
                  f"err={e!r} → 공간 몫이 바뀌지 않았다", flush=True)
            return False
        log(f"{time.time():.2f}", "SPACE", agg_tenant, f"lvl={lvl}", f"pct={pct}", reason)
        return True

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
            # [Exp_118 A] ★기준선 확정(BASE_LOCK)이 r 계산보다 먼저여야 한다.
            #   자기 교정 도입 후 base 가 None 으로 시작하므로 순서가 뒤바뀌면
            #   `TypeError: float / NoneType` 로 폐루프가 즉시 죽는다(1차 실행 전 회차 실패).
            ap95_pre = (agg or {}).get("p95_ms")
            if not base_locked[0] and level == 0 and p95 and ap95_pre:
                vk0 = (vic.get("name") or vic.get("tenant") or "vic")
                ak0 = ((agg or {}).get("name") or (agg or {}).get("tenant") or "agg")
                self_base[vk0] = p95; self_base[ak0] = ap95_pre
                base_locked[0] = True; base_lock_t[0] = now
                if base is None:
                    base = p95
                log(f"{now:.2f}", "BASE_LOCK", f"{vk0}={p95:.1f}ms",
                    f"{ak0}={ap95_pre:.1f}ms", f"r_base={base:.1f}", "-", "-", "-")
            if base is None:
                # 아직 두 관측이 다 모이지 않았다 — 판정을 미룬다(T-5: 조용히 넘기지 않는다)
                _warn_once("base_wait", "기준선 미확정(관측 2개 대기 중) → 판정 보류")
                time.sleep(a.interval); continue
            r = p95 / base
            act = "hold"

            # ── [Exp_116 2부] 대칭 판정 ────────────────────────────────────
            #   양쪽이 비슷하게 아프면 조일 대상이 없다 → 사다리를 끝까지 내리는 동안
            #   상황만 악화시킨다(Exp_115 3-C: 총합 -16.7%, 서빙 측 -25.0%).
            #   ★완전 생략이 아니라 **한 칸만 시도 후 즉시 승격**한다(오탐 비용 대비).
            #   ★비대칭이면 이 블록을 통째로 건너뛴다 — 기존 동작이 그대로 보존된다.
            ap95 = ap95_pre
            # ★sym 을 **절대 p95 비가 아니라 각자의 저하율 비**로 계산한다(Exp_117 6-3).
            #   역할이 다르면 기준 지연이 13배까지 차이 나(prefill 810.7 / decode 61.6)
            #   둘이 똑같이 아파도 절대 비가 커져 항상 비대칭으로 판정됐다.
            sym = None; sym_abs = None
            if ap95 and p95:
                sym_abs = max(p95, ap95) / min(p95, ap95)     # 구 지표(대조용 기록)
                vk = (vic.get("name") or vic.get("tenant") or "vic")
                ak = ((agg or {}).get("name") or (agg or {}).get("tenant") or "agg")
                bv, ba = self_base.get(vk), self_base.get(ak)
                if bv and ba:
                    dv, da = p95 / bv, ap95 / ba              # 각자의 저하율
                    if dv > 0 and da > 0:
                        sym = max(dv, da) / min(dv, da)
                if sym is None:
                    sym = sym_abs                              # 기준 미확정 구간 폴백
            if (sym_locked[0] is None and level == 0 and sym is not None
                    and base_lock_t[0] is not None
                    and (now - base_lock_t[0]) >= SYM_DECIDE_AFTER_S):
                sym_locked[0] = (sym <= SYMMETRY_RATIO)
                _warn_once("sym_lock",
                           f"대칭 판정 확정: sym={sym:.3f} (구 지표 절대비={sym_abs:.3f}) → "
                           f"{'대칭(한 칸 시도 후 즉시 승격)' if sym_locked[0] else '비대칭(기존 사다리)'}")
            symmetric = bool(sym_locked[0])

            if symmetric and r > a.k_intervene and not sym_promoted[0]:
                if not sym_tried[0]:
                    if (now - last_change) >= a.min_dwell and level < len(steps) - 1:
                        old_lv = level
                        level = min(level + SYM_TRY_STEPS, len(steps) - 1)
                        last_change = now; transitions += 1
                        sym_tried[0] = True; sym_p95_at_try[0] = p95
                        actuate(level, last_agg, f"sym-try r={r:.2f} sym={sym:.3f}")
                        log(f"{now:.2f}", f"{p95:.1f}", f"{r:.2f}", level, steps[level],
                            transitions, f"SYM_TRY({old_lv}->{level})")
                    continue
                # 한 칸 내려본 뒤 — 개선 없으면 즉시 3단계
                if (now - last_change) >= a.min_dwell:
                    prev = sym_p95_at_try[0] or p95
                    improved = (prev - p95) / prev >= SYM_IMPROVE
                    if improved:
                        log(f"{now:.2f}", f"{p95:.1f}", f"{r:.2f}", level, steps[level],
                            transitions, f"SYM_IMPROVED({(prev-p95)/prev*100:.1f}%)")
                        sym_tried[0] = False; sym_p95_at_try[0] = None
                    else:
                        sym_promoted[0] = True
                        emit_relocation(last_agg, vic.get("tenant") if vic else None,
                                        f"symmetric-degradation sym={sym:.3f} "
                                        f"one-step-no-improve({(prev-p95)/prev*100:+.1f}%)")
                        handed_off[0] = True      # [Exp_118 B] 시간 축 동결
                        log(f"{now:.2f}", f"{p95:.1f}", f"{r:.2f}", level, steps[level],
                            transitions, "SYM_PROMOTE-3")
                    continue
                continue
            # ★비대칭으로 확정됐으면 아래 기존 로직이 그대로 돈다(동작 보존).
            if sym_locked[0] is None:
                # 대칭 판정 확정 전에는 사다리를 내리지 않는다 — 개입이 대칭성을
                # 스스로 깨뜨리기 때문(Exp_116 2부 처방과 같은 이유).
                log(f"{now:.2f}", f"{p95:.1f}", f"{r:.2f}", level, steps[level],
                    transitions, "SYM_WAIT",
                    (f"sym={sym:.3f}/abs={sym_abs:.3f}" if sym and sym_abs else "sym=NA"))
                time.sleep(a.interval); continue

            # [Exp_118 B] 상위로 넘긴 뒤에는 시간 축을 건드리지 않는다(현 위치 동결).
            if handed_off[0]:
                log(f"{now:.2f}", f"{p95:.1f}", f"{r:.2f}", level, steps[level],
                    transitions, "FROZEN(handed-off)",
                    (f"sym={sym:.3f}/abs={sym_abs:.3f}" if sym and sym_abs else "sym=NA"))
                time.sleep(a.interval); continue

            # 개입: 임계 초과가 intervene_hold 초 이상 지속돼야 조임(순간 스파이크 무시)
            if r > a.k_intervene:
                if above_since is None:
                    above_since = now
            else:
                above_since = None
            sustained = above_since is not None and (now - above_since) >= a.intervene_hold
            # [Exp_108 D-3] 3단계 승격 — 사다리 끝(level=max)에서도 저하가 지속되면
            #   시간 축으로는 못 푼다. 공간 축은 런타임 재설정 불가이므로 재배치를 추천한다.
            # [Exp_109 Q-3] 2단계 — 시간 사다리 끝에서 SPACE_AFTER_S 지속 시 공간 조임.
            #   ★충돌 방지: 시간 축은 이미 최저 단계(steps[-1])로 고정된 상태에서만
            #     공간을 건드린다. 둘이 동시에 내려가면 과하게 줄어든다.
            if (sustained and level >= len(steps) - 1
                    and above_since is not None
                    and (now - above_since) >= SPACE_AFTER_S
                    and space_level[0] < len(SPACE_STEPS) - 1
                    and (now - space_last[0]) >= SPACE_DWELL):
                space_level[0] += 1
                space_last[0] = now
                if actuate_space(space_level[0], last_agg,
                                 f"space-tighten r={r:.2f} lvl={space_level[0]}"):
                    if a.emit_events:
                        emit_event("SpaceControlApplied",
                                   f"aggressor TPC → {SPACE_STEPS[space_level[0]]}% "
                                   f"(time axis exhausted, r={r:.2f})")
            # 저하가 풀리면 공간도 되돌린다(진동 방지: 시간 축이 완전히 풀린 뒤에만)
            if level == 0 and space_level[0] > 0 and (now - space_last[0]) >= SPACE_DWELL:
                space_level[0] = 0
                space_last[0] = now
                actuate_space(0, last_agg, "space-release(level=0)")

            if (sustained and level >= len(steps) - 1
                    and above_since is not None
                    and (now - above_since) >= PROMOTE_AFTER_S):
                emit_relocation(last_agg, vic.get("tenant") if vic else None,
                                f"victim p95 {p95:.1f}ms = {r:.2f}x baseline, "
                                f"level={level}(max) ratio={steps[level]}")
                handed_off[0] = True      # [Exp_118 B] 시간 축 동결
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

            log(f"{now:.2f}", f"{p95:.1f}", f"{r:.2f}", level, steps[level], transitions, act,
                (f"sym={sym:.3f}/abs={sym_abs:.3f}" if sym and sym_abs else "sym=NA"))
    finally:
        # 종료 시 aggressor 원복(release) — 다음 실험 오염 방지
        if last_agg:
            _post(f"{a.feeder_url}/feeder/release", {"tenant": last_agg})
        log(f"# END transitions={transitions}")
        lg.close()


if __name__ == "__main__":
    main()
