#!/usr/bin/env python3
"""[Exp_108 D-1] 적응형 슬라이싱 — 관측 → 판단 → 예산 재설정 폐루프.

계획서(슬라이드 12)의 두 문제를 서로 다른 처방으로 다룬다.

  · 학습형(연산 집약): 예산을 다 쓰고도 더 쓸 게 있다 → 예산을 **늘린다**
  · 추론형(메모리 집약): 예산이 남는데 자리를 차지한다 → 예산을 **줄인다**

★두 경우를 사용률 하나로는 못 가른다. 둘 다 "사용률이 100%가 아니다"로 보일 수 있기
  때문이다. 그래서 두 축을 함께 본다.

    usage  = charged_delta_us / 배정 예산(us)        (feeder.sample_occupancy)
    gate   = 게이트 전환 횟수·대기시간              (libbless gate_stats, Exp_108 D-2)

  판정:
    usage 높음 + gate 대기 많음 → 예산 부족(학습형)   → ratio ↑
    usage 낮음 + gate 대기 적음 → 예산 과다(추론형)   → ratio ↓
    그 외                       → 유지

★기존 동작 불변: 이 루프를 띄우지 않으면 아무 것도 바뀌지 않는다(별도 프로세스).
★closed_loop.py 와 같은 형태로 만들었다 — 나중에 둘의 상호작용을 같은 틀에서 볼 수 있다.

임계값 근거(임의 상수 금지):
  HIGH_USAGE 0.85 / LOW_USAGE 0.55
    Exp_98 의 전이 곡선에서 Σpct 100%→120% 구간이 "여유"와 "경합"을 가르는 지점이었다.
    사용률로 환산하면 0.83~0.85 부근이 포화 시작이다. 0.85 초과를 "더 쓸 게 있다"로,
    그 절반 남짓(0.55) 미만을 "남긴다"로 둔다. 두 임계 사이를 넓게 벌려(0.30) 진동을 막는다.
  STEP 0.10, 상한 [0.5×, 1.5×]
    Exp_89 폐루프의 ratio 사다리가 1.0→0.7→0.5→0.4 로 한 칸 0.2~0.3 이었고 진동이 없었다.
    예산 조절은 그보다 자주 돌므로(1s) 한 칸을 그 절반 이하인 0.10 으로 잡는다.
    상한은 초기값의 ±50% — 이를 넘으면 배정 계약(LSU 요청량)에서 너무 멀어진다.
  UP_HOLD 2 / DOWN_HOLD 3 (연속 관측 횟수)
    같은 방향 신호가 연속으로 와야 움직인다. 되돌리는 쪽(줄이기)에 더 큰 요구를 두어
    (2 vs 3) 왕복을 막는다 — closed_loop 의 intervene-hold < release-hold 와 같은 비대칭.
"""
import argparse, json, os, time, urllib.request

HIGH_USAGE = 0.85
LOW_USAGE = 0.55
STEP = 0.10
BOUND_LO, BOUND_HI = 0.5, 1.5      # 초기 ratio 대비 배수 상한
UP_HOLD, DOWN_HOLD = 2, 3

_WARNED = set()


def _warn_once(key, msg):
    """[Exp_107 T-5] 조용한 폴백 금지 — 같은 사유는 1회만."""
    if key not in _WARNED:
        _WARNED.add(key)
        print(f"[slice][경고] {msg}", flush=True)


def _get(url, timeout=3):
    try:
        with urllib.request.urlopen(url, timeout=timeout) as r:
            return json.load(r)
    except Exception as e:
        _warn_once("get:" + url, f"조회 실패 {url} err={e!r} → 이번 주기 건너뜀")
        return None


def _post(url, body, timeout=3):
    try:
        req = urllib.request.Request(url, data=json.dumps(body).encode(),
                                     headers={"Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.status < 300
    except Exception as e:
        _warn_once("post:" + url, f"POST 실패 {url} err={e!r} → 조절이 반영되지 않는다")
        return False


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--feeder-url", default="http://localhost:8090")
    ap.add_argument("--interval", type=float, default=1.0)
    ap.add_argument("--duration", type=float, default=0, help=">0 이면 그 초 뒤 종료")
    ap.add_argument("--log", default=os.environ.get("KRAKEN_SLICE_LOG",
                                                    "/tmp/kraken_slice_loop.log"))
    ap.add_argument("--dry-run", action="store_true", help="판정만 하고 적용하지 않는다")
    ap.add_argument("--tag", default="")
    a = ap.parse_args()

    lg = open(a.log, "a")

    def log(*x):
        line = "\t".join(str(i) for i in x)
        lg.write(line + "\n"); lg.flush()
        print("[slice] " + line, flush=True)

    log(f"# START tag={a.tag} interval={a.interval}s high={HIGH_USAGE} low={LOW_USAGE} "
        f"step={STEP} bound=[{BOUND_LO},{BOUND_HI}] hold=up{UP_HOLD}/down{DOWN_HOLD} "
        f"dry_run={a.dry_run}")

    base_ratio = {}      # tenant → 초기 ratio (상한 계산 기준)
    cur_mult = {}        # tenant → 현재 배수
    streak = {}          # tenant → (방향, 연속 횟수)
    t_start = time.time()
    n_apply = 0
    # [Exp_114 1-A] ★사용률 분모는 **실측 샘플 간격**이어야 한다.
    #   구 구현은 alloc_us = tgt * a.interval * 1e6 으로 설정값(1.0s)을 썼는데,
    #   루프 한 바퀴는 조회 2회 + sleep 이라 실측 중앙값이 2.21초였다(Exp_113 M-6).
    #   분모가 2.2배 작으니 usage 가 2.2배 부풀고 판정이 뒤집혔다
    #   (기록 1.069~1.111 → 보정 0.484~0.503, HIGH 0.85 초과 vs LOW 0.55 미만).
    #   charged_delta 는 feeder 가 "직전 샘플 이후" 증가분으로 주므로, 분모도
    #   같은 구간이어야 단위가 맞는다.
    #   ★첫 샘플: 이전 시각이 없다 → 그 주기는 **판정을 건너뛴다**(0 으로 나누거나
    #     설정값으로 대체하면 첫 판정이 틀린다). feeder 도 첫 호출은 delta 를 주지
    #     않으므로 버리는 것이 자연스럽다.
    last_sample_t = None

    while True:
        if a.duration and time.time() - t_start > a.duration:
            log(f"# END applied={n_apply}")
            return

        st = _get(f"{a.feeder_url}/feeder/status")
        occ = _get(f"{a.feeder_url}/feeder/occupancy")
        if not st or not occ:
            time.sleep(a.interval); continue
        # [Exp_114 1-A] occupancy 응답 직후를 샘플 시각으로 본다(조회에 걸린 시간이
        #   구간에 포함되지 않도록 delta 의 끝단에 맞춘다).
        now_t = time.time()
        elapsed = None if last_sample_t is None else (now_t - last_sample_t)
        last_sample_t = now_t
        if elapsed is None:
            _warn_once("first", "첫 주기는 직전 샘플이 없어 판정을 건너뛴다(정상)")
            time.sleep(a.interval); continue
        if elapsed <= 0:
            time.sleep(a.interval); continue

        tenants = st.get("tenants", {})
        share = occ.get("observed_share", {}) or {}
        deltas = occ.get("charged_delta_us", {}) or {}
        if not tenants:
            time.sleep(a.interval); continue

        for name, t in tenants.items():
            if not t.get("armed"):
                continue
            r = float(t.get("ratio", 0)) or 0.0
            if r <= 0:
                continue
            base_ratio.setdefault(name, r / cur_mult.get(name, 1.0))
            cur_mult.setdefault(name, 1.0)

            # 사용률 = 실제 청구시간 / 배정 예산(구간). target_share 가 배정 몫이다.
            tgt = t.get("target_share")
            d = deltas.get(name)
            if tgt is None or d is None or tgt <= 0:
                continue
            # 구간 동안 배정된 시간(us) = target_share × **실측 경과 구간**
            alloc_us = tgt * elapsed * 1e6
            usage = d / alloc_us if alloc_us > 0 else 0.0

            # 두 번째 축: 게이트 대기. occupancy 응답에 없으면 사용률만으로 판단하되
            # 그 사실을 남긴다(T-5).
            gate_busy = occ.get("gate_wait", {}).get(name)
            if gate_busy is None:
                _warn_once("nogate", "gate_wait 미제공 → 사용률 단독 판정. "
                                     "libbless BLESS_GATE_METRICS=1 이 필요하다(D-2).")

            direction = 0
            if usage >= HIGH_USAGE and (gate_busy is None or gate_busy > 0):
                direction = +1      # 예산 부족 — 더 준다
            elif usage <= LOW_USAGE and (gate_busy is None or gate_busy == 0):
                direction = -1      # 예산 과다 — 줄인다

            prev_dir, cnt = streak.get(name, (0, 0))
            cnt = cnt + 1 if direction == prev_dir and direction != 0 else (1 if direction else 0)
            streak[name] = (direction, cnt)

            need = UP_HOLD if direction > 0 else DOWN_HOLD
            if direction == 0 or cnt < need:
                # [Exp_141 2부] 조절 유지 중에도 임대를 갱신한다. feeder 의 기본
                #   만료(RATIO_LEASE_DEFAULT_S)가 생겨, 변경 시에만 보내던 구
                #   동작으로는 안정 유지 구간에서 갱신이 끊겨 3초마다 계약값으로
                #   튕긴다. 계약 그대로(mult==1.0)면 보낼 것이 없다.
                if not a.dry_run and abs(cur_mult[name] - 1.0) > 1e-9:
                    _post(f"{a.feeder_url}/feeder/ratios",
                          {"ratios": {name: round(base_ratio[name] * cur_mult[name], 4)},
                           "reason": "adaptive-renew"})
                continue

            new_mult = cur_mult[name] + STEP * direction
            clamped = min(BOUND_HI, max(BOUND_LO, new_mult))
            if clamped != new_mult:
                _warn_once(f"bound:{name}",
                           f"{name} 조절값이 상한을 벗어남({new_mult:.2f}) → "
                           f"{clamped:.2f} 로 되돌림. 배정 계약에서 멀어지지 않게 막는다.")
            if abs(clamped - cur_mult[name]) < 1e-9:
                continue

            new_ratio = round(base_ratio[name] * clamped, 4)
            log(f"{time.time():.2f}", name, f"usage={usage:.3f}", f"dt={elapsed:.2f}s",
                f"gate={gate_busy}",
                f"dir={direction:+d}", f"streak={cnt}",
                f"mult {cur_mult[name]:.2f}->{clamped:.2f}", f"ratio->{new_ratio}")
            if not a.dry_run:
                if _post(f"{a.feeder_url}/feeder/ratios",
                         {"ratios": {name: new_ratio},
                          "reason": f"adaptive-slice usage={usage:.2f} dir={direction:+d}"}):
                    n_apply += 1
            cur_mult[name] = clamped
            streak[name] = (direction, 0)      # 적용 후 연속 카운트 초기화(진동 방지)

        time.sleep(a.interval)


if __name__ == "__main__":
    main()
