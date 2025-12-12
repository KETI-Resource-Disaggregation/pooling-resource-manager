#!/usr/bin/env python3
# SPARK Scheduler with Priority-aware Auto Planner (lexicographic & waterfilling)
import os, sys, time, socket, re, json, csv, math, threading, random
from typing import Dict, List, Optional, Tuple
from collections import deque
from http.server import BaseHTTPRequestHandler, HTTPServer

# ======================================================================
# Basic I/O
# ======================================================================
MASTER       = os.environ.get("BLESS_MASTER", "/tmp/bless-master.sock")
API_ADDR     = os.environ.get("API_ADDR", "127.0.0.1")
API_PORT     = int(os.environ.get("API_PORT", "6060"))
protocol_version = "HTTP/1.0"
# ======================================================================
# Scheduling knobs (env)
# ======================================================================
SQUAD           = int(os.environ.get("SQUAD", "10000"))
SQUAD_TMO_S     = float(os.environ.get("SQUAD_TMO_S", "3.0"))
EQUAL_SHARE     = os.environ.get("EQUAL_SHARE", "1") == "1"
TICK_S          = float(os.environ.get("TICK_S", "0.006"))
CREDIT_PER_TICK = int(os.environ.get("CREDIT_PER_TICK", "524288"))

# Boost/solo/ramp
BOOST_DEBOUNCE_S    = float(os.environ.get("BOOST_DEBOUNCE_S", "0.4"))
ROTATE_BOOST        = os.environ.get("ROTATE_BOOST", "1") == "1"
SOLO_UNLIMITED      = os.environ.get("SOLO_UNLIMITED", "0") == "1"
SOLO_MAX_FRACTION   = float(os.environ.get("SOLO_MAX_FRACTION", "0.6"))
MIN_CREDIT_PER_TICK = int(os.environ.get("MIN_CREDIT_PER_TICK", "256"))
FREED_RAMP_ALPHA    = float(os.environ.get("FREED_RAMP_ALPHA", "0.5"))

# Auto-planner policy & weights
GOAL_POLICY   = os.environ.get("GOAL_POLICY", "lexi")  # lexi | wf
ALPHA_THR_W   = float(os.environ.get("ALPHA_THR_W", "1.0"))  # 처리량 위반 가중
ALPHA_LAT_W   = float(os.environ.get("ALPHA_LAT_W", "1.0"))  # 지연 위반 가중
JITTER_PCT    = float(os.environ.get("PLANNER_JITTER_PCT", "0.0"))  # 계획잡음(실험용)

# Tick adapt bounds
TICK_MIN      = float(os.environ.get("TICK_MIN", "0.0018"))
TICK_MAX      = float(os.environ.get("TICK_MAX", "0.0100"))
TICK_KAPPA    = float(os.environ.get("TICK_KAPPA", "0.35"))

# Priority weights (High > Med > Low)
PRIO_W = {
    "HIGH": float(os.environ.get("PRIO_W_HIGH", "3.0")),
    "MED":  float(os.environ.get("PRIO_W_MED",  "1.0")),
    "LOW":  float(os.environ.get("PRIO_W_LOW",  "0.5")),
}
# Donor steal caps per priority (max fraction we can steal FROM a donor of that prio)
STEAL_FRAC = {
    "HIGH": float(os.environ.get("STEAL_FRAC_HIGH", "0.30")),
    "MED":  float(os.environ.get("STEAL_FRAC_MED",  "0.60")),
    "LOW":  float(os.environ.get("STEAL_FRAC_LOW",  "0.90")),
}

def parse_base_shares(txt: str) -> Dict[str, float]:
    out: Dict[str, float] = {}
    for tok in txt.split(","):
        tok = tok.strip()
        if not tok or ":" not in tok: continue
        k, v = tok.split(":"); k = k.strip()
        try: out[k] = float(v)
        except: pass
    return out
TARGET_W = parse_base_shares(os.environ.get("BASE_SHARES", ""))

# ======================================================================
# CSV logging (lightweight)
# ======================================================================
# ===== CSV 로깅 =====
LOG_CSV = os.environ.get("SQUAD_LOG", "squad_log.csv")
csv_f = open(LOG_CSV, "w", newline="", buffering=1)
csv_w = csv.writer(csv_f)
csv_w.writerow(["ts_s","ev","squad","tenant","share","remain","note"])
def now_s(): return f"{time.time():.6f}"
def jdump(d):
    try: return json.dumps(d, ensure_ascii=False)
    except: return str(d)
def log(ev, squad_id, tenant="", share=None, remain=None, note=""):
    csv_w.writerow([now_s(), ev, squad_id, tenant, jdump(share or {}), jdump(remain or {}), note])

# ===== (NEW) 크레딧/스쿼드 타임시리즈 CSV =====
CREDIT_TS_CSV = os.environ.get("CREDIT_CSV", "credit_timeseries.csv")
credit_f = open(CREDIT_TS_CSV, "w", newline="", buffering=1)
credit_w = csv.writer(credit_f)
# type: assign | used | squad_cfg | rebase
credit_w.writerow([
    "ts_s","type","tick_id","squad_id","tenant",
    "assigned","used","base_pct","boost","freed_pct",
    "tick_s","credits_per_tick"
])

_g_tick_id = 0  # 전역 틱 카운터

def clog(row_type:str, tick_id:int, squad_id:int, tenant:str,
         assigned:int|None=None, used:int|None=None,
         base_pct:float|None=None, boost:str|None=None, freed:float|None=None):
    """크레딧/스쿼드 타임시리즈 한 줄 기록"""
    credit_w.writerow([
        now_s(), row_type, tick_id, squad_id, tenant,
        ("" if assigned is None else int(assigned)),
        ("" if used is None else int(used)),
        ("" if base_pct is None else float(base_pct)),
        ("" if not boost else str(boost)),
        ("" if freed is None else float(freed)),
        TICK_S, CREDIT_PER_TICK
    ])

# ======================================================================
# Tenant protocol (DGRAM control)
# ======================================================================
HELLO_RE = re.compile(r"HELLO pid=(\d+)\s+sock=([/\w\.\-]+)\s+tenant=(\w*)")
SD_RE    = re.compile(r"SD pid=(\d+)\s+t=(\w+)\s+sp=(\d+)\s+kseq=(\d+)")
BYE_RE   = re.compile(r"BYE pid=(\d+)\s+t=(\w+)")

CTRL_EQUAL_RE      = re.compile(r"\s*SET_EQUAL\s+([01])\s*$")
CTRL_WEIGHTS_RE    = re.compile(r"\s*SET_WEIGHTS\s+(.+?)\s*$")
CTRL_SET_SHARE_RE  = re.compile(r"\s*SET_SHARE\s+([A-Za-z0-9_\-]+)\s+([0-9]+(?:\.[0-9]+)?)\s*$")
CTRL_TICK_RE       = re.compile(r"\s*SET_TICK\s+([0-9]*\.?[0-9]+)\s*$")
CTRL_CRED_RE       = re.compile(r"\s*SET_CREDITS\s+([0-9]+)\s*$")
CTRL_BOOST_RE      = re.compile(r"\s*BOOST\s+([A-Za-z0-9_\-]+)\s*$")
CTRL_BOOST_ON_RE   = re.compile(r"\s*BOOST_ENABLE\s*$|^\s*BOOST_ON\s*$")
CTRL_BOOST_OFF_RE  = re.compile(r"\s*BOOST_DISABLE\s*$|^\s*BOOST_OFF\s*$")
CTRL_CRED_OFF_RE   = re.compile(r"\s*CREDIT_OFF\s*$")
CU_RE = re.compile(r"CU pid=(\d+)\s+t=(\w+)\s+used=(\d+)")

# ======================================================================
# Runtime state
# ======================================================================
class Tenant:
    __slots__ = ("pid","sock","alive")
    def __init__(self, pid:int, sock:str):
        self.pid = pid; self.sock = sock; self.alive=True

tenants: Dict[str,Tenant] = {}
remain : Dict[str,int]   = {}
inbox_events: deque = deque()

# credit & squad state
CUR_BASE_PCT: Dict[str, float] = {}     # current squad base%, ~sum 100
CUR_FINISHED: set = set()
CUR_BOOST: Optional[str] = None
BOOST_ALLOWED: bool = True
LAST_CRED_ALLOC: Dict[str, int] = {}    # last tick credit_set per tenant

_boost_last_change = 0.0
_boost_last_tid    = None
_freed_ramp        = 0.0

# send cache to reduce overhead
_snd = None
_last_sent_credit: Dict[str,int] = {}

def _ensure_sender():
    global _snd
    if _snd is None:
        _snd = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
        try: _snd.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 1<<20)
        except: pass
    return _snd

def send(sock_path: str, msg: str, tries=6, delay=0.005):
    s = _ensure_sender()
    data = msg.encode()
    for _ in range(tries):
        try:
            s.sendto(data, sock_path); return True
        except OSError:
            time.sleep(delay)
    print(f"[warn] send fail -> {sock_path}: {msg[:48]}...")
    return False

def _send_credit_once(tid: str, val: int):
    prev = _last_sent_credit.get(tid)
    if prev == val: return
    send(tenants[tid].sock, f"credit_set {val}")
    _last_sent_credit[tid] = val

# ======================================================================
# Telemetry & simple models (EMA)
# ======================================================================
EMA_A = float(os.environ.get("TELEM_EMA", "0.3"))
class Tele:
    __slots__=("qps","lat_p90","tok_per_s","imgs_per_s","ts","hist")
    def __init__(self):
        self.qps=None; self.lat_p90=None; self.tok_per_s=None; self.imgs_per_s=None; self.ts=0.0
        self.hist = deque(maxlen=8)  # (credit, lat_p90, ts)

TELEM: Dict[str, Tele] = {}

def _ema(old: Optional[float], x: Optional[float], a=EMA_A):
    if x is None: return old
    return x if (old is None) else (a*x+(1-a)*old)

def estimate_alpha(tid: str) -> Optional[float]:
    """ q ≈ alpha * c """
    t = TELEM.get(tid)
    c = float(max(LAST_CRED_ALLOC.get(tid, 0), 1))
    if t and t.qps and c>0:
        return max(1e-9, t.qps / c)
    return None

def update_latency_hist(tid: str):
    t = TELEM.get(tid)
    if not t or t.lat_p90 is None: return
    c = float(max(LAST_CRED_ALLOC.get(tid, 0), 1))
    t.hist.append((c, float(t.lat_p90), time.time()))

def fit_latency_uv(tid: str) -> Optional[Tuple[float,float]]:
    """
    Fit L ≈ u/c + v from last two distinct credit samples.
    Robust fallback to EMA (u,v) if not enough data.
    """
    t = TELEM.get(tid)
    if not t or len(t.hist) < 2: return None
    # find two points with distinct credits
    p2 = list(t.hist)[-1]
    p1 = None
    for k in range(len(t.hist)-2, -1, -1):
        if abs(t.hist[k][0] - p2[0]) >= 1.0:
            p1 = t.hist[k]; break
    if not p1: return None
    c1,L1,_ = p1; c2,L2,_ = p2
    denom = (1.0/c2 - 1.0/c1)
    if abs(denom) < 1e-12: return None
    u = (L2 - L1) / denom
    v = L1 - u / c1
    # clamp v (non-negative baseline)
    v = max(0.0, v)
    return (float(u), float(v))

# ======================================================================
# Priority map & helpers
# ======================================================================
PRIO: Dict[str, str] = {}  # tid-> "HIGH"|"MED"|"LOW"
def get_prio(tid:str) -> str:
    return PRIO.get(tid, "MED")

def prio_weight(tid:str) -> float:
    return PRIO_W.get(get_prio(tid), 1.0)

def donor_steal_frac(tid:str) -> float:
    return STEAL_FRAC.get(get_prio(tid), 0.6)

# ======================================================================
# Common helpers
# ======================================================================
def _norm_pct(d: Dict[str,float]) -> Dict[str,float]:
    s = sum(max(0.0, v) for v in d.values())
    if s <= 0:
        n = len(d) or 1
        return {k: (100.0/n) for k in d}
    return {k: 100.0 * max(0.0, v) / s for k,v in d.items()}

def _fill_weights_for_active(active: List[str], target_w: Dict[str,float]) -> Dict[str,float]:
    specified = {t: float(target_w[t]) for t in active if t in target_w}
    sum_spec  = sum(specified.values())
    unspec    = [t for t in active if t not in target_w]
    out: Dict[str,float] = {}
    if unspec:
        rest = max(0.0, 100.0 - sum_spec); each = rest/len(unspec) if unspec else 0.0
        for t in active: out[t] = specified.get(t, each)
    else:
        out = {t: specified.get(t, 0.0) for t in active}
    return _norm_pct(out)

def _compute_base_pct(active: List[str]) -> Dict[str,float]:
    if not active: return {}
    if EQUAL_SHARE or not TARGET_W:
        each = 100.0 / len(active); return {t: each for t in active}
    return _fill_weights_for_active(active, TARGET_W)

def _rebase_current_for_unfinished(active: List[str]):
    global CUR_BASE_PCT
    if not active: return
    finished_sum = sum(CUR_BASE_PCT.get(t,0.0) for t in CUR_FINISHED)
    remain_budget = max(0.0, 100.0 - finished_sum)
    unfinished = [t for t in active if t not in CUR_FINISHED]
    if not unfinished: return
    desired_all = _compute_base_pct(active)
    denom = sum(desired_all[t] for t in unfinished) or 1.0
    new_map = {}
    for t in active:
        if t in CUR_FINISHED: new_map[t] = CUR_BASE_PCT.get(t, 0.0)
        else: new_map[t] = remain_budget * (desired_all[t] / denom)
    CUR_BASE_PCT = new_map

def weighted_split_int_by_pct(active: List[str], total: int, pct: Dict[str,float]) -> Dict[str,int]:
    if not active: return {}
    raw = [total * max(0.0, pct.get(t, 0.0)) / 100.0 for t in active]
    ints= [int(math.floor(x)) for x in raw]
    rem = total - sum(ints)
    if len(active)>0 and rem>0:
        frac= [(raw[i]-ints[i], i) for i in range(len(active))]
        frac.sort(reverse=True)
        for k in range(rem):
            ints[frac[k % len(active)][1]] += 1
    return {active[i]: ints[i] for i in range(len(active))}

def _freed_pct() -> float:
    return sum(CUR_BASE_PCT.get(t, 0.0) for t in CUR_FINISHED)

def _pick_boost_target(active: List[str]) -> Optional[str]:
    unfinished = [t for t in active if t not in CUR_FINISHED]
    if not unfinished: return None
    if ROTATE_BOOST:
        uf = sorted(unfinished)
        global _boost_last_tid
        if _boost_last_tid not in uf: return uf[0]
        return uf[(uf.index(_boost_last_tid)+1) % len(uf)]
    return max(unfinished, key=lambda k: prio_weight(k))

# ======================================================================
# Receive control & tenant events
# ======================================================================
def _recv_all():
    global EQUAL_SHARE, TICK_S, CREDIT_PER_TICK, BOOST_ALLOWED, CUR_BOOST
    try:
        while True:
            data,_ = srv.recvfrom(512)
            s = data.decode("utf-8","ignore").strip()

            m = HELLO_RE.match(s)
            if m:
                pid, sock, tid = m.groups()
                tenants[tid] = Tenant(int(pid), sock)
                if tid not in remain: remain[tid] = -1
                if tid not in PRIO: PRIO[tid] = "MED"
                inbox_events.append(("HELLO", tid))
                print(f"[HELLO] {tid} -> {sock}")
                continue
            m = SD_RE.match(s)
            if m:
                _pid, tid, sp, kseq = m.groups()
                inbox_events.append(("SD", tid)); continue
            m = BYE_RE.match(s)
            if m:
                _pid, tid = m.groups()
                if tid in tenants: tenants[tid].alive=False
                inbox_events.append(("BYE", tid)); continue

            m = CTRL_CRED_OFF_RE.match(s)
            if m: inbox_events.append(("CTRL_CRED_OFF", True)); continue

            m = CTRL_EQUAL_RE.match(s)
            if m:
                EQUAL_SHARE = (int(m.group(1))==1)
                inbox_events.append(("CTRL_EQUAL", EQUAL_SHARE))
                print(f"[ctrl] EQUAL_SHARE -> {1 if EQUAL_SHARE else 0}")
                continue

            m = CTRL_WEIGHTS_RE.match(s)
            if m:
                spec = m.group(1)
                new_w: Dict[str,float] = {}
                for tok in spec.split(","):
                    if ":" not in tok: continue
                    k,v = tok.split(":"); k = k.strip()
                    try: new_w[k] = float(v)
                    except: pass
                if new_w:
                    TARGET_W.clear(); TARGET_W.update(new_w)
                    EQUAL_SHARE = False
                    inbox_events.append(("CTRL_WEIGHTS", dict(TARGET_W)))
                    print(f"[ctrl] WEIGHTS -> {TARGET_W} (EQUAL=0)")
                continue

            m = CTRL_SET_SHARE_RE.match(s)
            if m:
                tgt = m.group(1); pct = float(m.group(2))
                alive = [tid for tid, info in tenants.items() if info.alive]
                if tgt not in alive: alive = sorted(set(alive + [tgt]))
                others = [t for t in alive if t != tgt]
                tmp_w: Dict[str,float] = {}
                if not others: tmp_w[tgt] = 100.0
                else:
                    rest = max(0.0, 100.0 - pct); each = rest/len(others)
                    for t in alive: tmp_w[t] = pct if t==tgt else each
                TARGET_W.clear(); TARGET_W.update(tmp_w)
                EQUAL_SHARE = False
                inbox_events.append(("CTRL_WEIGHTS", dict(TARGET_W)))
                print(f"[ctrl] SET_SHARE {tgt}={pct}% -> TARGET_W={TARGET_W}")
                continue

            m = CTRL_TICK_RE.match(s)
            if m:
                TICK_S = float(m.group(1))
                inbox_events.append(("CTRL_TICK", TICK_S))
                print(f"[ctrl] TICK_S -> {TICK_S}s")
                continue

            m = CTRL_CRED_RE.match(s)
            if m:
                CREDIT_PER_TICK = int(m.group(1))
                inbox_events.append(("CTRL_CRED", CREDIT_PER_TICK))
                print(f"[ctrl] CREDIT_PER_TICK -> {CREDIT_PER_TICK}")
                continue

            m = CTRL_BOOST_ON_RE.match(s)
            if m:
                BOOST_ALLOWED = True
                inbox_events.append(("CTRL_BOOST_ALLOWED", True))
                print(f"[ctrl] BOOST enable"); continue
            m = CTRL_BOOST_OFF_RE.match(s)
            if m:
                BOOST_ALLOWED = False
                CUR_BOOST = None
                inbox_events.append(("CTRL_BOOST_ALLOWED", False))
                print(f"[ctrl] BOOST disable"); continue

            m = CTRL_BOOST_RE.match(s)
            if m:
                tgt = m.group(1)
                if BOOST_ALLOWED:
                    CUR_BOOST = tgt
                    inbox_events.append(("CTRL_BOOST", tgt))
                    print(f"[ctrl] BOOST target -> {tgt}")
                else:
                    print(f"[ctrl] BOOST ignored (disabled)")
                continue
            
            m = CU_RE.match(s)
            if m:
                _pid, tid, used = m.groups()
                inbox_events.append(("CU", (tid, int(used))))
                continue

    except socket.timeout:
        pass

# ======================================================================
# Credit ticking
# ======================================================================
def push_credits(active: List[str], tick_id:int, squad_id:int):
    """틱마다 크레딧 분배. 솔로 폭주 억제 + freed 램핑 + 부스트 보너스."""
    global _freed_ramp
    if not active: return {}

    alive_unfinished = [t for t in active if t not in CUR_FINISHED]
    total = max(1, CREDIT_PER_TICK)

    # ---- 혼자 남은 경우 처리 ----
    if len(alive_unfinished) == 1:
        only = alive_unfinished[0]
        if SOLO_UNLIMITED:
            for t in active:
                n = -1 if t == only else 0
                send(tenants[t].sock, f"credit_set {n}")
                # assign 로그
                clog("assign", tick_id, squad_id, t,
                     assigned=n, base_pct=CUR_BASE_PCT.get(t,0.0),
                     boost=(CUR_BOOST if CUR_BOOST==t else ""),
                     freed=_freed_ramp)
            return {only: -1, **{t:0 for t in active if t!=only}}
        cap = int(max(MIN_CREDIT_PER_TICK, SOLO_MAX_FRACTION * total))
        shares = {only: cap}
        for t in active:
            if t != only:
                shares[t] = 0
        for t in active:
            send(tenants[t].sock, f"credit_set {shares.get(t,0)}")
            clog("assign", tick_id, squad_id, t,
                 assigned=shares.get(t,0), base_pct=CUR_BASE_PCT.get(t,0.0),
                 boost=(CUR_BOOST if CUR_BOOST==t else ""), freed=_freed_ramp)
        return shares

    # ---- 2개 이상: freed 램핑, 부스트 보너스, 가중치 ----
    freed_target = _freed_pct()
    _freed_ramp = _freed_ramp + FREED_RAMP_ALPHA * (freed_target - _freed_ramp)  # 0~100
    bonus_pool = _freed_ramp if BOOST_ALLOWED and CUR_BOOST else 0.0

    pct_map: Dict[str, float] = {}
    for t in active:
        if t in CUR_FINISHED:
            pct_map[t] = 0.0
        else:
            base  = CUR_BASE_PCT.get(t, 0.0)
            bonus = (bonus_pool if (CUR_BOOST == t) else 0.0)
            pct_map[t] = base + bonus

    shares = weighted_split_int_by_pct(active, total, pct_map)

    # 최소 크레딧 보장(옵션)
    if MIN_CREDIT_PER_TICK > 0:
        for t in active:
            if t not in CUR_FINISHED:
                shares[t] = max(shares.get(t,0), MIN_CREDIT_PER_TICK)

    for t in active:
        send(tenants[t].sock, f"credit_set {shares.get(t,0)}")
        clog("assign", tick_id, squad_id, t,
             assigned=shares.get(t,0), base_pct=CUR_BASE_PCT.get(t,0.0),
             boost=(CUR_BOOST if CUR_BOOST==t else ""), freed=_freed_ramp)
    return shares


# ======================================================================
# Planning core: requirement from goals (math model)
# ======================================================================
def c_req_from_goals(tid: str, g: dict, total: int) -> int:
    """ compute required credit for tenant given goals & models """
    # minimum
    cmin = max(1, MIN_CREDIT_PER_TICK)
    # throughput -> alpha
    thr = g.get("target_qps")
    alpha = estimate_alpha(tid)
    creq_thr = None
    if thr is not None and alpha and alpha>0:
        creq_thr = thr / alpha

    # latency -> u,v
    lat_dead = g.get("deadline_ms")
    update_latency_hist(tid)
    uv = fit_latency_uv(tid)
    creq_lat = None
    if lat_dead is not None and uv:
        u,v = uv
        denom = max(1e-6, lat_dead - v)
        creq_lat = u / denom

    req = cmin
    for x in (creq_thr, creq_lat):
        if x is not None and math.isfinite(x):
            req = max(req, float(x))
    # clamp (no single tenant > 90% by design)
    req = min(req, 0.9 * total)
    return int(max(cmin, math.floor(req)))

def violation_score(tid: str, g: dict, c_alloc: int) -> float:
    """ estimate violation score with current models under c_alloc """
    score = 0.0
    # throughput
    thr = g.get("target_qps")
    if thr is not None and thr>0:
        a = estimate_alpha(tid)
        if a:
            q_pred = a * c_alloc
            score += ALPHA_THR_W * max(0.0, (thr - q_pred)/thr)
    # latency
    lat_dead = g.get("deadline_ms")
    if lat_dead is not None and lat_dead>0:
        uv = fit_latency_uv(tid)
        if uv:
            u,v = uv
            Lp = u/max(1.0, c_alloc) + v
            score += ALPHA_LAT_W * max(0.0, (Lp - lat_dead)/lat_dead)
    return float(score)

# ----------------------------------------------------------------------
# (B1) Lexicographic priority planner
# ----------------------------------------------------------------------
def plan_lexi(active: List[str], goals: Dict[str,dict], total: int, base_alloc: Dict[str,int]) -> Tuple[Dict[str,int], Dict[str,float], float, Optional[str], Dict]:
    """
    Return (credits, squad_pct, tick_new, boost_target, debug)
    """
    debug = {}
    # Step 1: required credits from goals (or floor)
    req = {}
    for t in active:
        g = goals.get(t, {})
        req[t] = c_req_from_goals(t, g, total)

    # Step 2: start from base_alloc (current or min)
    desired = dict(base_alloc)

    # Helper: steal from donors up to their stealable cap
    def steal(pool_from: List[str], need: int) -> Dict[str,int]:
        if need <= 0: return {}
        caps = {}
        total_cap = 0
        for d in pool_from:
            frac = donor_steal_frac(d)
            floor_by_frac = int((1.0 - frac) * desired[d])
            floor_by_min  = MIN_CREDIT_PER_TICK
            floor = max(floor_by_min, floor_by_frac)
            cap = max(0, desired[d] - floor)
            caps[d] = cap; total_cap += cap
        take = {}
        if total_cap <= 0: return {}
        for d in pool_from:
            w = caps[d] / total_cap if total_cap>0 else 0.0
            take[d] = int(min(caps[d], round(w * need)))
        # adjust rounding
        diff = need - sum(take.values())
        if diff>0:
            # still need more: take greedily
            for d,_cap in sorted(caps.items(), key=lambda kv: -kv[1]):
                if diff<=0: break
                more = min(diff, caps[d] - take.get(d,0))
                if more>0: take[d]=take.get(d,0)+more; diff-=more
        return take

    # Priority partitions
    H = [t for t in active if get_prio(t)=="HIGH"]
    M = [t for t in active if get_prio(t)=="MED"]
    L = [t for t in active if get_prio(t)=="LOW"]

    # Function to satisfy a level S using donors D
    def satisfy(level: List[str], donors: List[str]):
        nonlocal desired
        need = sum(max(0, req[t]-desired[t]) for t in level)
        if need<=0: return
        take = steal(donors, need)
        got = sum(take.values())
        for d,v in take.items():
            desired[d] -= v
        # Distribute to level proportional to (req-desired) (or priority weight)
        remaining_need = need
        if got>0:
            # proportion by gap
            gaps = {t: max(0, req[t]-desired[t]) for t in level}
            s = sum(gaps.values()) or 1
            for t in level:
                add = int(round(gaps[t] * got / s))
                desired[t] += add
                remaining_need -= add
        # If still lacking, distribute leftover equally within level (best effort)
        k = 0
        while remaining_need>0 and level:
            t = level[k % len(level)]
            desired[t] += 1; remaining_need -= 1; k+=1

    # Order: HIGH gets from M+L, then MED from L
    satisfy(H, M+L)
    satisfy(M, L)

    # Clamp to totals
    s = sum(desired.values())
    if s != total and s>0:
        scale = total / s
        for t in desired:
            desired[t] = int(max(MIN_CREDIT_PER_TICK, desired[t]*scale))
        # fix rounding
        diff = total - sum(desired.values())
        keys = sorted(desired.keys(), key=lambda x: -prio_weight(x))
        i=0
        while diff!=0 and keys:
            t = keys[i % len(keys)]
            desired[t] += 1 if diff>0 else -1
            diff += -1 if diff>0 else 1
            i+=1

    # Squad % ~= credit share
    pct = {t: 100.0*desired[t]/total for t in desired}

    # Global violation score & tick adapt
    V = 0.0
    for t in active:
        V += prio_weight(t) * violation_score(t, goals.get(t,{}), desired[t])
    tick_new = max(TICK_MIN, min(TICK_MAX, TICK_S / (1.0 + TICK_KAPPA*V))) if V>0 else None

    # Boost target: max weighted violation
    viol_by_t = {t: prio_weight(t)*violation_score(t, goals.get(t,{}), desired[t]) for t in active}
    bst = max(viol_by_t.keys(), key=lambda x: viol_by_t[x]) if active else None

    debug["req"]=req; debug["V"]=V; debug["viol"]=viol_by_t
    return desired, pct, tick_new, bst, debug

# ----------------------------------------------------------------------
# (B2) Weighted waterfilling (coarse, gradient-like)
# ----------------------------------------------------------------------
def plan_wf(active: List[str], goals: Dict[str,dict], total: int, base_alloc: Dict[str,int]) -> Tuple[Dict[str,int], Dict[str,float], float, Optional[str], Dict]:
    """
    Greedy allocate remaining budget by largest marginal gain (negative gradient).
    """
    debug={}
    # start from floors (min) or base
    desired = {t: max(MIN_CREDIT_PER_TICK, base_alloc.get(t, MIN_CREDIT_PER_TICK)) for t in active}
    budget = total - sum(desired.values())
    if budget < 0:
        # scale down proportionally but keep min
        s = sum(desired.values())
        for t in desired:
            desired[t] = int(max(MIN_CREDIT_PER_TICK, desired[t] * total / max(1,s)))
        budget = total - sum(desired.values())

    def grad(t: str, c: int) -> float:
        """
        Approximate -∂J/∂c (positive = benefit if we add credit).
        """
        g = goals.get(t, {})
        w  = prio_weight(t)
        val = 0.0
        # throughput term
        T = g.get("target_qps")
        if T and T>0:
            a = estimate_alpha(t)
            if a:
                # If unmet, gain ~ w*ALPHA_THR_W* a/T
                q_pred = a*c
                if q_pred < T:
                    val += w * ALPHA_THR_W * (a / T)
        # latency term
        D = g.get("deadline_ms")
        if D and D>0:
            uv = fit_latency_uv(t)
            if uv:
                u,v = uv
                Lp = u/max(1.0,c) + v
                if Lp > D:
                    # d/dc (u/c) = -u/c^2, penalty scale 1/D
                    val += w * ALPHA_LAT_W * (u / (D * (c**2)))
        return val

    step = max(1, total//256)  # coarse step to keep overhead small
    iters = 0; max_iters = 4096
    while budget > 0 and iters < max_iters:
        # pick tenant with largest marginal benefit
        t_best = None; g_best = 0.0
        for t in active:
            gval = grad(t, desired[t])
            if gval > g_best + 1e-12:
                g_best = gval; t_best = t
        if not t_best:
            break
        add = min(step, budget)
        desired[t_best] = min(desired[t_best] + add, int(0.9*total))
        budget -= add
        iters += 1

    # final normalization (just in case)
    s = sum(desired.values())
    if s != total and s>0:
        scale = total / s
        for t in desired:
            desired[t] = int(max(MIN_CREDIT_PER_TICK, desired[t]*scale))
        diff = total - sum(desired.values())
        keys = sorted(desired.keys(), key=lambda x: -prio_weight(x))
        i=0
        while diff!=0 and keys:
            t = keys[i % len(keys)]
            desired[t] += 1 if diff>0 else -1
            diff += -1 if diff>0 else 1
            i+=1

    pct = {t: 100.0*desired[t]/total for t in desired}
    # tick & boost like lexi
    V = 0.0
    for t in active:
        V += prio_weight(t) * violation_score(t, goals.get(t,{}), desired[t])
    tick_new = max(TICK_MIN, min(TICK_MAX, TICK_S / (1.0 + TICK_KAPPA*V))) if V>0 else None
    viol_by_t = {t: prio_weight(t)*violation_score(t, goals.get(t,{}), desired[t]) for t in active}
    bst = max(viol_by_t.keys(), key=lambda x: viol_by_t[x]) if active else None
    debug["iters"]=iters; debug["V"]=V; debug["viol"]=viol_by_t
    return desired, pct, tick_new, bst, debug

# ======================================================================
# Apply plan to runtime
# ======================================================================
def _apply_plan(plan: dict, enqueue_restart: bool=False) -> Dict:
    global TICK_S, CREDIT_PER_TICK, MIN_CREDIT_PER_TICK, EQUAL_SHARE, BOOST_ALLOWED, CUR_BOOST, TARGET_W
    applied = []
    if "tick_s" in plan:
        try:
            TICK_S = float(plan["tick_s"])
            inbox_events.append(("CTRL_TICK", TICK_S)); applied.append("CTRL_TICK")
        except: pass
    if "min_credit" in plan:
        try:
            MIN_CREDIT_PER_TICK = int(plan["min_credit"])
        except: pass
    b = plan.get("boost")
    if isinstance(b, dict):
        en = b.get("enable", None); tgt= b.get("target", None)
        if en is not None:
            BOOST_ALLOWED = bool(en)
            inbox_events.append(("CTRL_BOOST_ALLOWED", BOOST_ALLOWED)); applied.append("CTRL_BOOST_ALLOWED")
        if tgt and BOOST_ALLOWED:
            CUR_BOOST = str(tgt)
            inbox_events.append(("CTRL_BOOST", CUR_BOOST)); applied.append("CTRL_BOOST")
    cr = plan.get("credits")
    weights_from_credits = None
    if isinstance(cr, dict) and cr:
        try: tot = int(sum(int(v) for v in cr.values()))
        except: tot = int(sum(float(v) for v in cr.values()))
        if tot > 0:
            CREDIT_PER_TICK = tot
            weights_from_credits = {k: (float(v)*100.0/tot) for k,v in cr.items()}
    sq = plan.get("squad_pct") or plan.get("weights") or weights_from_credits
    if isinstance(sq, dict) and sq:
        TARGET_W.clear(); TARGET_W.update({str(k): float(v) for k,v in sq.items()})
        EQUAL_SHARE = False
        inbox_events.append(("CTRL_WEIGHTS", dict(TARGET_W))); applied.append("CTRL_WEIGHTS")
    if plan.get("apply_now") or enqueue_restart:
        inbox_events.append(("CTRL_RESTART", True)); applied.append("CTRL_RESTART")
    return {"ok": True, "applied_events": applied}

# ======================================================================
# HTTP API: /stats /telemetry /plan /goal /goals /priority /model
# ======================================================================
class _API(BaseHTTPRequestHandler):
    def _json(self, code:int, obj:dict):
        b = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(b)))
        self.end_headers()
        self.wfile.write(b)
        try:
            self.wfile.flush()
        except Exception:
            pass
        except (BrokenPipeError, ConnectionResetError):
            # 클라이언트가 먼저 닫은 경우: 조용히 무시
            pass        

    def _send_to_tenant(tid: str, msg: str) -> bool:
        t = tenants.get(tid)
        if not t or not t.alive: return False
        return send(t.sock, msg)

    def do_GET(self):
        if self.path == "/stats":
            alive = [tid for tid,info in tenants.items() if info.alive]
            self._json(200, {
                "tick_s": TICK_S, "credits_per_tick": CREDIT_PER_TICK,
                "min_credit": MIN_CREDIT_PER_TICK, "equal_share": int(EQUAL_SHARE),
                "target_w": TARGET_W, "cur_base_pct": CUR_BASE_PCT,
                "boost_allowed": BOOST_ALLOWED, "cur_boost": CUR_BOOST,
                "alive": alive, "last_credit_alloc": LAST_CRED_ALLOC,
                "prio": PRIO
            }); return
        if self.path == "/model":
            m = {}
            for t in tenants:
                a = estimate_alpha(t)
                uv = fit_latency_uv(t)
                m[t] = {"alpha": a, "lat_uv": uv}
            self._json(200, {"policy": GOAL_POLICY, "model": m}); return
        if self.path == "/priority":
            self._json(200, {"prio": PRIO, "weights": PRIO_W, "steal_frac": STEAL_FRAC}); return
        self._json(404, {"error":"not found"})

    def do_POST(self):
        length = int(self.headers.get("Content-Length","0") or "0")
        try:
            body = json.loads(self.rfile.read(length) or b"{}")
        except Exception as e:
            self._json(400, {"error": f"bad json: {e}"}); return

        if self.path == "/telemetry":
            tid = body.get("tenant")
            if not tid or tid not in tenants or not tenants[tid].alive:
                self._json(400, {"error":"unknown or inactive tenant"}); return
            t = TELEM.get(tid) or Tele()
            t.qps       = _ema(t.qps,      body.get("qps"))
            t.lat_p90   = _ema(t.lat_p90,  body.get("lat_p90"))
            t.tok_per_s = _ema(t.tok_per_s,body.get("tok_per_s"))
            t.imgs_per_s= _ema(t.imgs_per_s,body.get("imgs_per_s"))
            t.ts = time.time()
            TELEM[tid] = t
            update_latency_hist(tid)
            self._json(200, {"ok": True}); return

        if self.path == "/plan":
            res = _apply_plan(body, enqueue_restart=bool(body.get("apply_now", False)))
            self._json(200, res); return

        if self.path == "/priority":
            # {tenant:"A", priority:"HIGH"} or {"map":{"A":"HIGH","B":"MED"}}
            mp = {}
            if "tenant" in body and "priority" in body:
                mp[body["tenant"]] = str(body["priority"]).upper()
            mp.update({k:str(v).upper() for k,v in (body.get("map") or {}).items()})
            for k,v in mp.items():
                if k in tenants: PRIO[k] = "HIGH" if v=="HIGH" else ("LOW" if v=="LOW" else "MED")
            self._json(200, {"ok": True, "prio": PRIO}); return
        elif self.path == "/sm":
            # JSON: {"tenant": "A", "sm_pct": 30}
            body = self._read_json()
            tid  = body.get("tenant","").strip()
            pct  = float(body.get("sm_pct", -1))
            if not tid or pct <= 0 or pct >= 100:
                self._json(400, {"ok":False, "err":"bad_args"}); return
            ok = _send_to_tenant(tid, f"set_limit_pct {pct}")
            # 주의: libbless는 any_alloc 이후 무시. 여기서는 낙관적 ack + 힌트 제공
            note = "sent set_limit_pct; may be ignored if allocations already exist"
            if not ok:
                self._json(404, {"ok":False, "err":"tenant_not_found"}); return
            # 동적 변경 실패 시를 대비해, 동일 효과에 가까운 시간공유 보정도 같이 적용 (선택)
            # 예: 목표 pct를 base weight로 반영
            alive = [k for k,v in tenants.items() if v.alive]
            weights = {k: (pct if k==tid else (100.0 - pct)/max(1,len(alive)-1)) for k in alive}
            TARGET_W.clear(); TARGET_W.update(weights);  # 전역 목표 가중치 교체
            inbox_events.append(("CTRL_WEIGHTS", dict(TARGET_W)))
            _ = push_credits(alive, tick_id=_g_tick_id, squad_id=0)  # 즉시 한 틱 재분배
            self._json(200, {"ok":True, "applied":True, "note":note, "weights_applied":weights}); return

        elif self.path == "/sm_count":
            # JSON: {"tenant":"B","sms":20}
            body = self._read_json()
            tid  = body.get("tenant","").strip()
            sms  = int(body.get("sms",-1))
            if not tid or sms <= 0:
                self._json(400, {"ok":False, "err":"bad_args"}); return
            ok = _send_to_tenant(tid, f"reconf_sm {sms}")
            note = "sent reconf_sm; will be ignored if allocations already exist"
            if not ok:
                self._json(404, {"ok":False, "err":"tenant_not_found"}); return
            self._json(200, {"ok":True, "applied":True, "note":note}); return

        if self.path in ("/goal","/goals"):
            # single or multi goals
            active = [tid for tid,info in tenants.items() if info.alive]
            if not active:
                self._json(400, {"ok": False, "error":"no active tenants"}); return

            # normalize goals dict
            goals: Dict[str,dict] = {}
            if self.path == "/goal":
                t = body.get("tenant"); 
                if not t: self._json(400, {"ok":False,"error":"tenant missing"}); return
                goals[t] = {k:v for k,v in body.items() if k not in ("tenant","apply_now")}
            else:
                # {goals: {"A":{...},"B":{...}}}
                goals = body.get("goals") or {}
                if not isinstance(goals, dict) or not goals:
                    self._json(400, {"ok":False,"error":"goals map missing"}); return

            total = max(1, CREDIT_PER_TICK)
            base_alloc = {t: int(LAST_CRED_ALLOC.get(t, max(MIN_CREDIT_PER_TICK, total//max(1,len(active))))) for t in active}

            if GOAL_POLICY.lower() == "wf":
                credits, pct, tick_new, boost_t, dbg = plan_wf(active, goals, total, base_alloc)
            else:
                credits, pct, tick_new, boost_t, dbg = plan_lexi(active, goals, total, base_alloc)

            # jitter (optional)
            if JITTER_PCT > 0.0:
                for k in credits:
                    jitter = int(credits[k] * (random.uniform(-JITTER_PCT, JITTER_PCT)))
                    credits[k] = max(MIN_CREDIT_PER_TICK, credits[k] + jitter)
                # re-norm
                s = sum(credits.values())
                if s>0:
                    scale = total / s
                    for k in credits:
                        credits[k] = int(max(MIN_CREDIT_PER_TICK, credits[k]*scale))

            plan = {
                "credits": credits,
                "squad_pct": pct,
                "min_credit": MIN_CREDIT_PER_TICK,
                "boost": {"enable": True, "target": boost_t} if boost_t else {"enable": True},
                "apply_now": bool(body.get("apply_now", True))
            }
            if tick_new is not None:
                plan["tick_s"] = float(f"{tick_new:.6f}")

            res = _apply_plan(plan, enqueue_restart=plan["apply_now"])
            res.update({"ok": True, "plan": plan, "debug": dbg, "policy": GOAL_POLICY})
            self._json(200, res); return

        self._json(404, {"error":"not found"})

# ======================================================================
# Main loop
# ======================================================================
def wait_tenants():
    deadline = time.time() + 30.0
    while True:
        _recv_all()
        if any(t.alive for t in tenants.values()): break
        if time.time() > deadline: break
        time.sleep(0.05)

def _start_api():
    try:
        httpd = HTTPServer((API_ADDR, API_PORT), _API)
    except Exception as e:
        print(f"[api] failed to bind {API_ADDR}:{API_PORT}: {e}")
        return
    print(f"[api] listening http://{API_ADDR}:{API_PORT}")
    httpd.serve_forever()

def main():
    global srv, inbox_events, CUR_BASE_PCT, CUR_FINISHED, CUR_BOOST, BOOST_ALLOWED, _g_tick_id
    # 마스터 소켓 준비
    if os.path.exists(MASTER):
        try: os.unlink(MASTER)
        except: pass
    srv = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    srv.bind(MASTER); srv.settimeout(0.1)

    print(f"[master] listen {MASTER}  tick={TICK_S}s credits/tick={CREDIT_PER_TICK} base={TARGET_W} equal={int(EQUAL_SHARE)}", flush=True)

    # 초기 대기(최초 테넌트 등장까지)
    def any_alive(): return any(t.alive for t in tenants.values())
    idle_sleep = float(os.environ.get("IDLE_SLEEP_S","0.2"))

    squad_id  = 0
    last_tick = time.time()

    while True:
        _recv_all()

        # 활성 집합
        active = [tid for tid,info in tenants.items() if info.alive]

        # --- 아무도 없으면 idle 대기 ---
        if not active:
            # 주기적으로만 trace 남기고 대기
            now = time.time()
            if now - last_tick >= max(TICK_S, 1.0):
                last_tick = now
                # keep-alive 느낌 로그(너무 시끄러우면 주석)
                # print("[master] idle...", flush=True)
            time.sleep(idle_sleep)
            continue

        # --- 틱 크레딧 분배 ---
        now = time.time()
        if now - last_tick >= TICK_S:
            _ = push_credits(active)     # assign 기록 + credit_set 전송
            _g_tick_id += 1
            last_tick = now

        # --- 새 스쿼드 시작 ---
        squad_id += 1
        CUR_FINISHED = set()
        CUR_BOOST    = None
        globals()['_freed_ramp'] = 0.0

        CUR_BASE_PCT = _compute_base_pct(active)
        start_share = weighted_split_int_by_pct(active, SQUAD, CUR_BASE_PCT)
        for t in active:
            send(tenants[t].sock, f"set_squad {SQUAD}")
            send(tenants[t].sock, f"set_share {start_share[t]}")
            send(tenants[t].sock, "squad_reset")
        log("SQUAD_START", squad_id, "", start_share, remain, f"active={active}; base_pct={CUR_BASE_PCT}")

        t0 = time.time()
        while True:
            _recv_all()

            # 틱 크레딧 분배(러닝 중)
            now2 = time.time()
            if now2 - last_tick >= TICK_S:
                _ = push_credits(active)
                _g_tick_id += 1
                last_tick = now2

            restart = False
            while inbox_events:
                ev, arg = inbox_events.pop(0)

                if ev == "SD" and arg in active and arg not in CUR_FINISHED:
                    CUR_FINISHED.add(arg)
                    _rebase_current_for_unfinished(active)
                    if BOOST_ALLOWED:
                        nowt = time.time()
                        if (nowt - globals().get("_boost_last_change", 0.0)) >= BOOST_DEBOUNCE_S:
                            tgt = _pick_boost_target(active)
                            if tgt is not None:
                                globals()["_boost_last_change"] = nowt
                                globals()["_boost_last_tid"]    = tgt
                                CUR_BOOST = tgt
                                log("BOOST_SET", squad_id, tgt, start_share, remain, f"cause=SD:{arg}")
                    _ = push_credits(active); _g_tick_id += 1; last_tick = time.time()

                elif ev == "BYE":
                    if arg in active: restart = True

                elif ev == "CTRL_CRED_OFF":
                    for t in active:
                        send(tenants[t].sock, "credit_off")
                    last_tick = time.time()
                    print("[ctrl] CREDIT_OFF broadcast", flush=True)

                elif ev in ("CTRL_EQUAL","CTRL_WEIGHTS","CTRL_TICK","CTRL_CRED","CTRL_BOOST","CTRL_BOOST_ALLOWED"):
                    if ev == "CTRL_BOOST":
                        if BOOST_ALLOWED and isinstance(arg, str):
                            CUR_BOOST = arg if (arg in active) else None
                        else:
                            CUR_BOOST = None
                    if ev in ("CTRL_EQUAL","CTRL_WEIGHTS"):
                        _rebase_current_for_unfinished(active)
                    _ = push_credits(active); _g_tick_id += 1; last_tick = time.time()
                    log(ev, squad_id, str(arg) if not isinstance(arg, dict) else "", start_share, remain,
                        f"TICK={TICK_S}, CRED={CREDIT_PER_TICK}, TARGET_W={TARGET_W}, base_pct={CUR_BASE_PCT}, boost_allowed={BOOST_ALLOWED}")

            # 토폴로지 변경(누군가 종료) → 스쿼드 재시작
            if restart:
                log("SQUAD_RESTART", squad_id, "", start_share, remain, "topology change")
                break

            # 모두 SD면 스쿼드 종료
            if len(CUR_FINISHED) == len(active):
                log("SQUAD_END", squad_id, "", start_share, remain, f"boosted={CUR_BOOST}")
                break

            # 타임아웃으로 스쿼드 종료(다음 라운드)
            if time.time() - t0 > SQUAD_TMO_S:
                log("SQUAD_END_TIMEOUT", squad_id, "", start_share, remain,
                    f"finished={sorted(list(CUR_FINISHED))},boosted={CUR_BOOST}")
                break

    csv_f.close()
    credit_f.close()
    try:
        srv.close()
        if os.path.exists(MASTER): os.unlink(MASTER)
    except: pass
    try:
        if _snd: _snd.close()
    except: pass

if __name__ == "__main__":
    main()
