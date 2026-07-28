"""urgent 선점 배선 — policy → libbless 소켓 발신 (Exp_27, G5).

libbless 의 선점 로직(priority_queue_gate, libbless.cpp:811-856)은 완비되어
있으나 발신자가 없었다 (Exp_23 §2.2). 이 모듈이 controller 측 발신 경로다.

프로토콜 (libbless.cpp 소스 확인 — 추측 금지 규약):
  "priority_set <n>"  :701-708  n∈{0,1,2}, 0=URGENT 1=NORMAL 2=BACKGROUND.
                       응답 없음 (stderr 로그만) — fire-and-forget DGRAM
  "urgent_request"    :710-714  urgent_pending=true + preempt_requested=true
  "urgent_clear"      :716-720  둘 다 false
  "queue_stats"       :722-727  stderr 덤프

의미론 (소스 근거):
  · urgent_request 는 **상태** — urgent_clear 까지 유지 (1회성 아님)
  · 플래그·큐는 프로세스-로컬 → 교차 테넌트 선점은 controller 팬아웃으로 성립:
    urgent(X) = X 에 priority_set 0 + X 를 제외한 전 테넌트에 urgent_request
    (URGENT 인 프로세스는 urgent_pending 을 무시 — gate :818 분기)
  · 게이트 순서 (:876-896): priority_queue_gate → reconf/pause → time gate.
    time gate 는 urgent 를 검사하지 않음 → urgent 라도 time_credit 소진이면
    블록될 것으로 예상 (3-3 실측 대조 — report §5)
"""
import threading
import time

# libbless.cpp:701-708 등급 어휘 (scheduler.py 의 LOW/MED/HIGH 와 다름 — §결정문)
PRIORITY_CLASSES = {"URGENT": 0, "NORMAL": 1, "BACKGROUND": 2}


class PriorityManager:
    def __init__(self, send_fn, sock_resolver):
        """send_fn(sock_path, msg) — feeder 발신 경로 재사용 (Exp_26 §6).
        sock_resolver(tenant) -> sock_path | None — feeder 등록부 조회."""
        self._send = send_fn
        self._resolve = sock_resolver
        self._lock = threading.Lock()
        self._class = {}          # tenant -> class 문자열 (마지막 발신 기준)
        self._urgent_holder = None
        self._pending_sent = set()  # urgent_request 를 보낸 테넌트
        self.audit = []

    def _log(self, kind, **kw):
        with self._lock:
            self.audit.append({"t": round(time.time(), 3), "kind": kind, **kw})

    def _sock(self, tenant):
        s = self._resolve(tenant)
        if s is None:
            raise KeyError(f"미등록 테넌트: {tenant!r} (feeder 등록 필요)")
        return s

    def set_priority(self, tenant, klass):
        """등급 설정 — libbless 'priority_set <n>'."""
        if klass not in PRIORITY_CLASSES:
            raise ValueError(f"등급은 {sorted(PRIORITY_CLASSES)} 중 하나: {klass!r}")
        sock = self._sock(tenant)
        self._send(sock, f"priority_set {PRIORITY_CLASSES[klass]}")
        with self._lock:
            self._class[tenant] = klass
        self._log("priority_set", tenant=tenant, klass=klass,
                  code=PRIORITY_CLASSES[klass])

    def urgent(self, tenant, others):
        """urgent 선점: tenant 를 URGENT 승격 + others 전원에 urgent_request.

        상태 의미론 — urgent_clear() 호출까지 유지 (libbless :710-720).
        others 는 호출자(loop_api)가 feeder 등록부에서 tenant 제외 목록으로 공급.
        """
        self.set_priority(tenant, "URGENT")
        sent = []
        for o in others:
            if o == tenant:
                continue
            self._send(self._sock(o), "urgent_request")
            sent.append(o)
        with self._lock:
            self._urgent_holder = tenant
            self._pending_sent = set(sent)
        self._log("urgent", tenant=tenant, pending_sent=sent)
        return sent

    def urgent_clear(self):
        """선점 해제 — urgent_request 를 보냈던 테넌트 전원에 urgent_clear."""
        with self._lock:
            targets = sorted(self._pending_sent)
            holder = self._urgent_holder
        for o in targets:
            self._send(self._sock(o), "urgent_clear")
        with self._lock:
            self._urgent_holder = None
            self._pending_sent = set()
        self._log("urgent_clear", was_holder=holder, cleared=targets)
        return targets

    def status(self):
        with self._lock:
            return {"classes": dict(self._class),
                    "urgent_holder": self._urgent_holder,
                    "pending_sent": sorted(self._pending_sent),
                    "audit": self.audit[-30:]}
