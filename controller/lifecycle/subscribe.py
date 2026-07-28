"""phase_online 이벤트 구독 배선 (Exp_26 작업 2-(b)).

phase_online/detector_proc 가 남기는 events JSONL 을 tail-follow 하고,
transition 이벤트마다 등록된 핸들러를 호출한다. **자동 정책 판단은 여기
없음** — 어떤 액션을 취할지는 핸들러(수동 등록) 몫 (전략 선택은 Agent 영역,
PROJECT_CONTEXT §5.3). 기본 제공 핸들러는 adaptive_map 의
phase_transition_actions '규칙 따르기'까지만 (Exp_20 폐루프와 동일 수준).

tail 방식은 Exp_22 run_pipeline.py controller() (:133-152, md5 05a44065)의
파일 폴링 이식 (50ms — 원본 동일).
"""
import json
import os
import threading
import time

POLL_S = 0.05   # Exp_22 controller() 폴링 주기 동일


class EventSubscriber:
    def __init__(self, events_path, handler, audit=None, match=None):
        """handler(event_dict) — match 를 통과한 이벤트마다 호출.

        match(event)->bool: 기본 None = 기존 동작(transition 이벤트만 —
        Exp_26 하위호환). [Exp_28] TERM_PREDICTED 등 다른 스키마 구독을
        위해 최소 확장 — 기존 호출부 무영향 (기본값 경로 동일).
        """
        self.events_path = events_path
        self.handler = handler
        self.audit = audit if audit is not None else []
        self.match = match if match is not None else (
            lambda e: bool(e.get("transition")))
        self._stop = threading.Event()
        self._thread = None

    def start(self):
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=2.0)

    def _run(self):
        pos = 0
        while not self._stop.is_set():
            try:
                lines = open(self.events_path).readlines()
            except OSError:
                time.sleep(POLL_S)
                continue
            for line in lines[pos:]:
                pos += 1
                try:
                    e = json.loads(line)
                except ValueError:
                    continue
                if self.match(e):
                    self.audit.append({"t": round(time.time(), 3),
                                       "kind": "event_seen",
                                       "event": e})
                    try:
                        self.handler(e)
                    except Exception as ex:      # 핸들러 오류는 구독을 죽이지 않음
                        self.audit.append({"t": round(time.time(), 3),
                                           "kind": "handler_error",
                                           "error": repr(ex)})
            time.sleep(POLL_S)
