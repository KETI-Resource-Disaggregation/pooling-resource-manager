#!/usr/bin/env python3
"""[Exp_138 2-B 5] ③ 모드 전환 이벤트 발행 — 파드 안에서 K8s API 직접 호출.

★왜 kubectl 이 아닌가. closed_loop.py 의 emit_event 는 호스트에서 돌기 때문에
  `kubectl create -f -` 를 쓸 수 있었다. ③ 의 전환은 device-plugin 파드 안에서
  일어나고 그 컨테이너에는 kubectl 이 없다. 서비스 어카운트로 API 를 직접 친다
  (stderr_signals.py 와 같은 방식 — 새 의존을 만들지 않는다).

★발행 실패는 제어를 막지 않는다. 다만 조용히 넘기지 않는다(T-5) —
  호출자가 경고를 남기도록 예외를 올린다.
"""
import json
import os
import ssl
import urllib.error
import urllib.request

SA = "/var/run/secrets/kubernetes.io/serviceaccount"
NODE = os.environ.get("KRAKEN_NODE_NAME", "")


def _api_base():
    h = os.environ.get("KUBERNETES_SERVICE_HOST", "")
    p = os.environ.get("KUBERNETES_SERVICE_PORT", "443")
    return "https://%s:%s" % (h, p) if h else ""


def _token():
    try:
        return open(os.path.join(SA, "token")).read().strip()
    except Exception:
        return ""


def _ctx():
    ca = os.path.join(SA, "ca.crt")
    return ssl.create_default_context(cafile=ca) if os.path.exists(ca) else None


def emit(reason, message, tenant="", warn=False, namespace="kube-system",
         pod="", pod_ns="", event_name="", component="kraken-bless-feeder"):
    """K8s Event 발행. 성공 True, 중복(409) "dup", 실패 시 예외.

    [Exp_140 3부] 일반화 — 기존 호출(ControlModeSwitched, Node 대상, generateName)은
    기본 인자로 동작이 한 비트도 안 바뀐다. 추가:
      pod/pod_ns   — 주면 involvedObject 를 그 Pod 로, Event 도 그 네임스페이스에
                     만든다(A-4: Slice*/ControlModeSwitched 의 대상은 Pod.
                     kubectl describe pod 에서 보이려면 같은 ns 여야 한다).
      event_name   — 주면 generateName 대신 결정적 이름으로 만든다. 재발행은
                     API 가 409(AlreadyExists)로 거르므로 중복 발행이 멱등해진다
                     (파드 재시작·watcher 재조정·플러그인 재기동 경로).
    """
    base = _api_base()
    tok = _token()
    if not base or not tok:
        raise RuntimeError("클러스터 내부가 아니거나 서비스 어카운트 없음 "
                           "(api=%r token=%s)" % (base, bool(tok)))
    if pod:
        involved = {"kind": "Pod", "name": pod,
                    "namespace": pod_ns or namespace}
        namespace = pod_ns or namespace
    else:
        node = NODE or os.environ.get("NODE_NAME", "")
        if not node:
            raise RuntimeError("KRAKEN_NODE_NAME 미설정 — involvedObject 를 만들 수 없다")
        involved = {"kind": "Node", "name": node}
    meta = {"namespace": namespace}
    if event_name:
        meta["name"] = event_name
    else:
        meta["generateName"] = "kraken-mode-"
    ev = {
        "apiVersion": "v1", "kind": "Event",
        "metadata": meta,
        "involvedObject": involved,
        "reason": reason, "message": message,
        "type": "Warning" if warn else "Normal",
        "source": {"component": component},
    }
    req = urllib.request.Request(
        "%s/api/v1/namespaces/%s/events" % (base, namespace),
        data=json.dumps(ev).encode(), method="POST",
        headers={"Authorization": "Bearer " + tok,
                 "Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=5, context=_ctx()) as r:
            if r.status not in (200, 201):
                raise RuntimeError("HTTP %s" % r.status)
    except urllib.error.HTTPError as e:
        if e.code == 409 and event_name:
            return "dup"    # 결정적 이름 재발행 — 이미 냈다. 정상 경로
        raise
    return True
