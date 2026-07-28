#!/usr/bin/env python3
# schedctl.py — PRISM 컨트롤러 CLI
#
# 사용 예:
#   python schedctl.py register A --sm 40 --mem 4096 --weight 1.0
#   python schedctl.py deregister 0
#   python schedctl.py set_weights 0.6 0.4
#   python schedctl.py set_priority 0 2
#   python schedctl.py set_round 50000
#   python schedctl.py set_equal
#   python schedctl.py stats
#   python schedctl.py status
#
# --url : 컨트롤러 HTTP 주소 (기본 http://localhost:8090)

import argparse
import json
import sys
import urllib.request
import urllib.error


def call(url: str, path: str, data: dict | None = None) -> dict:
    full = url.rstrip("/") + path
    if data is not None:
        body = json.dumps(data).encode()
        req  = urllib.request.Request(full, data=body,
                                       headers={"Content-Type": "application/json"},
                                       method="POST")
    else:
        req = urllib.request.Request(full, method="GET")

    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        body = e.read()
        try:
            return json.loads(body)
        except Exception:
            return {"error": body.decode()}
    except Exception as e:
        print(f"[schedctl] 연결 실패: {e}", file=sys.stderr)
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(prog="schedctl")
    parser.add_argument("--url", default="http://localhost:8090",
                        help="컨트롤러 주소")
    sub = parser.add_subparsers(dest="cmd", required=True)

    # register
    p = sub.add_parser("register", help="테넌트 등록")
    p.add_argument("tenant_id")
    p.add_argument("--sm",     type=int,   default=40,   help="virtual SM 수")
    p.add_argument("--mem",    type=int,   default=4096, help="virtual 메모리(MB)")
    p.add_argument("--weight", type=float, default=1.0,  help="스케줄링 가중치")

    # deregister
    p = sub.add_parser("deregister", help="테넌트 해제")
    p.add_argument("tenant_idx", type=int)

    # set_weights
    p = sub.add_parser("set_weights", help="시간 가중치 설정")
    p.add_argument("weights", nargs="+", type=float)

    # set_priority
    p = sub.add_parser("set_priority", help="우선순위 설정 (0=LOW 1=MED 2=HIGH)")
    p.add_argument("tenant_idx", type=int)
    p.add_argument("priority",   type=int)

    # set_round
    p = sub.add_parser("set_round", help="라운드 길이 설정(μs)")
    p.add_argument("duration_us", type=int)

    # set_equal
    sub.add_parser("set_equal", help="동일 가중치 적용")

    # stats / status / tenants
    sub.add_parser("stats",   help="테넌트별 실행 통계")
    sub.add_parser("status",  help="전체 SHM 상태")
    sub.add_parser("tenants", help="등록된 테넌트 목록")

    # profiling
    p = sub.add_parser("profiling_enter", help="PROFILING 모드 진입")
    p.add_argument("tenant_idx", type=int)
    p.add_argument("--iters", type=int, default=5)

    sub.add_parser("profiling_exit", help="PROFILING 모드 종료")

    args = parser.parse_args()
    url  = args.url

    if args.cmd == "register":
        r = call(url, "/register", {
            "tenant_id":    args.tenant_id,
            "virtual_sm":  args.sm,
            "virtual_mem_mb": args.mem,
            "weight":       args.weight,
        })
        print(json.dumps(r, indent=2))

    elif args.cmd == "deregister":
        r = call(url, "/deregister", {"tenant_idx": args.tenant_idx})
        print(json.dumps(r, indent=2))

    elif args.cmd == "set_weights":
        r = call(url, "/policy/weights", {"weights": args.weights})
        print(json.dumps(r, indent=2))

    elif args.cmd == "set_priority":
        r = call(url, "/policy/priority",
                 {"tenant_idx": args.tenant_idx, "priority": args.priority})
        print(json.dumps(r, indent=2))

    elif args.cmd == "set_round":
        r = call(url, "/policy/round", {"duration_us": args.duration_us})
        print(json.dumps(r, indent=2))

    elif args.cmd == "set_equal":
        r = call(url, "/policy/equal", {})
        print(json.dumps(r, indent=2))

    elif args.cmd == "stats":
        r = call(url, "/stats")
        print(json.dumps(r, indent=2))

    elif args.cmd == "status":
        r = call(url, "/status")
        print(json.dumps(r, indent=2))

    elif args.cmd == "tenants":
        r = call(url, "/tenants")
        print(json.dumps(r, indent=2))

    elif args.cmd == "profiling_enter":
        r = call(url, "/profiling/enter",
                 {"tenant_idx": args.tenant_idx, "iter_count": args.iters})
        print(json.dumps(r, indent=2))

    elif args.cmd == "profiling_exit":
        r = call(url, "/profiling/exit", {})
        print(json.dumps(r, indent=2))


if __name__ == "__main__":
    main()
