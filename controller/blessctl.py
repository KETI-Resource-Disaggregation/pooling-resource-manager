#!/usr/bin/env python3
import os
import sys
import socket
import time
import errno
import argparse
from typing import List, Tuple, Dict, Optional

# --------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------
def sock_path_from_pid(pid: int) -> str:
    return f"/tmp/bless-{pid}.sock"

def send_to_sock(path: str, msg: str, retries: int = 30, delay: float = 0.2) -> bool:
    data = msg.encode()
    for _ in range(retries):
        try:
            s = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
            s.sendto(data, path)
            s.close()
            return True
        except OSError as e:
            if e.errno in (errno.ENOENT, errno.ECONNREFUSED):
                time.sleep(delay)
                continue
            raise
    return False

def send_to_pid(pid: int, msg: str, retries: int = 30, delay: float = 0.2) -> bool:
    return send_to_sock(sock_path_from_pid(pid), msg, retries=retries, delay=delay)

def list_bless_sockets() -> List[Tuple[int, str]]:
    """Scan /tmp/bless-*.sock and return [(pid, path), ...] that exist and pid is running."""
    out: List[Tuple[int, str]] = []
    try:
        for name in os.listdir("/tmp"):
            if not name.startswith("bless-") or not name.endswith(".sock"):
                continue
            pid_str = name[len("bless-"):-len(".sock")]
            if not pid_str.isdigit():
                continue
            pid = int(pid_str)
            path = os.path.join("/tmp", name)
            if os.path.exists(f"/proc/{pid}") and os.path.exists(path):
                out.append((pid, path))
    except FileNotFoundError:
        pass
    return sorted(out, key=lambda t: t[0])

def read_env_for_pid(pid: int) -> Dict[str, str]:
    """Read /proc/<pid>/environ into dict; best-effort (requires permissions)."""
    env: Dict[str, str] = {}
    try:
        with open(f"/proc/{pid}/environ", "rb") as f:
            raw = f.read().split(b"\x00")
        for b in raw:
            if not b:
                continue
            try:
                s = b.decode(errors="ignore")
            except Exception:
                continue
            if "=" in s:
                k, v = s.split("=", 1)
                env[k] = v
    except Exception:
        pass
    return env

def discover_by_tenant(tenants: List[str]) -> List[int]:
    """Map BLESS_TENANT to PIDs by scanning /proc; returns list of PIDs (may be empty)."""
    targets: List[int] = []
    # quick path: match via bless sockets + environ
    for pid, _path in list_bless_sockets():
        env = read_env_for_pid(pid)
        tname = env.get("BLESS_TENANT", "")
        if tname in tenants:
            targets.append(pid)
    # de-dup
    return sorted(list(set(targets)))

def format_table(rows: List[Tuple[int, str, str]]) -> str:
    # rows: (pid, tenant, sock)
    if not rows:
        return "(no bless tenants found)"
    pid_w = max(3, max(len(str(r[0])) for r in rows))
    ten_w = max(6, max(len(r[1]) for r in rows))
    lines = [f"{'PID'.ljust(pid_w)}  {'TENANT'.ljust(ten_w)}  SOCK"]
    for pid, ten, sockp in rows:
        lines.append(f"{str(pid).ljust(pid_w)}  {ten.ljust(ten_w)}  {sockp}")
    return "\n".join(lines)

# --------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------
def build_argparser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Send control commands to libbless tenants."
    )
    g = p.add_mutually_exclusive_group()
    g.add_argument("-p", "--pid", type=int, nargs="+", help="Target PID(s)")
    g.add_argument("-t", "--tenant", type=str, nargs="+", help="Target by BLESS_TENANT name(s)")
    g.add_argument("-S", "--sock", type=str, nargs="+", help="Target by explicit UNIX socket path(s)")
    g.add_argument("-A", "--all", action="store_true", help="Broadcast to all discovered bless sockets")

    p.add_argument("--list", action="store_true", help="List discovered bless tenants and exit")
    p.add_argument("--retries", type=int, default=30, help="Send retries (default: 30)")
    p.add_argument("--delay", type=float, default=0.2, help="Delay between retries in seconds (default: 0.2)")

    # Backward-compatible positional form: blessctl <pid> <message...>
    p.add_argument("positional", nargs="*", help="(compat) <pid> <message...>")
    return p

def main():
    ap = build_argparser()
    args = ap.parse_args()

    # --list mode
    if args.list:
        rows: List[Tuple[int, str, str]] = []
        for pid, path in list_bless_sockets():
            env = read_env_for_pid(pid)
            rows.append((pid, env.get("BLESS_TENANT", ""), path))
        print(format_table(rows))
        return 0

    # Backward-compatible path: blessctl <pid> <message...>
    if not (args.pid or args.tenant or args.sock or args.all):
        if len(args.positional) < 2:
            ap.print_help(sys.stderr)
            print("\nExamples:")
            print("  blessctl -p 12345 set_route 1")
            print("  blessctl -t A pause")
            print("  blessctl -A set_limit_pct 33")
            print("  blessctl 12345 set_share 250   # backward-compat")
            return 2
        try:
            pid = int(args.positional[0])
        except ValueError:
            print("error: first positional must be PID when using compat mode", file=sys.stderr)
            return 2
        msg = " ".join(args.positional[1:])
        ok = send_to_pid(pid, msg, retries=args.retries, delay=args.delay)
        if not ok:
            print(f"send failed -> {sock_path_from_pid(pid)}: {msg}", file=sys.stderr)
            return 3
        return 0

    # Resolve targets
    targets_sock: List[str] = []
    targets_pid: List[int] = []

    if args.all:
        for pid, path in list_bless_sockets():
            targets_sock.append(path)

    if args.pid:
        targets_pid.extend(args.pid)

    if args.tenant:
        pids = discover_by_tenant(args.tenant)
        if not pids:
            print(f"[warn] no PID found for tenant(s): {', '.join(args.tenant)}", file=sys.stderr)
        targets_pid.extend(pids)

    if args.sock:
        targets_sock.extend(args.sock)

    # Unique-ify & normalize
    targets_pid = sorted(list(set(targets_pid)))
    targets_sock = sorted(list(set(targets_sock)))
    # add pid->sock mapping
    for pid in targets_pid:
        targets_sock.append(sock_path_from_pid(pid))
    targets_sock = sorted(list(set(targets_sock)))

    # Require message
    msg_parts = args.positional
    if not msg_parts:
        print("error: missing message to send", file=sys.stderr)
        return 2
    msg = " ".join(msg_parts)

    if not targets_sock:
        print("error: no targets resolved (use -p/--pid, -t/--tenant, -S/--sock, or --all)", file=sys.stderr)
        return 2

    # Send
    ok_any = False
    for path in targets_sock:
        ok = send_to_sock(path, msg, retries=args.retries, delay=args.delay)
        print(f"[send] {path}: {'OK' if ok else 'FAIL'}  :: {msg}")
        ok_any = ok_any or ok

    return 0 if ok_any else 3

if __name__ == "__main__":
    sys.exit(main())
