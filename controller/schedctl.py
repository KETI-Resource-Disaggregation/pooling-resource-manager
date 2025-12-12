#!/usr/bin/env python3
import os, sys, socket

MASTER = os.environ.get("BLESS_MASTER", "/tmp/bless-master.sock")

def send(msg: str):
    s = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    s.sendto(msg.encode(), MASTER)
    s.close()

def usage():
    print("""usage:
  schedctl set_equal 1|0
  schedctl set_weights A:50,B:30,C:20
  schedctl set_share <TENANT> <PCT>     # 예: set_share A 50  (나머지는 기존 비율로 50%에 맞춰 재스케일)
  schedctl set_tick <seconds>           # 예: set_tick 0.0025
  schedctl set_credits <n>              # 예: set_credits 196608
  schedctl boost <TENANT> | boost_off   # 수동 부스트/해제
""")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        usage(); sys.exit(1)
    cmd = sys.argv[1].lower()
    if cmd == "set_equal" and len(sys.argv)==3:
        send(f"SET_EQUAL {sys.argv[2]}")
    elif cmd == "set_weights" and len(sys.argv)==3:
        send(f"SET_WEIGHTS {sys.argv[2]}")
    elif cmd == "set_share" and len(sys.argv)==4:
        send(f"SET_SHARE {sys.argv[2]} {sys.argv[3]}")
    elif cmd == "set_tick" and len(sys.argv)==3:
        send(f"SET_TICK {sys.argv[2]}")
    elif cmd == "set_credits" and len(sys.argv)==3:
        send(f"SET_CREDITS {sys.argv[2]}")
    elif cmd == "boost" and len(sys.argv)==3:
        send(f"BOOST {sys.argv[2]}")
    elif cmd == "boost_off":
        send("BOOST_OFF")
    else:
        usage(); sys.exit(1)
