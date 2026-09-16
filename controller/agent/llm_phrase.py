#!/usr/bin/env python3
"""[Exp_145 3-B] 문장 생성 전용 LLM 래퍼 — 판정은 절대 여기 없다.

stdin 으로 규칙이 만든 권고(템플릿)를 받고, 같은 내용을 읽기 쉬운 한국어 한 문단으로
다시 쓴 것만 stdout 에 낸다. **수치·조치·등급을 바꾸지 않는다.**

제약(지시서 3-B):
  · GPU 미사용 — CUDA_VISIBLE_DEVICES="" 를 호출자가 주고, 여기서도 device="cpu" 고정.
    VRAM 을 잡으면 240 LSU 풀에서 빠지고 활용률 측정에 끼어든다
  · bf16 CPU — fp32 대비 23.3s → 14.6s (Exp_145 3-B 실측, 판정 주기 15s 안)
  · temperature 0 — do_sample=False(그리디). 같은 입력에 같은 문장
  · 코어 고정은 호출자(taskset)가 한다
  · 실패는 비정상 종료로 알린다 — 호출자가 템플릿 폴백으로 간다
"""
import argparse
import os
import sys

os.environ.setdefault("CUDA_VISIBLE_DEVICES", "")
os.environ.setdefault("HF_HUB_OFFLINE", "1")
os.environ.setdefault("TOKENIZERS_PARALLELISM", "false")

SYS = ("너는 GPU 자원 풀링 시스템의 운영 보조다. 아래 권고를 운영자에게 읽어주듯 "
       "한국어 평문 2~3문장으로 요약해라. "
       "규칙: 마크다운·목록 기호·제목을 쓰지 마라. 줄바꿈 없이 한 문단으로 써라. "
       "수치·조치·적용 등급을 바꾸거나 새로 지어내지 마라. 없는 값을 추측하지 마라.")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--model", required=True)
    ap.add_argument("--threads", type=int, default=4)
    ap.add_argument("--max-new", type=int, default=120)   # [Exp_145] 생성 시간 ≤ 판정 주기
    a = ap.parse_args()

    text = sys.stdin.read().strip()
    if not text:
        print("입력 없음", file=sys.stderr)
        return 2

    import torch
    from transformers import AutoModelForCausalLM, AutoTokenizer
    torch.set_num_threads(a.threads)

    tok = AutoTokenizer.from_pretrained(a.model)
    model = AutoModelForCausalLM.from_pretrained(
        a.model, dtype=getattr(torch, os.environ.get("KRAKEN_LLM_DTYPE", "bfloat16")), device_map=None).to("cpu").eval()

    msgs = [{"role": "system", "content": SYS}, {"role": "user", "content": text}]
    try:    # Qwen3 계열: 사고 모드 끔 — 판정은 규칙이 하므로 추론 토큰이 불필요하다
        prompt = tok.apply_chat_template(msgs, tokenize=False,
                                         add_generation_prompt=True,
                                         enable_thinking=False)
    except TypeError:
        prompt = tok.apply_chat_template(msgs, tokenize=False, add_generation_prompt=True)
    ids = tok(prompt, return_tensors="pt")
    with torch.no_grad():
        out = model.generate(**ids, max_new_tokens=a.max_new,
                             do_sample=False,             # temperature 0 = 그리디
                             pad_token_id=tok.eos_token_id)
    gen = tok.decode(out[0][ids["input_ids"].shape[1]:], skip_special_tokens=True)
    gen = " ".join(gen.split())          # 줄바꿈·목록 흔적 제거 — 한 문단 강제
    if not gen:
        print("빈 생성", file=sys.stderr)
        return 3
    # 템플릿의 기계 판독부는 그대로 남기고 LLM 문장을 덧댄다 — 수치 근거를 잃지 않는다
    print(text + "\n  [요약] " + gen)
    return 0


if __name__ == "__main__":
    sys.exit(main())
