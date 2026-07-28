"""controller.ratio — 비율 결정 엔진 v1 + adaptive_map 조회 (Exp_25)
+ 페어 봉투 정책 (Exp_40)."""
from .engine import decide, decide_for_device, bless_limit_pct
from .pair import decide_pair, POLICIES
