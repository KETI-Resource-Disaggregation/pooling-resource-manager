"""controller.lifecycle — 상태머신·make-before-break·residual 장부·구독 (Exp_26)."""
from .machine import (StateMachine, SwapOrchestrator, IllegalTransition,
                      NORMAL, GATED, HANDOFF, NORMAL_PRIME, FALLBACK_GATED)
from .ledger import ResidualLedger
from .subscribe import EventSubscriber
