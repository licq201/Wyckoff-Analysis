"""Strategy reflection shadow workflow."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass

from core.strategy_reflection import build_policy_candidate, build_strategy_reflection
from integrations.supabase_signal_feedback import load_policy_shadow_runs, load_recent_signal_outcomes
from integrations.supabase_strategy_reflection import upsert_strategy_policy_candidate, upsert_strategy_reflection


@dataclass(frozen=True)
class StrategyReflectionRequest:
    market: str
    as_of_date: str
    horizon_days: int
    outcome_days: int
    shadow_days: int
    limit: int
    dry_run: bool = False


def strategy_reflection_enabled() -> bool:
    return os.getenv("WYCKOFF_STRATEGY_REFLECTION", "off").strip().lower() == "shadow"


def build_strategy_reflection_payloads(request: StrategyReflectionRequest) -> tuple[dict, dict | None]:
    outcomes = load_recent_signal_outcomes(request.outcome_days, request.limit, request.market)
    shadow_runs = load_policy_shadow_runs(request.shadow_days, request.limit, request.market)
    reflection = build_strategy_reflection(
        outcomes,
        shadow_runs,
        market=request.market,
        as_of_date=request.as_of_date,
        horizon_days=request.horizon_days,
    )
    return reflection, build_policy_candidate(reflection)


def run_strategy_reflection_job(request: StrategyReflectionRequest) -> int:
    from integrations.supabase_base import is_admin_configured

    if not strategy_reflection_enabled():
        print("[strategy_reflection] disabled; set WYCKOFF_STRATEGY_REFLECTION=shadow to enable")
        return 0
    if not is_admin_configured():
        print("[strategy_reflection] Supabase admin 未配置，跳过每周策略反思")
        return 0
    try:
        reflection, candidate = build_strategy_reflection_payloads(request)
        if request.dry_run:
            print(json.dumps({"reflection": reflection, "candidate": candidate}, ensure_ascii=False, indent=2))
            return 0
        reflection_written = upsert_strategy_reflection(reflection)
        candidate_written = upsert_strategy_policy_candidate(candidate)
        status = reflection.get("status", "unknown") if isinstance(reflection, dict) else "unknown"
        print(
            f"[strategy_reflection] written: reflection={reflection_written}, candidate={candidate_written}, status={status}"
        )
        return 0
    except Exception as exc:
        print(f"[strategy_reflection] 执行跳过: {exc}")
        return 0
