# -*- coding: utf-8 -*-
"""
进程内 Agent 执行器 — 替代 GitHub Actions dispatch 的本地模式。

当环境变量 AGENT_MODE=1 时，submit_background_job() 和 sync_background_job_state()
走本模块的实现，在 Streamlit 进程内用 daemon thread 执行后台任务。

优势：
  - 消除 GH Actions 30-90 秒冷启动延迟
  - 所有日志和错误可直接在 Streamlit 进程中查看
  - 无需配置 GITHUB_ACTIONS_TOKEN

注意：
  - 本模式不适合 Community Cloud（内存受限），推荐本地或自建部署使用
  - GH Actions cron 定时任务不受影响，仍走 daily_job.py / run_pipeline.py
"""
from __future__ import annotations

import logging
import threading
import traceback
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

import streamlit as st

logger = logging.getLogger(__name__)

# 进程级全局存储：{run_id: {"status": ..., "result": ..., "error": ...}}
_JOB_STORE: dict[str, dict[str, Any]] = {}
_STORE_LOCK = threading.Lock()


def _store_update(run_id: str, data: dict[str, Any]) -> None:
    with _STORE_LOCK:
        _JOB_STORE[run_id] = {**_JOB_STORE.get(run_id, {}), **data}


def _store_get(run_id: str) -> dict[str, Any] | None:
    with _STORE_LOCK:
        entry = _JOB_STORE.get(run_id)
        return dict(entry) if entry is not None else None


def _store_set_stage_running(run_id: str, agent_name: str) -> None:
    """标记当前正在运行的 stage（供 UI 实时展示进度）。"""
    with _STORE_LOCK:
        job = _JOB_STORE.get(run_id) or {}
        job["current_stage"] = agent_name
        job["current_stage_status"] = "running"
        _JOB_STORE[run_id] = job


def _store_append_stage(run_id: str, stage_dict: dict[str, Any]) -> None:
    """将已完成 stage 的 checkpoint 追加到 stages 列表。"""
    with _STORE_LOCK:
        job = _JOB_STORE.get(run_id) or {}
        stages = list(job.get("stages") or [])
        stages.append(stage_dict)
        job["stages"] = stages
        job["current_stage"] = stage_dict.get("agent_name", "")
        job["current_stage_status"] = stage_dict.get("status", "completed")
        _JOB_STORE[run_id] = job


def _run_job(job_kind: str, run_id: str, payload: dict[str, Any]) -> None:
    """在 daemon thread 中执行后台任务。"""
    _store_update(run_id, {"status": "in_progress", "started_at": datetime.now(timezone.utc).isoformat()})

    try:
        if job_kind == "funnel_screen":
            from scripts.web_background_job import _run_funnel_screen, _apply_funnel_env
            _apply_funnel_env(payload)
            result = _run_funnel_screen(run_id, payload)
        elif job_kind == "batch_ai_report":
            from scripts.web_background_job import _run_batch_ai_report
            result = _run_batch_ai_report(run_id, payload)
        elif job_kind == "full_pipeline":
            result = _run_full_pipeline(run_id, payload)
        else:
            raise ValueError(f"不支持的 job_kind: {job_kind}")

        _store_update(run_id, {
            "status": "completed",
            "result": result,
            "completed_at": datetime.now(timezone.utc).isoformat(),
        })
        logger.info("[agent_jobs] %s completed: run_id=%s", job_kind, run_id)

    except Exception as e:
        logger.exception("[agent_jobs] %s failed: run_id=%s", job_kind, run_id)
        _store_update(run_id, {
            "status": "failed",
            "result": {
                "request_id": run_id,
                "job_kind": job_kind,
                "ok": False,
                "error": str(e),
                "traceback": traceback.format_exc(),
            },
            "completed_at": datetime.now(timezone.utc).isoformat(),
        })


def submit_agent_job(job_kind: str, payload: dict[str, Any], *, state_key: str) -> str:
    """
    在 daemon thread 中提交后台任务。

    与 submit_background_job() 同签名、同 session_state 写法，
    用于 AGENT_MODE=1 下替代 GH Actions dispatch。
    """
    run_id = f"{job_kind}_{uuid4().hex[:12]}"
    logger.info("[agent_jobs] submitting %s: run_id=%s", job_kind, run_id)

    _store_update(run_id, {"status": "queued"})

    thread = threading.Thread(
        target=_run_job,
        args=(job_kind, run_id, payload),
        daemon=True,
        name=f"agent_job_{run_id}",
    )
    thread.start()

    st.session_state[state_key] = {
        "job_kind": job_kind,
        "request_id": run_id,
        "run": None,
        "result": None,
        "_agent_mode": True,  # 标记为本地执行模式，sync 时据此判断
    }
    return run_id


def sync_agent_job_state(*, state_key: str) -> dict[str, Any] | None:
    """
    检查进程内任务的执行状态。

    与 sync_background_job_state() 同签名。
    """
    state = st.session_state.get(state_key)
    if not isinstance(state, dict):
        return None

    request_id = str(state.get("request_id", "") or "").strip()
    if not request_id:
        return state

    job = _store_get(request_id)
    if not job:
        return state

    status = job.get("status", "queued")

    if status in ("completed", "failed"):
        # 模拟 GH Actions run 对象的结构，让页面渲染逻辑兼容
        state["run"] = _FakeRun(
            run_id=request_id,
            status="completed",
            conclusion="success" if status == "completed" else "failure",
        )
        state["result"] = job.get("result")
    elif status == "in_progress":
        state["run"] = _FakeRun(
            run_id=request_id,
            status="in_progress",
            conclusion=None,
        )
    else:
        # queued
        state["run"] = _FakeRun(
            run_id=request_id,
            status="queued",
            conclusion=None,
        )

    # Pipeline 阶段级进度数据（供 Pipeline 页面实时渲染）
    state["stages"] = job.get("stages", [])
    state["current_stage"] = job.get("current_stage", "")
    state["current_stage_status"] = job.get("current_stage_status", "")

    st.session_state[state_key] = state
    return state


class _FakeRun:
    """
    模拟 integrations/github_actions.WorkflowRun 的最小接口，
    让页面的 _render_job_status() 等函数能正常渲染。
    """

    def __init__(self, run_id: str, status: str, conclusion: str | None = None):
        self.run_id = run_id
        self.status = status
        self.conclusion = conclusion
        self.html_url = ""  # 本地模式没有 GH Actions URL
        self.created_at = ""

    def __repr__(self) -> str:
        return f"_FakeRun(run_id={self.run_id!r}, status={self.status!r})"


def agent_mode_enabled() -> bool:
    """检查是否启用了 AGENT_MODE（进程内执行模式）。"""
    import os
    return os.environ.get("AGENT_MODE", "").strip().lower() in ("1", "true", "yes")


def agent_mode_ready_for_current_user() -> tuple[bool, str]:
    """
    AGENT_MODE 下的权限检查。

    本地模式不需要 GITHUB_ACTIONS_TOKEN，但仍需要用户登录。
    """
    user = st.session_state.get("user") or {}
    user_id = ""
    if isinstance(user, dict):
        user_id = str(user.get("id", "") or "").strip()
    if not user_id:
        return (False, "当前未登录")
    return (True, "")


# ---------------------------------------------------------------------------
# full_pipeline: 通过 OrchestratorAgent 运行完整 5 阶段管线
# ---------------------------------------------------------------------------


def _build_orchestrator_from_payload(
    run_id: str,
    payload: dict[str, Any],
) -> Any:
    """从 Web UI payload 构建 OrchestratorAgent 实例。"""
    import os

    from scripts.web_background_job import _apply_funnel_env, _resolve_model_credentials

    # 注入 Funnel 环境变量（pool_mode, board, trading_days 等）
    _apply_funnel_env(payload)

    # 解析 LLM 凭证
    provider, api_key, model, base_url = _resolve_model_credentials(payload)

    no_llm = not bool(api_key)
    webhook_url = str(payload.get("webhook_url", "") or "").strip()
    notify = bool(webhook_url)
    skip_step4 = bool(payload.get("skip_step4", False)) or no_llm

    # Portfolio 配置
    portfolio_id = str(payload.get("portfolio_id", "") or "").strip()
    tg_bot_token = str(
        payload.get("tg_bot_token", "")
        or os.getenv("TG_BOT_TOKEN", "")
    ).strip()
    tg_chat_id = str(
        payload.get("tg_chat_id", "")
        or os.getenv("TG_CHAT_ID", "")
    ).strip()

    from agents.orchestrator import OrchestratorAgent

    return OrchestratorAgent(
        webhook_url=webhook_url,
        notify=notify,
        no_llm=no_llm,
        api_key=api_key,
        model=model,
        provider=provider,
        llm_base_url=base_url,
        wecom_webhook=str(payload.get("wecom_webhook", "") or "").strip(),
        dingtalk_webhook=str(payload.get("dingtalk_webhook", "") or "").strip(),
        portfolio_id=portfolio_id,
        tg_bot_token=tg_bot_token,
        tg_chat_id=tg_chat_id,
        skip_step4=skip_step4,
    )


def _pipeline_result_to_dict(
    run_id: str,
    pipeline_result: Any,
    ctx: dict[str, Any],
) -> dict[str, Any]:
    """将 OrchestratorAgent 的 AgentResult 转为可序列化 dict（供 UI 渲染）。"""
    from agents.contracts import PipelineStatus

    payload = pipeline_result.payload or {}
    stages = payload.get("stages", []) if isinstance(payload, dict) else []

    result: dict[str, Any] = {
        "request_id": run_id,
        "job_kind": "full_pipeline",
        "ok": pipeline_result.ok,
        "status": pipeline_result.status.value if hasattr(pipeline_result.status, "value") else str(pipeline_result.status),
        "duration_ms": pipeline_result.duration_ms,
        "stages": stages,
    }

    # 从 ctx 提取各阶段 payload 供 UI 渲染
    screener_result = ctx.get("screener")
    if screener_result and screener_result.payload:
        screen_payload = screener_result.payload
        result["symbols_for_report"] = (
            screen_payload.to_legacy_symbols_info()
            if hasattr(screen_payload, "to_legacy_symbols_info")
            else []
        )
        result["funnel_stats"] = getattr(screen_payload, "funnel_stats", {}) or {}
        result["total_scanned"] = getattr(screen_payload, "total_scanned", 0)

    mc_result = ctx.get("market_context")
    if mc_result and mc_result.payload:
        mc_payload = mc_result.payload
        result["benchmark_context"] = (
            mc_payload.to_legacy_dict()
            if hasattr(mc_payload, "to_legacy_dict")
            else {}
        )

    analyst_result = ctx.get("analyst")
    if analyst_result and analyst_result.payload:
        a_payload = analyst_result.payload
        result["report_text"] = getattr(a_payload, "report_text", "") or ""
        result["springboard_codes"] = getattr(a_payload, "springboard_codes", []) or []

    strategist_result = ctx.get("strategist")
    if strategist_result and strategist_result.payload:
        s_payload = strategist_result.payload
        result["strategy_decisions"] = getattr(s_payload, "decisions", []) or []
        result["strategy_reason"] = getattr(s_payload, "reason", "") or ""

    return result


def _run_full_pipeline(run_id: str, payload: dict[str, Any]) -> dict[str, Any]:
    """运行完整 OrchestratorAgent pipeline，实时回写阶段进度。"""
    orch = _build_orchestrator_from_payload(run_id, payload)

    def _on_start(agent_name: str) -> None:
        _store_set_stage_running(run_id, agent_name)

    def _on_done(checkpoint: dict[str, Any]) -> None:
        _store_append_stage(run_id, checkpoint)

    pipeline_result = orch.run(
        trigger={"run_id": run_id, "trigger": "web_ui"},
        on_stage_start=_on_start,
        on_stage_done=_on_done,
    )

    # 从 pipeline_result.payload["ctx"] 提取各阶段 payload 供 UI 渲染
    pl = pipeline_result.payload or {}
    ctx = pl.get("ctx", {}) if isinstance(pl, dict) else {}
    return _pipeline_result_to_dict(run_id, pipeline_result, ctx)
