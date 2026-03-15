# -*- coding: utf-8 -*-
"""
Supabase 推荐跟踪数据存取模块
"""
from __future__ import annotations

import os
from datetime import date, datetime
from typing import Any

from supabase import Client, create_client
from core.constants import TABLE_RECOMMENDATION_TRACKING

def _get_supabase_admin_client() -> Client:
    url = (os.getenv("SUPABASE_URL") or "").strip()
    key = (
        (os.getenv("SUPABASE_SERVICE_ROLE_KEY") or "").strip() 
        or (os.getenv("SUPABASE_KEY") or "").strip()
    )
    if not url or not key:
        raise ValueError("SUPABASE_URL/SUPABASE_SERVICE_ROLE_KEY 未配置")
    return create_client(url, key)

def is_supabase_configured() -> bool:
    url = (os.getenv("SUPABASE_URL") or "").strip()
    key = (os.getenv("SUPABASE_SERVICE_ROLE_KEY") or "").strip() or (os.getenv("SUPABASE_KEY") or "").strip()
    return bool(url and key)

def upsert_recommendations(recommend_date: int, symbols_info: list[dict[str, Any]]) -> bool:
    """
    将每日选出的股票存入推荐跟踪表
    recommend_date: YYYYMMDD (int)
    """
    if not is_supabase_configured() or not symbols_info:
        return False
    try:
        client = _get_supabase_admin_client()
        payload = []
        for s in symbols_info:
            raw_code = str(s.get("code", "")).strip()
            # 提取纯数字部分 (比如 "000001.SZ" -> "000001")
            code_str = "".join(filter(str.isdigit, raw_code))
            if not code_str:
                continue
            
            # 这里的 price 通常是结果生成当日的收盘价
            price = float(s.get("initial_price") or 0.0)
            
            payload.append({
                "code": int(code_str),  # 存为 INT，首位0会消失
                "name": str(s.get("name", "")).strip(),
                "recommend_reason": str(s.get("tag", "")).strip(),
                "recommend_date": recommend_date,
                "initial_price": price,
                "current_price": price, # 初始时当前价等于加入价
                "change_pct": 0.0,      # 初始涨跌幅为 0
                "updated_at": datetime.utcnow().isoformat()
            })
        
        if payload:
            # 使用 upsert，基于 (code, recommend_date) 唯一约束
            client.table(TABLE_RECOMMENDATION_TRACKING).upsert(
                payload, on_conflict="code,recommend_date"
            ).execute()
        return True
    except Exception as e:
        print(f"[supabase_recommendation] upsert_recommendations failed: {e}")
        return False

def sync_all_tracking_prices() -> int:
    """
    遍历表中所有股票，获取最新实时价格并刷新
    返回成功更新的数量
    """
    if not is_supabase_configured():
        return 0
    
    try:
        from integrations.data_source import fetch_stock_spot_snapshot
        client = _get_supabase_admin_client()
        
        # 1. 获取所有需要跟踪的股票代码（去重以节省 API 调用）
        resp = client.table(TABLE_RECOMMENDATION_TRACKING).select("code").execute()
        if not resp.data:
            return 0
        
        unique_codes = sorted(list(set(int(r["code"]) for r in resp.data)))
        
        # 2. 批量获取实时价格并构建更新数据
        # 实际上目前 fetch_stock_spot_snapshot 是一只只取的，后期可优化为批量接口
        updated_count = 0
        for code_int in unique_codes:
            code_str = f"{code_int:06d}" # 补齐 6 位以适配行情接口
            snap = fetch_stock_spot_snapshot(code_str, force_refresh=True)
            if not snap or snap.get("close") is None:
                continue
            
            new_current_price = float(snap["close"])
            
            # 3. 针对该股票的所有推荐记录进行价格和涨跌幅更新
            # 注意：同一个股票可能在不同日期被推荐过，需要分别计算
            rec_resp = client.table(TABLE_RECOMMENDATION_TRACKING).select("*").eq("code", code_int).execute()
            for record in rec_resp.data:
                initial_price = float(record.get("initial_price") or 0.0)
                if initial_price > 0:
                    change_pct = (new_current_price - initial_price) / initial_price * 100.0
                else:
                    change_pct = 0.0
                
                client.table(TABLE_RECOMMENDATION_TRACKING).update({
                    "current_price": new_current_price,
                    "change_pct": round(change_pct, 2),
                    "updated_at": datetime.utcnow().isoformat()
                }).eq("id", record["id"]).execute()
                updated_count += 1
                
        return updated_count
    except Exception as e:
        print(f"[supabase_recommendation] sync_all_tracking_prices failed: {e}")
        return 0

def load_recommendation_tracking(limit: int = 1000) -> list[dict[str, Any]]:
    """加载推荐跟踪数据"""
    try:
        # 这里可以使用普通 client，也可以用 admin
        from integrations.supabase_client import get_supabase_client
        client = get_supabase_client()
        resp = (
            client.table(TABLE_RECOMMENDATION_TRACKING)
            .select("*")
            .order("recommend_date", desc=True)
            .limit(limit)
            .execute()
        )
        return resp.data or []
    except Exception as e:
        print(f"[supabase_recommendation] load_recommendation_tracking failed: {e}")
        return []
