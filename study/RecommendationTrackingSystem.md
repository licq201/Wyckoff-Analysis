# 推荐跟踪系统设计与实现 (v2.0.1)

## 1. 业务背景 (Background)
在量化选股系统中，往往存在“选出即结束”的问题。用户需要一个直观的界面来追踪这些被选出来的股票在过去一段时间内的真实表现，以便验证策略的有效性并总结经验。

## 2. 核心需求 (Requirements)
- **历史记录**：自动保存每日威科夫漏斗跑出的推荐结果。
- **性能跟踪**：每日同步最新收盘价，实时计算自推荐以来的涨跌幅。
- **高效检索**：支持对海量历史推荐进行股票代码、名称的搜索，并能按涨跌幅和日期快速排序。
- **数据一致性**：处理 A 股代码的“前导零”问题，并防止同一天重复入库。

## 3. 技术架构 (Architecture)

### 3.1 数据库方案 (Supabase & SQL)
- **数据结构**：采用 `INT` 类型存储 `code` 和 `recommend_date` (YYYYMMDD)，以获得极致的索引性能。
- **索引设计**：
    - `idx_rec_sort_date`: 针对日期降序排列。
    - `idx_rec_sort_change`: 针对涨跌幅排序，快速筛选出“牛股”和“地雷”。
    - `idx_rec_search_code`: 针对数字型代码的精准匹配。

### 3.2 后端逻辑 (Python)
- **DAO层**：新建 `integrations/supabase_recommendation.py`，封装了 `upsert_recommendations`（入库）和 `sync_all_tracking_prices`（定时同步）。
- **流程整合**：在 `daily_job.py` 中增加了两个 Hook 环节：
    - **Hook A**：Funnel 运行后，将候选名单及其当前价（作为初始价）存入。
    - **Hook B**：整个流水线结束前，触发“全量同步”，刷新所有历史记录的价格。

### 3.3 前端实现 (Streamlit)
- **View层**：`pages/RecommendationTracking.py`。
- **显示优化**：自动对数字型的 `code` 进行 `{:06d}` 格式化，确保用户看到的是标准代码。
- **交互设计**：增加了摘要统计卡片（平均收益、胜率等）和多维筛选工具。

## 4. 关键代码片段 (Core Code)

### 涨跌幅计算口径
```python
change_pct = (current_price - initial_price) / initial_price * 100.0
```

### 数据库唯一约束
```sql
UNIQUE(code, recommend_date) -- 确保同一标的当日仅记录一次
```

## 5. 总结 (Conclusion)
该功能的上线标志着系统从单纯的“选股器”向“策略生命周期管理系统”迈进。通过高效的数据库设计和自动化的同步机制，为用户提供了低延迟、高可用的复盘工具。
