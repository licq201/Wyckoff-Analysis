# Wyckoff-Analysis (wkf-bsk 分支) 部署指南与实战问题排查手册 (FAQ)

> 本文档汇总了在 `wkf-bsk` 分支下基于 **Tushare Pro + DeepSeek + Supabase + 飞书** 进行 A 股量化交易分析的完整部署步骤、核心交易原则及常见问题排查解答。

---

## 目录
1. [系统定位与 A 股实盘核心交易流程](#一-系统定位与-a-股实盘核心交易流程)
2. [环境配置与凭据准备清单](#二-环境配置与凭据准备清单)
3. [多端运行与部署模式](#三-多端运行与部署模式)
   - [3.1 本地 CLI 智能体与可视化看板](#31-本地-cli-智能体与可视化看板)
   - [3.2 Web 智能投研助手 (端口 5173 + 8787)](#32-web-智能投研助手-端口-5173--8787)
   - [3.3 GitHub Actions 云端全自动盯盘 (针对 wkf-bsk 分支)](#33-github-actions-云端全自动盯盘-针对-wkf-bsk-分支)
4. [常见问题排查与避坑指南 (FAQ)](#四-常见问题排查与避坑指南-faq)

---

## 一、 系统定位与 A 股实盘核心交易流程

WyckoffAgent 是一个基于**威科夫（Wyckoff）量价理论**的多市场量化分析与交易智能体系统。实盘交易严格执行 **「日漏斗 × 次日开盘」** 闭环：

```text
【盘后 17:17】                  【次日 08:20-09:25】                 【次日 09:30 开盘】              【成交后】
日漏斗扫描 + AI 研报   ───►   跨日信号确认 (VALIDATED)   ───►   开盘价落在 OMS 允许区间?   ───►   实盘成交回填
(看大盘水温 + 挑起跳板)         (未确认/观察 = 不买)              (高于不追，低于不抄底)            (同步账本防误报)
```

### 核心操作纪律：
1. **大盘水温（Regime）优先**：`NEUTRAL` 允许主线新开仓；`RISK_ON`（市场过热）**绝对禁止新开仓**；`RISK_OFF / CRASH` 防守减仓。
2. **AI 研报三阵营**：仅选择评级为 **🏹 处于起跳板（Springboard）** 的候选，储备营地（Camp）与逻辑破产（Broken）一律不买。
3. **跨日确认**：次日开盘前确认信号状态为 **`VALIDATED`**（数据库字段为 `confirmed`）。
4. **唯一允许买入区间**：仅当 09:30 开盘价落在 Step4 OMS 给出的区间内才下单。
5. **成交必须回填**：通过 `wyckoff portfolio fill` 或在 Agent 对话中回填成交，防止止损重复报警。

---

## 二、 环境配置与凭据准备清单

### 1. 本地 `.env` 配置文件模版
在项目根目录创建 `.env`：

```bash
# 1. 行情与数据源 (A 股)
TUSHARE_TOKEN=your_tushare_token

# 2. 大模型配置 (DeepSeek 全面接管)
DEFAULT_LLM_PROVIDER=deepseek
DEEPSEEK_API_KEY=your_deepseek_api_key
DEEPSEEK_MODEL=deepseek-chat
DEEPSEEK_BASE_URL=https://api.deepseek.com/v1

# 盘后 AI 研报 (Step3) 与 持仓 OMS (Step4) 显式绑定 DeepSeek
STEP3_LLM_PROVIDER=deepseek
STEP4_LLM_PROVIDER=deepseek

# 3. 数据库存储 (Supabase 云端同步)
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_KEY=your_supabase_anon_key
SUPABASE_SERVICE_ROLE_KEY=your_supabase_service_role_key
SUPABASE_USER_ID=your_supabase_user_uuid

# 4. 消息推送 (飞书 Webhook)
FEISHU_WEBHOOK_URL=https://open.feishu.cn/open-apis/bot/v2/hook/xxxxxx
```

---

## 三、 多端运行与部署模式

### 3.1 本地 CLI 智能体与可视化看板

```powershell
# 1. 激活虚拟环境 (Windows PowerShell)
.\.venv\Scripts\Activate.ps1

# 2. 安装项目依赖
pip install -e ".[mcp]"

# 3. 注册本地 CLI 模型
wyckoff model set deepseek deepseek "your_deepseek_api_key" --model deepseek-chat --base-url https://api.deepseek.com/v1
wyckoff model default deepseek

# 4. 启动交互式 Agent
wyckoff

# 5. 启动本地可视化面板
wyckoff dashboard
```

---

### 3.2 Web 智能投研助手 (端口 5173 + 8787)

- **官方免部署版**：直接访问 [https://wyckoff-analysis.pages.dev/](https://wyckoff-analysis.pages.dev/)，登录并在「设置」中输入 Key 即可使用。
- **本地启动版**：
  ```powershell
  cd web
  pnpm install
  pnpm dev
  ```
  浏览器访问 `http://127.0.0.1:5173/`。

---

### 3.3 GitHub Actions 云端全自动盯盘 (针对 wkf-bsk 分支)

#### 1. 配置仓库 Secrets / Variables
在 GitHub 仓库 **Settings $\to$ Secrets and variables $\to$ Actions** 中配置：
- `DEFAULT_LLM_PROVIDER`: `deepseek`
- `STEP3_LLM_PROVIDER`: `deepseek`
- `STEP4_LLM_PROVIDER`: `deepseek`
- `DEEPSEEK_API_KEY`: 你的 DeepSeek Key
- `TUSHARE_TOKEN`: 你的 Tushare Token
- `SUPABASE_URL` / `SUPABASE_KEY` / `SUPABASE_SERVICE_ROLE_KEY` / `SUPABASE_USER_ID`
- `FEISHU_WEBHOOK_URL`: 你的飞书 Webhook

#### 2. `wkf-bsk` 测试分支说明
- **定时任务触发规则**：GitHub Actions 的 `cron` 定时任务（每天 17:17）**只在默认分支运行**。
- **即时测试**：在 Actions 页面点击 **Wyckoff Funnel $\to$ Run workflow**，在分支下拉框中选择 **`wkf-bsk`** 手动触发。
- **长期定时**：若想每天自动运行 `wkf-bsk` 分支，需在仓库 **Settings $\to$ General** 将默认分支临时改为 `wkf-bsk`，或测试稳定后合并回 `main`。

---

## 四、 常见问题排查与避坑指南 (FAQ)

### Q1: 运行 `wyckoff` 提示 `ModuleNotFoundError: No module named 'termios'`？
- **原因**：Windows 操作系统上没有 Unix 系统的 `termios` 模块，TUI 初始化时硬编码导入了 `LinuxDriver`。
- **修复**：已在 `cli/tui.py` 中增加对 `LinuxDriver` 的导入异常捕获，在 Windows 下自动回退至原生 `WindowsDriver`。

### Q2: 初始化时提示 `AttributeError: _ARRAY_API not found`？
- **原因**：Anaconda 基础环境中的 `numpy` 与 `bottleneck` C 扩展二进制版本不兼容。
- **解决**：在专用的 `.venv` 虚拟环境中运行，并在 PowerShell 中执行 `conda config --set auto_activate_base false` 防止全局环境污染。

### Q3: 运行 `wyckoff update` 导致 `ModuleNotFoundError: No module named 'cli'`？
- **原因**：`wyckoff update` 是为独立 pip 包用户设计的升级命令，在源码 Git 仓库执行会覆盖本地开发 `-e` 符号链接。
- **解决**：在源码仓库下请通过 `git pull` 更新代码，然后执行 `pip install -e ".[mcp]"` 重新挂载，切勿运行 `wyckoff update`。

### Q4: 进入 `wyckoff` TUI 后提示 `⚠ 未配置模型，请先输入 /model add`？
- **原因**：`.env` 文件供后台脚本使用，CLI 交互有自己独立的本地模型列表文件（`~/.wyckoff/wyckoff.json`）。
- **解决**：在命令行执行 `wyckoff model set deepseek deepseek "KEY" --model deepseek-chat --base-url https://api.deepseek.com/v1` 并设为 default。

### Q5: 为什么模型分析开头输出了「默认假设声明 / 证据台账」？
- **原因**：这是系统设计的严谨投研纪律（防幻觉与证据链机制）。威科夫指标基于已收盘日线计算，盘中运行时模型会明确提示“基于昨日收盘价，盘中未收盘”，并将 320 天历史精炼为近 60 天切片进行审判，属于正常的高质量输出。

### Q6: GitHub Actions 报错 `Wyckoff Funnel workflow run failed`？
- **原因**：工作流默认将大模型提供商设为 `gemini` 和 `efficiency`，云端未配置对应 Key 导致预检退出（`exit 1`）。
- **解决**：在 GitHub 仓库 Secrets/Variables 中添加 `DEFAULT_LLM_PROVIDER=deepseek`、`STEP3_LLM_PROVIDER=deepseek`、`STEP4_LLM_PROVIDER=deepseek` 及 `DEEPSEEK_API_KEY`。

### Q7: Web 网页端点击「测试模型连通性」提示 `Failed to fetch`？
- **原因**：前端（5173）在向本地 API 后端（8787）发送网络请求时连接失败。通常是因为只启动了前端，未启动 8787 的 Wrangler 后端。
- **解决**：在 `web` 根目录下直接运行 `pnpm dev` 同时启动 5173 与 8787；或直接使用云端免部署版 [wyckoff-analysis.pages.dev](https://wyckoff-analysis.pages.dev/)。

### Q8: 8787 后端报错 `Supabase env is missing`？
- **原因**：本地运行 Wrangler 时未单独注入 Supabase 环境变量导致 Token 校验失败。
- **解决**：已在 `web/apps/api/src/middleware/auth.ts` 与 `chat.ts` 中补充了内置默认 Supabase 配置回退。

### Q9: 飞书 Webhook 如何测试？为什么 Dashboard 单股分析不推飞书？
- **机制说明**：飞书 Webhook 专门用于**每日定时批处理报告**与**持仓风控工单**，个人实时对话直接渲染在界面上以避免刷屏。
- **测试命令**：
  ```powershell
  python -c "import os, requests; from dotenv import load_dotenv; load_dotenv(); url = os.getenv('FEISHU_WEBHOOK_URL'); r = requests.post(url, json={'msg_type': 'text', 'content': {'text': '🔔 飞书机器人连接正常！'}}); print('飞书返回:', r.json())"
  ```

### Q10: 网页端的配置是否会与 `.env` 冲突？
- **机制说明**：Web 网页端运行在浏览器沙箱中，不读取磁盘 `.env` 文件。登录后在网页设置的参数保存在用户私有的 Supabase `user_settings` 表中，完全由用户私有使用。

### Q11: 读盘室对话分析股票提示 `Missing CHAT_TOOL_APPROVAL_SECRET`？
- **原因**：本地 8787 后端（Wrangler）在执行带有工具审批签名的对话请求时，需要 `CHAT_TOOL_APPROVAL_SECRET`。
- **解决**：在 `web/apps/api/.dev.vars` 中配置 `CHAT_TOOL_APPROVAL_SECRET=wyckoff-local-dev-tool-approval-secret-key-32chars`，Wrangler 启动时会自动读取并注入。

