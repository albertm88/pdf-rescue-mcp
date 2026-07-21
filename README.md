# 中文 PDF 书籍救援 MCP

[![Python](https://img.shields.io/badge/Python-3.11+-blue.svg)](https://www.python.org/)
[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)
[![MCP](https://img.shields.io/badge/MCP-1.13.0+-purple.svg)](https://modelcontextprotocol.io/)

中文 | [English](README_EN.md)

面向中文书籍 PDF 的本地 MCP 服务与命令行工具。支持扫描版 PDF 的 OCR 文本提取、质量巡检、断点续传和批量处理。

当前发布线：**1.0.0** · [架构文档](docs/ARCHITECTURE_1.0.md)

---

## 目录

- [快速开始](#快速开始)
  - [第一步：环境准备](#第一步环境准备)
  - [第二步：克隆并安装](#第二步克隆并安装)
  - [第三步：体检确认](#第三步体检确认)
  - [第四步：配置 MCP 客户端](#第四步配置-mcp-客户端)
  - [第五步：开始使用](#第五步开始使用)
- [批量处理书库](#批量处理书库)
- [识别模式](#识别模式)
- [输出结构](#输出结构)
- [命令行用法](#命令行用法)
- [后处理：条目拆分](#后处理条目拆分)
- [MCP 工具参考](#mcp-工具参考)
- [架构](#架构)
- [性能优化](#性能优化)
- [配置参考](#配置参考)
- [常见问题](#常见问题)
- [开发](#开发)

---

## 快速开始

### 第一步：环境准备

| 需求 | 说明 |
|------|------|
| **Python** | ≥ 3.11（[下载](https://www.python.org/downloads/)） |
| **uv** | 包管理器（[安装指南](https://docs.astral.sh/uv/)） |

```bash
# 验证环境
python --version   # 应 ≥ 3.11
uv --version
```

### 第二步：克隆并安装

```bash
git clone https://github.com/albertm88/pdf-rescue-mcp.git
cd pdf-rescue-mcp
```

| 场景 | 安装命令 | 说明 |
|------|----------|------|
| 日常使用（CPU） | `uv sync --extra ocr` | 适用所有平台 |
| NVIDIA 显卡加速 | `uv sync --extra ocr-gpu` | CUDA 11.8，3-5 倍提速 |

### 第三步：体检确认

```bash
uv run python -m pdf_rescue_mcp.cli 体检
```

正常输出应包含：CPU 核心数、可用内存、OCR 引擎状态等信息。

### 第四步：配置 MCP 客户端

> 生成带绝对路径的配置文件，避免客户端找不到启动脚本。

```bash
uv run python scripts/generate_mcp_config.py --client vscode     --output .vscode/mcp.json
uv run python scripts/generate_mcp_config.py --client claude     --output ~/claude-mcp.json
uv run python scripts/generate_mcp_config.py --client cursor     --output ~/cursor-mcp.json
uv run python scripts/generate_mcp_config.py --client anythingllm --output ~/anythingllm-mcp.json
uv run python scripts/generate_mcp_config.py --client trae       --output .trae/mcp.json
uv run python scripts/generate_mcp_config.py --client codex      --output ~/codex-mcp.toml
```

> **手动配置**：复制 `examples/` 下的模板，将 `{{PROJECT_ROOT}}` 替换为项目绝对路径。VS Code 用户可直接用 `examples/mcp-config.vscode.json`（`${workspaceFolder}` 无需替换）。

### 第五步：开始使用

在 MCP 客户端中直接对 AI 说：

> 帮我把 `D:\扫描书籍\某某书.pdf` 转成文字

AI 会自动调用 `rescue_pdf` 完成 **诊断 → 规划 → OCR → 质检** 全流程。

📁 处理结果保存在：`<PDF 同级目录>/pdf_rescue_output/<书名>-rescue-result/`

---

## 批量处理书库

适合大量 PDF 批量 OCR 的场景。

```bash
# 1. 扫描书库
uv run python -m pdf_rescue_mcp.cli 书库扫描 <书库目录>

# 2. 启动批量提取（后台运行，支持断点续传）
uv run python -B scripts/batch_extract_all.py
```

**批量控制器能力：**

- 🔍 自动发现书库中所有 PDF
- 📊 按 CPU/内存/worker 实时占用动态分配并发
- 💾 逐页缓存，中断重启自动从断点继续
- 📈 每 30 秒输出进度报告（已完成/进行中/排队、页数、ETA、资源占用）

**自定义书库路径：** 修改 `scripts/batch_extract_all.py` 中的 `ROOT` 和 `OUTPUT` 变量。

---

## 识别模式

| 模式 | DPI | 适用场景 | 速度（CPU） |
|------|-----|----------|:-----------:|
| `book-fast` | 180 | 快速预览、大批量处理 | 8-30 秒/页 |
| `book-balanced` | 220 | 日常使用 ⭐ 默认 | 15-45 秒/页 |
| `book-quality` | 300 | 高质量输出 | 30-90 秒/页 |
| `book-forensic` | 300+ | 取证级、低质量扫描件 | 60-180 秒/页 |

---

## 输出结构

```
<输出目录>/<书名>-rescue-result/
├── 状态.json              # 实时进度（页数、速度、ETA、引擎）
├── 清单.yaml              # 处理清单和配置
├── 文本/
│   └── 全书.md            # 合并的全文 Markdown
├── 数据/
│   ├── 页面.jsonl         # 逐页文本 + 置信度 + 来源
│   ├── 质量.json          # 质量报告
│   ├── 低置信页.jsonl     # 低置信页详情
│   └── 失败页.jsonl       # 失败页详情
├── 缓存/
│   └── 页面OCR/           # 逐页 OCR 缓存（断点续传）
├── 审计/
│   └── 审计.html          # 可视化质量审计报告
└── 日志/                  # 子进程运行日志
```

---

## 命令行用法

除 MCP 客户端外，也可直接在终端操作：

```bash
# ── 诊断 ──
uv run python -m pdf_rescue_mcp.cli 体检                    # 环境体检
uv run python -m pdf_rescue_mcp.cli 检查 <pdf路径>           # 检查 PDF 类型
uv run python -m pdf_rescue_mcp.cli 规划 <pdf路径>           # 规划处理路线

# ── 提取 ──
uv run python -m pdf_rescue_mcp.cli 提取 <pdf路径>           # 提取单本书
    --mode book-fast --output-dir <输出目录>

# ── 管理 ──
uv run python -m pdf_rescue_mcp.cli 状态 <任务目录>           # 查询进度
uv run python -m pdf_rescue_mcp.cli 恢复 <任务目录>           # 恢复中断任务
uv run python -m pdf_rescue_mcp.cli 质检 <任务目录>           # 质量巡检

# ── 批量 ──
uv run python -m pdf_rescue_mcp.cli 书库扫描 <书库目录>       # 扫描书库
uv run python -m pdf_rescue_mcp.cli 书库提取 <书库目录>       # 批量提取
    --output-dir <输出目录> --mode book-fast
```

---

## 后处理：条目拆分

OCR 完成后，可将全书按百科条目拆分为独立 Markdown 文件：

```bash
uv run python scripts/split_into_entries_v2.py <rescue-result目录> <最终输出目录>
```

输出结构：

```
<最终输出目录>/<书名>/
├── 前言/
│   └── 前言与凡例.md
├── 条目/
│   ├── 鳖甲.md
│   ├── 冰硼散.md
│   └── ...
└── 索引.md
```

---

## MCP 工具参考

### 核心工具

| 工具 | 说明 |
|------|------|
| `rescue_pdf` | ⭐ **首选入口**：自动诊断→规划→提取→质检，传入 PDF 路径即可 |
| `extract_book_text` | 提取书籍文本，后台子进程运行，立即返回任务目录 |
| `get_job_status` | 查询任务进度（页数、速度、剩余时间、线程健康） |
| `resume_job` | 恢复中断的任务（断点续传） |
| `cancel_job` | 请求任务在当前页边界安全停止 |
| `audit_job_quality` | 质量巡检（低置信页、失败页、分裂标题检测） |
| `get_iteration_plan` | 生成版本化的质量/资源改善建议，需人工批准后执行 |

### 批量处理

| 工具 | 说明 |
|------|------|
| `batch_extract_library` | 批量提取书库，后台逐本处理 |
| `get_batch_status` | 查看批量进度（完成数/总数、当前书籍、页数、ETA、资源占用） |
| `stop_batch` | 停止批量（当前书籍继续完成） |
| `scan_pdf_library` | 扫描书库，生成 PDF 清单和建议动作 |

### 诊断与规划

| 工具 | 说明 |
|------|------|
| `run_health_check` | 运行体检（CPU/内存/GPU/OCR 依赖） |
| `inspect_pdf_text_layer` | 检查 PDF 文本层（扫描/原生/混合/加密） |
| `diagnose_pdf` | 诊断 PDF 类型、乱码风险、扫描页比例 |
| `plan_pdf_job` | 规划处理路线（模式、预计耗时、引擎选择） |

### 证据与词表

| 工具 | 说明 |
|------|------|
| `get_page_evidence` | 查看指定页的识别文本、置信度、识别块 |
| `export_page_image_evidence` | 导出页面渲染图片用于核对 |
| `get_term_glossary` | 查看术语词表 |
| `update_term_glossary` | 添加术语替换规则 |

### OCR 容量调优

| 工具 | 说明 |
|------|------|
| `plan_ocr_capacity_profile` | 规划 2/4/6/8 线程与多 worker 吞吐基准 |
| `start_ocr_capacity_profile` | 后台执行已规划基准（仅无生产 OCR 时） |
| `get_ocr_capacity_profile` | 查看页吞吐、RSS、线程利用率、质量门禁 |
| `activate_ocr_capacity_profile` | 激活推荐策略，仅影响之后新启动的 worker |

### 历史记录

| 工具 | 说明 |
|------|------|
| `get_processing_history` | 查看处理历史 |
| `share_processing_history` | 生成可分享的历史记录（JSON / Markdown / HTML） |

---

## 架构

```
┌─────────────────────────────────────────────────────────┐
│  VS Code / TRAE / Codex / AnythingLLM / 其他 MCP Host    │
└──────────────────────────┬──────────────────────────────┘
                           │ JSON-RPC (stdio)
┌──────────────────────────▼──────────────────────────────┐
│             FastMCP 适配器（stdio 或本机 HTTP）            │
│                                                         │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌─────────┐ │
│  │rescue_pdf│  │ 体检/诊断 │  │ 状态查询  │  │ 批量管理 │ │
│  └────┬─────┘  └──────────┘  └──────────┘  └────┬────┘ │
│       │                                         │      │
│  ┌────▼─────────────────────────────────────────▼────┐ │
│  │ LocalSupervisor / TaskStore / ProcessController  │ │
│  │  ┌─────────────────────────────────────────────┐  │ │
│  │  │ 监管层：本机 SQLite 租约 + 心跳 + 页级前进    │  │ │
│  │  │ 失联/卡页 → 安全停止 → 断点恢复              │  │ │
│  │  └─────────────────────────────────────────────┘  │ │
│  └────────────────────────┬──────────────────────────┘ │
└───────────────────────────┼─────────────────────────────┘
                            │ subprocess.Popen
┌───────────────────────────▼─────────────────────────────┐
│               子进程（隔离 OCR）                          │
│  python -u -m pdf_rescue_mcp.cli 提取                    │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌─────────┐ │
│  │ 渲染页面  │→│ PaddleOCR │→│ 原子状态/缓存 │→│ 页级事件 │ │
│  └──────────┘  └──────────┘  └──────────┘  └─────────┘ │
└─────────────────────────────────────────────────────────┘
```

### 1.0 三层运行架构

| 层级 | 机制 | 参数 |
|------|------|------|
| **业务层** | 独立 OCR 子进程、逐页缓存、原子状态/JSONL | 页数、进度、速度、ETA、质量证据 |
| **监管层** | SQLite WAL 任务账本、fencing lease、心跳与页级前进、进程树收尾 | `WATCH_INTERVAL=5s`, `HEARTBEAT_TIMEOUT=90s`, `PROGRESS_TIMEOUT=600s` |
| **迭代更新层** | 从质量巡检和监管事件生成版本化建议，需人工审核 | `get_iteration_plan`, `strategy_version=1.0.0` |

### 跨平台运行目录

监管层默认使用操作系统标准目录（Windows `%APPDATA%`，macOS `~/Library`，Linux XDG）。可设置环境变量覆盖：

```bash
# Linux / macOS
export PDF_RESCUE_RUNTIME_ROOT=/path/to/pdf-rescue-runtime

# Windows PowerShell
$env:PDF_RESCUE_RUNTIME_ROOT = "D:\pdf-rescue-runtime"
```

> ⚠️ SQLite 数据库仅支持本机磁盘，勿放在网络共享盘或同步盘。

### 可选：Streamable HTTP 模式

适用于无法启动 stdio 子进程的 MCP 客户端（仅回环地址）：

```powershell
$env:PDF_RESCUE_MCP_TRANSPORT = "streamable-http"
$env:PDF_RESCUE_MCP_HOST = "127.0.0.1"
$env:PDF_RESCUE_MCP_PORT = "8765"
uv run --locked --extra ocr python -B scripts/start_mcp.py
```

---

## 性能优化

- 自动检测 CPU 核心数，保留 2 核给系统
- **NVIDIA GPU**：安装 `ocr-gpu` 扩展，CUDA 加速 3-5 倍
- AMD Ryzen 7 5800H（8 核 16 线程）实测：`book-fast` 约 8-15 秒/页

---

## 配置参考

以下环境变量和参数可用于精细控制资源分配与监管行为。无需修改源代码，在终端设置后启动即可。

### Worker 线程与并发

| 环境变量 / 参数 | 默认值 | 说明 |
|:---|:---:|:---|
| `PDF_RESCUE_OCR_THREADS` | 自动（1–4） | 每个 OCR worker 的线程数。自动时按物理核心数计算，上限 4 |
| `PDF_RESCUE_MAX_WORKERS` | 自动（≤4） | 最大并行 worker 数量。自动时按 CPU 核心与内存综合计算 |

**自动线程分配逻辑：** 物理核心 ≥ 8 → 4 线程；6–7 核 → 3 线程；4–5 核 → 2 线程；≤3 核 → 1 线程。始终预留 2 个逻辑核心给系统。

**Worker 线程预算（按页数自动分配）：** <80 页 → 1 线程；≥80 页 → 2 线程；≥200 页 → 3 线程；≥400 页 → 4 线程。

```bash
# 示例：强制每个 worker 用 2 线程，最多 3 个并行 worker
$env:PDF_RESCUE_OCR_THREADS = "2"
$env:PDF_RESCUE_MAX_WORKERS = "3"
```

### 内存控制

| 环境变量 / 参数 | 默认值 | 说明 |
|:---|:---:|:---|
| `PDF_RESCUE_RESERVE_MEMORY_GB` | 2.0 GB | 为系统保留的内存，低于此值不再启动新 worker |
| `PDF_RESCUE_MEMORY_PER_WORKER_GB` | 2.0 GB | 每个 worker 的预估内存占用，用于计算内存槽位 |

可用内存槽位 = `(可用内存 - 保留内存) ÷ 每 worker 内存`。结合 CPU 核心约束取最小值决定实际并发数。

```bash
# 示例：大内存机器，每个 worker 分配 4 GB，保留 4 GB 给系统
$env:PDF_RESCUE_RESERVE_MEMORY_GB = "4"
$env:PDF_RESCUE_MEMORY_PER_WORKER_GB = "4"
```

### 监管超时机制

| 参数 | 默认值 | 说明 |
|:---|:---:|:---|
| `WATCH_INTERVAL` | 5 秒 | 任务看门狗轮询间隔 |
| `HEARTBEAT_TIMEOUT` | 90 秒 | Worker 心跳超时：超时无心跳视为失联 |
| `PROGRESS_TIMEOUT` | 600 秒 | 进度超时：存活但无页级推进视为卡死 |
| `STARTUP_TIMEOUT` | 120 秒 | 启动超时：等待 worker 首次心跳 |
| `CANCEL_GRACE` | 45 秒 | 取消宽限期：发送取消信号后等待优雅退出 |
| `MAX_AUTO_RESTART` | 1 | 异常退出后最大自动重启次数 |

### 批量控制器

| 参数 | 默认值 | 说明 |
|:---|:---:|:---|
| `PAGE_RATE_SAMPLE_WINDOW` | 300 秒 | 页速采样滑动窗口 |
| `PAGE_RATE_MAX_SAMPLES` | 12 | 页速最大保留样本数 |
| `OBSERVER_TAKEOVER_INTERVAL` | 5 秒 | 被动观察者接管轮询间隔 |
| `CONTROLLER_LEASE_SECONDS` | 45 秒 | 批量控制器本地排他租约 TTL |
| `LEASE_SECONDS` | 45 秒 | 单任务 MCP 适配器租约 TTL |

### 容量调优门禁

| 参数 | 默认值 | 说明 |
|:---|:---:|:---|
| 整机 CPU 安全护栏 | 92% | 调优试验期间 CPU 超过此阈值则拒绝候选方案 |
| 吞吐提升阈值 | 5% | 多 worker 方案须比最佳单 worker 基线提升的最低比例 |
| 质量回退容忍度 | 3% | 多 worker 方案允许的低置信页比例回退上限 |

### 品质阈值

| 参数 | 默认值 | 说明 |
|:---|:---:|:---|
| `LOW_CONFIDENCE_THRESHOLD` | 0.9 | 置信度低于此值触发质量警告 |
| `LOW_CONFIDENCE_RETRY_DPI` | 300 | 低置信页自动重试 DPI |
| `LOW_CONFIDENCE_MIN_TEXT_RATIO` | 0.85 | 低置信重试的最小文本比例 |

### 运行目录

| 环境变量 | 默认值 | 说明 |
|:---|:---|:---|
| `PDF_RESCUE_RUNTIME_ROOT` | OS 标准目录 | 监管层运行时持久化根目录（SQLite 账本等） |

> ⚠️ 以上超时、窗口等参数目前为硬编码常量，调优需修改 `src/pdf_rescue_mcp/server.py` 中 `_TaskManager` 类属性。后续版本将支持环境变量覆盖。

---

## 常见问题

<details>
<summary><b>Q: 为什么速度一直是 30-40 秒/页？</b></summary>

CPU 模式下这是正常速度。可尝试 `book-fast` 模式（DPI=180），或安装 NVIDIA GPU 扩展。
</details>

<details>
<summary><b>Q: 断点续传如何工作？</b></summary>

每完成一页，OCR 结果缓存到 `缓存/页面OCR/*.json`。重启时自动跳过已缓存页面。
</details>

<details>
<summary><b>Q: 如何处理密码保护的 PDF？</b></summary>

使用 `rescue_pdf` 时传入 `password` 参数。密码不会写入记录文件。
</details>

---

## 开发

```bash
# 安装开发依赖
uv sync --extra ocr --extra dev

# 运行测试
uv run pytest tests/ -v

# 代码检查
uv run ruff check src/

# 启动 MCP 服务器（调试）
uv run python -B scripts/start_mcp.py
```

---

## 许可证

GPL-3.0-or-later · 详见 [LICENSE](LICENSE)
