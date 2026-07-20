# 中文PDF书籍救援 MCP

[![Python](https://img.shields.io/badge/Python-3.11+-blue.svg)](https://www.python.org/)
[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)
[![MCP](https://img.shields.io/badge/MCP-1.13.0+-purple.svg)](https://modelcontextprotocol.io/)

中文 | [English](README_EN.md)

面向中文书籍 PDF 的本地 MCP 服务与命令行工具，支持扫描版 PDF 的 OCR 文本提取、质量巡检、断点续传和批量处理。

当前发布线：**1.0.0**。完整架构说明见 [1.0 架构文档](docs/ARCHITECTURE_1.0.md)。

---

## 快速开始

### 第一步：环境准备

- **Python >= 3.11**（[下载](https://www.python.org/downloads/)）
- **[uv](https://docs.astral.sh/uv/)** 包管理器（安装后确保 `uv` 命令可用）

```bash
# 验证环境
python --version   # 应 >= 3.11
uv --version
```

### 第二步：克隆并安装

```bash
git clone https://github.com/albertm88/pdf-rescue-mcp.git
cd pdf-rescue-mcp

# CPU 模式（适用所有平台）
uv sync --extra ocr

# NVIDIA GPU 加速（CUDA 11.8，3-5 倍提速）
uv sync --extra ocr-gpu
```

### 第三步：体检确认

```bash
uv run python -m pdf_rescue_mcp.cli 体检
```

输出应显示 CPU 核心数、可用内存、OCR 引擎状态等信息。

### 第四步：配置 MCP 客户端

生成带绝对路径的本机配置，避免客户端找不到启动脚本：

```bash
# 根据你的客户端选择：
uv run python scripts/generate_mcp_config.py --client vscode     --output .vscode/mcp.json
uv run python scripts/generate_mcp_config.py --client claude     --output ~/claude-mcp.json
uv run python scripts/generate_mcp_config.py --client cursor     --output ~/cursor-mcp.json
uv run python scripts/generate_mcp_config.py --client anythingllm --output ~/anythingllm-mcp.json
uv run python scripts/generate_mcp_config.py --client trae       --output .trae/mcp.json
uv run python scripts/generate_mcp_config.py --client codex      --output ~/codex-mcp.toml
```

> 也可手动复制 `examples/` 下的模板，将 `{{PROJECT_ROOT}}` 替换为项目绝对路径。VS Code 用户直接用 `examples/mcp-config.vscode.json` 即可（`${workspaceFolder}` 无需替换）。

### 第五步：开始使用

在 MCP 客户端中直接对 AI 说：

> 帮我把 `D:\扫描书籍\某某书.pdf` 转成文字

AI 会自动调用 `rescue_pdf` 工具完成诊断->规划->OCR->质检全流程。单本书处理完成后，结果在 `<PDF同级目录>/pdf_rescue_output/<书名>-rescue-result/` 中。

---

## 批量处理书库

如果你有大量 PDF 需要批量 OCR：

```bash
# 扫描书库
uv run python -m pdf_rescue_mcp.cli 书库扫描 <书库目录>

# 启动批量提取（后台运行，断点续传）
uv run python -B scripts/batch_extract_all.py
```

批量控制器会：
- 自动发现书库中所有 PDF
- 按 CPU/内存/worker 实时占用动态分配并发
- 逐页缓存，中断后重启自动从断点继续
- 每 30 秒输出进度报告（已完成/进行中/排队书本、页数、ETA、资源占用）

批量脚本默认处理 `D:\BaiduNetdiskDownload\dabao` 下的 PDF，输出到 `D:\农业百科全书-转文字\`。修改 `scripts/batch_extract_all.py` 中的 `ROOT` 和 `OUTPUT` 变量即可适配你的书库。

---

## 特性

- **一键救援**：单个 `rescue_pdf` 工具自动完成诊断->规划->提取->质检全流程
- **三层运行内核**：业务层（独立 OCR、原子页缓存）+ 监管层（SQLite 租约、心跳/页级前进、跨平台进程树治理）+ 迭代更新层（仅生成可审计、需批准的质量改善计划）
- **批量处理**：后台独立 worker 调度，MCP 服务器保持响应；按 CPU 线程、可用内存和 worker 实时占用动态分配并发
- **断点续传**：逐页缓存，中断后自动从断点恢复
- **质量审计**：低置信页检测、失败页记录、页面证据导出
- **CPU/GPU 自适应**：自动检测 NVIDIA GPU 加速，CPU 模式自动优化线程数
- **中文优化**：内置术语词表、OCR 后处理、中文标点清理

## 识别模式

| 模式 | DPI | 适用场景 | 速度（CPU） |
|------|-----|----------|-------------|
| `book-fast` | 180 | 快速预览、大批量处理 | ~8-30秒/页 |
| `book-balanced` | 220 | 日常使用（默认） | ~15-45秒/页 |
| `book-quality` | 300 | 高质量输出 | ~30-90秒/页 |
| `book-forensic` | 300+ | 取证级、低质量扫描件 | ~60-180秒/页 |

## 输出结构

```
<输出目录>/<书名>-rescue-result/
├── 状态.json              # 实时进度
├── 清单.yaml              # 处理清单和配置
├── 文本/
│   └── 全书.md            # 合并的全文 Markdown
├── 数据/
│   ├── 页面.jsonl         # 逐页文本 + 置信度
│   ├── 质量.json          # 质量报告
│   ├── 低置信页.jsonl     # 低置信页详情
│   └── 失败页.jsonl       # 失败页详情
├── 缓存/
│   └── 页面OCR/           # 逐页 OCR 缓存（断点续传用）
├── 审计/
│   └── 审计.html          # 可视化质量审计报告
└── 日志/                  # 子进程运行日志
```

## 命令行用法

除了通过 MCP 客户端使用，也可直接在命令行操作：

```bash
# 体检
uv run python -m pdf_rescue_mcp.cli 体检

# 检查 PDF 类型
uv run python -m pdf_rescue_mcp.cli 检查 <pdf路径>

# 规划处理路线
uv run python -m pdf_rescue_mcp.cli 规划 <pdf路径>

# 提取单本书
uv run python -m pdf_rescue_mcp.cli 提取 <pdf路径> --mode book-fast --output-dir <输出目录>

# 查询进度
uv run python -m pdf_rescue_mcp.cli 状态 <任务目录>

# 恢复中断任务
uv run python -m pdf_rescue_mcp.cli 恢复 <任务目录>

# 质量巡检
uv run python -m pdf_rescue_mcp.cli 质检 <任务目录>

# 书库扫描
uv run python -m pdf_rescue_mcp.cli 书库扫描 <书库目录>

# 批量提取
uv run python -m pdf_rescue_mcp.cli 书库提取 <书库目录> --output-dir <输出目录> --mode book-fast
```

## 后处理：条目拆分

OCR 完成后，可使用 `scripts/split_into_entries_v2.py` 将全书按百科条目拆分为独立 Markdown 文件：

```bash
uv run python scripts/split_into_entries_v2.py   <rescue-result目录>   <最终输出目录>
```

---

## MCP 工具参考

### 核心工具

| 工具 | 说明 |
|------|------|
| `rescue_pdf` | **首选入口**：自动诊断->规划->提取->质检，传入 PDF 路径即可 |
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
| `share_processing_history` | 生成可分享的历史记录（JSON/Markdown/HTML） |

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
│  │  │ 失联/卡页 -> 安全停止 -> 断点恢复            │  │ │
│  │  └─────────────────────────────────────────────┘  │ │
│  └────────────────────────┬──────────────────────────┘ │
└───────────────────────────┼─────────────────────────────┘
                            │ subprocess.Popen
┌───────────────────────────▼─────────────────────────────┐
│               子进程（隔离 OCR）                          │
│  python -u -m pdf_rescue_mcp.cli 提取                    │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌─────────┐ │
│  │ 渲染页面  │->│ PaddleOCR │->│ 原子状态/缓存 │->│ 页级事件 │ │
│  └──────────┘  └──────────┘  └──────────┘  └─────────┘ │
└─────────────────────────────────────────────────────────┘
```

## 1.0 三层运行架构

| 层级 | 机制 | 参数 |
|------|------|------|
| **业务层** | 独立 OCR 子进程、逐页缓存、原子状态/JSONL | 页数、进度、速度、ETA、质量证据 |
| **监管层** | SQLite WAL 任务账本、fencing lease、心跳与页级前进、进程树收尾 | `WATCH_INTERVAL=5s`, `HEARTBEAT_TIMEOUT=90s`, `PROGRESS_TIMEOUT=600s` |
| **迭代更新层** | 从质量巡检和监管事件生成版本化建议，需人工审核 | `get_iteration_plan`, `strategy_version=1.0.0` |

### 跨平台运行目录

监管层默认使用操作系统标准目录（Windows `%APPDATA%`，macOS `~/Library`，Linux XDG）。可设置环境变量覆盖：

```bash
# Linux/macOS
export PDF_RESCUE_RUNTIME_ROOT=/path/to/pdf-rescue-runtime

# Windows PowerShell
$env:PDF_RESCUE_RUNTIME_ROOT = "D:\pdf-rescue-runtime"
```

> SQLite 数据库仅支持本机磁盘，不要放在网络共享盘或同步盘。

### 可选：Streamable HTTP 模式

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
- AMD Ryzen 7 5800H（8核/16线程）实测：book-fast 约 8-15秒/页

---

## 常见问题

### Q: 为什么速度一直是 30-40秒/页？
A: CPU 模式下这是正常速度。可尝试 `book-fast` 模式，或安装 NVIDIA GPU 扩展。

### Q: 断点续传如何工作？
A: 每完成一页，OCR 结果缓存到 `缓存/页面OCR/*.json`。重启时自动跳过已缓存页面。

### Q: 如何处理密码保护的 PDF？
A: 使用 `rescue_pdf` 时传入 `password` 参数。密码不会写入记录文件。

---

## 开发

```bash
# 安装开发依赖
uv sync --extra ocr --extra dev

# 运行测试
uv run pytest tests/ -v

# 代码检查
uv run ruff check src/

# 启动 MCP 服务器（调试用）
uv run python -B scripts/start_mcp.py
```

## 许可证

GPL-3.0-or-later - 详见 [LICENSE](LICENSE) 文件。
