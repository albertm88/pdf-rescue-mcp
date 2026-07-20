# 中文PDF书籍救援 MCP

[![Python](https://img.shields.io/badge/Python-3.11+-blue.svg)](https://www.python.org/)
[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)
[![MCP](https://img.shields.io/badge/MCP-1.13.0+-purple.svg)](https://modelcontextprotocol.io/)

中文 | [English](README_EN.md)

面向中文书籍 PDF 的本地 MCP 服务与命令行工具，支持扫描版 PDF 的 OCR 文本提取、质量巡检、断点续传和批量处理。

当前发布线：**1.0.0**。

完整的运行边界、恢复状态机与跨平台策略见 [1.0 架构说明](docs/ARCHITECTURE_1.0.md)。

## 特性

- **一键救援**：单个 `rescue_pdf` 工具自动完成诊断→规划→提取→质检全流程
- **三层运行内核**：业务层（独立 OCR、原子页缓存）+ 监管层（SQLite 租约、心跳/页级前进、跨平台进程树治理）+ 迭代更新层（仅生成可审计、需批准的质量改善计划）
- **批量处理**：后台独立 worker 调度，MCP 服务器保持响应；按 CPU 线程、可用内存和 worker 实时占用动态分配并发
- **断点续传**：逐页缓存，中断后自动从断点恢复
- **质量审计**：低置信页检测、失败页记录、页面证据导出
- **CPU/GPU 自适应**：自动检测 NVIDIA GPU 加速，CPU 模式自动优化线程数
- **中文优化**：内置术语词表、OCR 后处理、中文标点清理

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
│  │  │ 失联/卡页 → 安全停止 → 断点恢复；不占用 MCP  │  │ │
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
                            │
┌───────────────────────────▼─────────────────────────────┐
│  业务输出 + 监管状态 + 迭代建议（建议永不直接改写运行代码） │
└─────────────────────────────────────────────────────────┘
```

## 安装

### 环境要求

- Python ≥ 3.11
- [uv](https://docs.astral.sh/uv/) 包管理器
- Windows、Linux 或 macOS（Python 核心、任务监管与进程治理均跨平台；具体 OCR 后端/GPU 依赖以本机 `run_health_check` 为准）

### 安装步骤

```bash
# 克隆仓库
git clone <repo-url>
cd pdf-rescue-mcp

# 安装基础依赖 + OCR 扩展
uv sync --extra ocr

# 如需 NVIDIA GPU 加速（CUDA 11.8）
uv sync --extra ocr-gpu
```

### MCP stdio 跨客户端配置

服务默认以标准 **stdio** MCP 方式启动；OCR 工作在独立子进程中。也可选择仅绑定本机回环地址的 Streamable HTTP，但下列客户端模板均不依赖 HTTP/SSE。先在项目根目录安装 CPU OCR 依赖：

```bash
uv sync --extra ocr
```

仓库中的启动命令统一为 `uv run --locked --extra ocr python -B scripts/start_mcp.py`：默认只启用 CPU OCR，**不会**拉取 GPU 依赖。需要 NVIDIA GPU 时，再显式执行 `uv sync --extra ocr-gpu` 并按本机环境调整配置。

| 客户端 | 模板 | 配置形式 |
|------|------|----------|
| VS Code | `.vscode/mcp.json`、`examples/mcp-config.vscode.json` | `servers` + `type: "stdio"` |
| Claude Desktop / Cursor | `examples/mcp-config.claude-cursor.json` | 标准 `mcpServers` JSON |
| Codex | `examples/mcp-config.codex.toml` | 合并到 `~/.codex/config.toml` 的 `mcp_servers` 段 |
| AnythingLLM | `examples/mcp-config.anythingllm.json` | 标准 `mcpServers` JSON；默认 `autoStart: false`，避免 OCR 在应用启动时常驻占用资源 |
| TRAE | `examples/mcp-config.trae.json` → `<项目根目录>/.trae/mcp.json` | 标准 `mcpServers` stdio 配置；先在 Settings > MCP 启用项目级 MCP |

除 VS Code 的 `${workspaceFolder}` 外，模板中的 `{{PROJECT_ROOT}}` 必须替换为项目根目录的**绝对路径**。不要把替换后的个人配置提交回仓库。

可由生成器直接生成带绝对 `cwd` 的本机配置，避免客户端从未知工作目录启动后找不到 `scripts/start_mcp.py`：

```bash
# JSON：generic / claude / cursor / trae / vscode / anythingllm
uv run --locked --extra ocr python scripts/generate_mcp_config.py \
  --client anythingllm --output <本机配置文件路径>

# TOML：生成后将该段合并到 ~/.codex/config.toml
uv run --locked --extra ocr python scripts/generate_mcp_config.py \
  --client codex --output <本机Codex配置片段路径>
```

`--runner auto` 会优先生成 `uv` 启动配置；也可以传入 `--runner python` 或 Windows 的 `--runner py`。Claude、Cursor、AnythingLLM 和 TRAE 都使用同一条 stdio 启动命令；TRAE 的项目配置放在 `.trae/mcp.json`。若改用可选 HTTP，只应使用本机回环地址，且应在客户端版本确认支持 Streamable HTTP 后再启用。

### 可选：本机 Streamable HTTP

适用于不能拉起本地 stdio 子进程、但支持 Streamable HTTP 的 MCP 客户端。服务默认不启用该模式；启用后只允许绑定回环地址，避免本机 PDF 访问能力直接暴露到局域网：

```powershell
$env:PDF_RESCUE_MCP_TRANSPORT = "streamable-http"
$env:PDF_RESCUE_MCP_HOST = "127.0.0.1"
$env:PDF_RESCUE_MCP_PORT = "8765"
uv run --locked --extra ocr python -B scripts/start_mcp.py
```

MCP 地址为 `http://127.0.0.1:8765/mcp`。若要跨机器访问，请部署在带认证的 MCP 网关之后；本服务不会以无认证形式监听 `0.0.0.0`。

## MCP 工具列表

### 核心工具

| 工具 | 说明 |
|------|------|
| `rescue_pdf` | **首选入口**：自动诊断→规划→提取→质检，传入 PDF 路径即可 |
| `extract_book_text` | 提取书籍文本，后台子进程运行，立即返回任务目录 |
| `get_job_status` | 查询任务进度（页数、速度、剩余时间、线程健康） |
| `resume_job` | 恢复中断的任务（断点续传） |
| `cancel_job` | 请求任务在当前页边界安全停止，不阻塞 MCP |
| `audit_job_quality` | 质量巡检（低置信页、失败页、分裂标题检测） |
| `get_iteration_plan` | 生成版本化、仅建议的质量/资源改善计划，需人工批准后再执行 |

### 批量处理

| 工具 | 说明 |
|------|------|
| `batch_extract_library` | 批量提取书库，后台逐本处理，立即返回 |
| `get_batch_status` | 查看批量进度（书本完成数/总数、当前书籍、页数、预计剩余时间、worker 资源与调度依据） |

资源字段中，`CPU占用率` 始终表示该 worker 占整机逻辑 CPU 能力的比例（0–100%）；多线程累计负载以 `CPU等效核心数` 表示，`线程CPU占用率` 按线程 ID 单独列出且每项不超过 100%。
| `stop_batch` | 停止批量（当前书籍继续完成） |
| `scan_pdf_library` | 扫描书库，生成 PDF 清单和建议处理动作 |

### OCR 容量调优

| 工具 | 说明 |
|------|------|
| `plan_ocr_capacity_profile` | 规划独立的 2/4/6/8 线程与多 worker 吞吐基准；发现生产 OCR 正在运行时只返回“已延期”，绝不启动压测。 |
| `start_ocr_capacity_profile` | 仅在机器没有生产 OCR 时后台执行已规划基准；每个候选使用私有、不重叠的 PDF 页样本，不阻塞 MCP。 |
| `get_ocr_capacity_profile` | 查看页吞吐、每个 worker 的 RSS、活跃/饱和线程、线程利用率、质量门禁及仅建议的推荐结果。 |
| `activate_ocr_capacity_profile` | 显式激活完整完成的推荐策略，只影响之后新启动的 worker；不会热改、重启或中断运行中的 OCR。 |

16 个逻辑线程机器默认先测 `1x2`、`1x4`、`1x6`、`1x8`，再在预留系统线程后的逻辑线程预算内测试多 worker（例如 `2x2`、`2x4`、`2x6`、`3x2`、`3x4`、`4x2`）。候选之间串行，候选内部 worker 并行。选择不只看整机占用率：以真实 OCR 页/分钟为主，再结合每 worker 的线程预算、线程利用率、RSS、外部 CPU 负载和质量门禁；吞吐相差 5% 以内时优先更少线程、更低 RSS 的组合。结果默认仅建议，必须显式激活。

### 诊断与规划

| 工具 | 说明 |
|------|------|
| `run_health_check` | 运行体检（CPU/内存/GPU/OCR 依赖），快速模式不加载模型 |
| `inspect_pdf_text_layer` | 检查 PDF 文本层（扫描/原生/混合/加密） |
| `diagnose_pdf` | 诊断 PDF 类型、乱码风险、扫描页比例 |
| `plan_pdf_job` | 规划处理路线（模式、预计耗时、引擎选择） |

### 证据与词表

| 工具 | 说明 |
|------|------|
| `get_page_evidence` | 查看指定页的识别文本、置信度、识别块 |
| `export_page_image_evidence` | 导出页面渲染图片用于核对 |
| `get_term_glossary` | 查看术语词表（书名限定错字替换规则） |
| `update_term_glossary` | 添加术语替换规则 |

### 历史记录

| 工具 | 说明 |
|------|------|
| `get_processing_history` | 查看处理历史（状态、页数、质量指标） |
| `share_processing_history` | 生成可分享的历史记录（JSON/Markdown/HTML） |

## 命令行用法

除 MCP 服务外，也可直接使用命令行：

```bash
# 体检
uv run python -m pdf_rescue_mcp.cli 体检

# 检查 PDF
uv run python -m pdf_rescue_mcp.cli 检查 <pdf路径>

# 规划处理
uv run python -m pdf_rescue_mcp.cli 规划 <pdf路径>

# 提取文本
uv run python -m pdf_rescue_mcp.cli 提取 <pdf路径> --mode book-fast --output-dir <输出目录>

# 查询状态
uv run python -m pdf_rescue_mcp.cli 状态 <任务目录>

# 恢复任务
uv run python -m pdf_rescue_mcp.cli 恢复 <任务目录>

# 质量巡检
uv run python -m pdf_rescue_mcp.cli 质检 <任务目录>

# 书库扫描
uv run python -m pdf_rescue_mcp.cli 书库扫描 <书库目录>

# 批量提取
uv run python -m pdf_rescue_mcp.cli 书库提取 <书库目录> --output-dir <输出目录> --mode book-fast
```

## 识别模式

| 模式 | DPI | 适用场景 | 速度（CPU） |
|------|-----|----------|-------------|
| `book-fast` | 180 | 快速预览、大批量处理 | ~8-30秒/页 |
| `book-balanced` | 220 | 日常使用（默认） | ~15-45秒/页 |
| `book-quality` | 300 | 高质量输出 | ~30-90秒/页 |
| `book-forensic` | 300+ | 取证级、低质量扫描件 | ~60-180秒/页 |

## 1.0 三层运行架构

| 层级 | 机制 | 参数 |
|------|------|------|
| **业务层** | 独立 OCR 子进程、逐页缓存、原子 `状态.json` / JSONL；每页完成后才推进业务状态 | 页数、进度、速度、ETA、质量证据 |
| **监管层** | `LocalSupervisor` + 本机 SQLite WAL 任务账本、fencing lease、心跳与“当前页/最后完成页”双信号、`psutil` 进程树收尾 | `WATCH_INTERVAL=5s`, `HEARTBEAT_TIMEOUT=90s`, `PROGRESS_TIMEOUT=600s`, `CANCEL_GRACE=45s` |
| **迭代更新层** | 从质量巡检、页级结果和监管事件生成版本化建议；只能由调用者审核后重新发起任务 | `get_iteration_plan`，`strategy_version=1.0.0` |

OCR 路由无论客户端传入何种前台偏好，都会使用独立工作进程；MCP 适配器只负责启动、查询、停止和恢复，因此 VS Code、TRAE、Codex、AnythingLLM 等 Host 不会被持续 OCR 占住。

### 跨平台运行目录

监管层不会把长期任务状态绑在源码目录。默认位置遵循操作系统约定：Windows 使用 `%APPDATA%` / `%LOCALAPPDATA%`，macOS 使用 `~/Library/Application Support` / `Caches` / `Logs`，Linux 使用 XDG 目录。便携部署可显式设置绝对路径：

```bash
export PDF_RESCUE_RUNTIME_ROOT=/absolute/path/to/pdf-rescue-runtime
```

Windows PowerShell 示例：

```powershell
$env:PDF_RESCUE_RUNTIME_ROOT = "D:\pdf-rescue-runtime"
```

SQLite 数据库仅支持本机磁盘；不要放在网络共享盘、同步盘或多机共享文件系统。`PDF_RESCUE_TASK_DATABASE` 和 `PDF_RESCUE_TASK_ATTEMPT_ID` 是监管层传给 OCR 子进程的内部变量，用户无需手动配置。

## 输出结构

```
<输出目录>/<书名>-rescue-result/
├── 状态.json              # 实时进度（书名、页数、百分比、速度、运行/剩余时间、引擎）
├── 清单.yaml              # 处理清单和配置
├── 文本/
│   └── 全书.md            # 合并的全文 Markdown
├── 数据/
│   ├── 页面.jsonl         # 逐页文本 + 置信度 + 来源
│   ├── 片段.jsonl         # 分段文本
│   ├── 质量.json          # 质量报告（低置信页、失败页统计）
│   ├── 低置信页.jsonl     # 低置信页详情
│   └── 失败页.jsonl       # 失败页详情
├── 缓存/
│   └── 页面OCR/           # 逐页 OCR 缓存（断点续传用）
├── 审计/
│   └── 审计.html          # 可视化质量审计报告
└── 日志/                  # 子进程运行日志
```

## 性能优化

### CPU 线程配置

- 自动检测 CPU 核心数，保留 2 核给系统
- 常规批处理保持保守线程预算；容量基准可在保留系统线程后的逻辑线程预算内实测 2/4/6/8 与多 worker 组合，再以吞吐、线程利用率和 RSS 决定后续策略
- AMD Ryzen 7 5800H（8核/16线程）实测：book-fast 模式约 8-15秒/页

### GPU 加速

- NVIDIA GPU：安装 `ocr-gpu` 扩展，自动启用 CUDA 加速（3-5倍提速）
- AMD GPU：Windows 下 PaddlePaddle 不支持 ROCm，需使用 Linux
- MKLDNN/oneDNN：AMD CPU 上存在兼容性问题（`NotImplementedError`），默认关闭

## 后处理：条目拆分

OCR 完成后，可使用 `scripts/split_into_entries_v2.py` 将全书按百科条目拆分为独立 Markdown 文件：

```bash
uv run python scripts/split_into_entries_v2.py \
  <rescue-result目录> \
  <最终输出目录>
```

输出结构：
```
<最终输出目录>/<书名>/
├── 前言/
│   └── 前言与凡例.md
├── 条目/
│   ├── 鳖甲.md
│   ├── 冰硼散.md
│   └── ...（数百个条目文件）
└── 索引.md
```

## 常见问题

### Q: 为什么速度一直是 30-40秒/页？
A: CPU 模式下这是正常速度。可尝试：
- 使用 `book-fast` 模式（DPI=180）
- 确认没有其他 CPU 密集型程序运行
- NVIDIA GPU 用户安装 `ocr-gpu` 扩展可提速 3-5 倍

### Q: 子进程频繁重启怎么办？
A: 检查日志文件 `logs/` 下的错误信息。常见原因：
- 内存不足：关闭其他程序，或使用 `book-fast-low-memory` 模式
- MKLDNN 兼容性：AMD CPU 上已默认关闭
- PDF 文件损坏：先用 `diagnose_pdf` 检查

### Q: 断点续传如何工作？
A: 每完成一页，OCR 结果缓存到 `缓存/页面OCR/*.json`。重启时自动跳过已缓存页面，从断点继续。

### Q: 如何处理密码保护的 PDF？
A: 使用 `rescue_pdf` 时传入 `password` 参数。密码不会写入记录文件。

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
