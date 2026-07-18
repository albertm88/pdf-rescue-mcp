# 中文PDF书籍救援 MCP

中文 | [English](README_EN.md)

面向中文书籍 PDF 的本地 MCP 服务与命令行工具，支持扫描版 PDF 的 OCR 文本提取、质量巡检、断点续传和批量处理。

## 特性

- **一键救援**：单个 `rescue_pdf` 工具自动完成诊断→规划→提取→质检全流程
- **三层监控机制**：业务层（子进程隔离）+ 监控层（10s 心跳检测）+ 优化层（卡死自动重启降级）
- **批量处理**：后台线程逐本迭代，MCP 服务器保持响应，实时进度查询
- **断点续传**：逐页缓存，中断后自动从断点恢复
- **质量审计**：低置信页检测、失败页记录、页面证据导出
- **CPU/GPU 自适应**：自动检测 NVIDIA GPU 加速，CPU 模式自动优化线程数
- **中文优化**：内置术语词表、OCR 后处理、中文标点清理

## 架构

```
┌─────────────────────────────────────────────────────────┐
│                  VS Code / MCP 客户端                    │
└──────────────────────────┬──────────────────────────────┘
                           │ JSON-RPC (stdio)
┌──────────────────────────▼──────────────────────────────┐
│              FastMCP 服务器进程（常驻）                    │
│                                                         │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌─────────┐ │
│  │rescue_pdf│  │ 体检/诊断 │  │ 状态查询  │  │ 批量管理 │ │
│  └────┬─────┘  └──────────┘  └──────────┘  └────┬────┘ │
│       │                                         │      │
│  ┌────▼─────────────────────────────────────────▼────┐ │
│  │              _TaskManager / _BatchManager          │ │
│  │  ┌─────────────────────────────────────────────┐  │ │
│  │  │ 三层机制：业务(子进程) → 监控(10s轮询) → 优化 │  │ │
│  │  │ 180s超时 → terminate → 降级book-fast重启1次  │  │ │
│  │  └─────────────────────────────────────────────┘  │ │
│  └────────────────────────┬──────────────────────────┘ │
└───────────────────────────┼─────────────────────────────┘
                            │ subprocess.Popen
┌───────────────────────────▼─────────────────────────────┐
│               子进程（隔离 OCR）                          │
│  python -u -m pdf_rescue_mcp.cli 提取                    │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌─────────┐ │
│  │ 渲染页面  │→│ PaddleOCR │→│ 写状态.json│→│ 缓存页面 │ │
│  └──────────┘  └──────────┘  └──────────┘  └─────────┘ │
└─────────────────────────────────────────────────────────┘
                            │
┌───────────────────────────▼─────────────────────────────┐
│                      输出文件                             │
│  文本/全书.md · 数据/页面.jsonl · 数据/质量.json · 缓存/  │
└─────────────────────────────────────────────────────────┘
```

## 安装

### 环境要求

- Python ≥ 3.11
- [uv](https://docs.astral.sh/uv/) 包管理器
- Windows 10/11（主要测试平台）或 Linux

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

### VS Code MCP 配置

在 `.vscode/mcp.json` 中添加：

```json
{
  "servers": {
    "中文PDF书籍救援": {
      "command": "uv",
      "args": ["run", "--extra", "ocr", "--extra", "dev", "python", "-B", "scripts/start_mcp.py"],
      "cwd": "${workspaceFolder}",
      "env": {
        "PYTHONUTF8": "1",
        "PYTHONIOENCODING": "utf-8"
      }
    }
  }
}
```

## MCP 工具列表

### 核心工具

| 工具 | 说明 |
|------|------|
| `rescue_pdf` | **首选入口**：自动诊断→规划→提取→质检，传入 PDF 路径即可 |
| `extract_book_text` | 提取书籍文本，后台子进程运行，立即返回任务目录 |
| `get_job_status` | 查询任务进度（页数、速度、剩余时间、线程健康） |
| `resume_job` | 恢复中断的任务（断点续传） |
| `audit_job_quality` | 质量巡检（低置信页、失败页、分裂标题检测） |

### 批量处理

| 工具 | 说明 |
|------|------|
| `batch_extract_library` | 批量提取书库，后台逐本处理，立即返回 |
| `get_batch_status` | 查看批量进度（总书数、已完成、当前书籍、预计剩余时间） |
| `stop_batch` | 停止批量（当前书籍继续完成） |
| `scan_pdf_library` | 扫描书库，生成 PDF 清单和建议处理动作 |

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

## 三层监控机制

| 层级 | 机制 | 参数 |
|------|------|------|
| **业务层** | 子进程隔离（`subprocess.Popen`），OCR 崩溃不影响 MCP 服务器 | - |
| **监控层** | Watcher 线程每 10 秒检测：子进程存活（`poll()`）+ 状态文件新鲜度（`mtime`） | `WATCH_INTERVAL=10s` |
| **优化层** | 单页超时 180 秒判定卡死 → `terminate()` 强杀 → 降级 `book-fast` 重启 1 次 | `PAGE_TIMEOUT=180s`, `MAX_AUTO_RESTART=1` |

## 输出结构

```
<输出目录>/<书名>-rescue-result/
├── 状态.json              # 实时进度（页数、速度、ETA、引擎）
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
- 线程数上限为物理核心数（通常 8 线程），避免 HyperThreading 争抢
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

