# Chinese PDF Book Rescue MCP

[![Python](https://img.shields.io/badge/Python-3.11+-blue.svg)](https://www.python.org/)
[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)
[![MCP](https://img.shields.io/badge/MCP-1.13.0+-purple.svg)](https://modelcontextprotocol.io/)

[中文](README.md) | English

A local MCP service and CLI tool for Chinese book PDFs, supporting OCR text extraction from scanned PDFs, quality auditing, resume from breakpoints, and batch processing.

Current release line: **1.0.0**. Full architecture details in [1.0 architecture document](docs/ARCHITECTURE_1.0.md).

---

## Quick Start

### Step 1: Prerequisites

- **Python ≥ 3.11** ([download](https://www.python.org/downloads/))
- **[uv](https://docs.astral.sh/uv/)** package manager

```bash
# Verify
python --version   # should be ≥ 3.11
uv --version
```

### Step 2: Clone & Install

```bash
git clone https://github.com/<your-username>/pdf-rescue-mcp.git
cd pdf-rescue-mcp

# CPU mode (all platforms)
uv sync --extra ocr

# NVIDIA GPU acceleration (CUDA 11.8, 3-5x speedup)
uv sync --extra ocr-gpu
```

### Step 3: Health Check

```bash
uv run python -m pdf_rescue_mcp.cli 体检
```

Should display CPU cores, available memory, OCR engine status, etc.

### Step 4: Configure MCP Client

Generate a local config with absolute paths so clients can locate the launch script:

```bash
# Pick your client:
uv run python scripts/generate_mcp_config.py --client vscode     --output .vscode/mcp.json
uv run python scripts/generate_mcp_config.py --client claude     --output ~/claude-mcp.json
uv run python scripts/generate_mcp_config.py --client cursor     --output ~/cursor-mcp.json
uv run python scripts/generate_mcp_config.py --client anythingllm --output ~/anythingllm-mcp.json
uv run python scripts/generate_mcp_config.py --client trae       --output .trae/mcp.json
uv run python scripts/generate_mcp_config.py --client codex      --output ~/codex-mcp.toml
```

> Alternatively, copy templates from `examples/` and replace `{{PROJECT_ROOT}}` with the absolute project path. VS Code users can directly use `examples/mcp-config.vscode.json` (`${workspaceFolder}` needs no replacement).

### Step 5: Start Using

In your MCP client, simply tell the AI:

> Extract text from `D:\scanned-books\some-book.pdf`

The AI will automatically call the `rescue_pdf` tool to complete the full pipeline: diagnose → plan → OCR → audit. Results are saved to `<PDF-sibling-dir>/pdf_rescue_output/<book-name>-rescue-result/`.

---

## Batch Processing

For large collections of PDFs:

```bash
# Scan library
uv run python -m pdf_rescue_mcp.cli 书库扫描 <library-dir>

# Start batch extraction (background, resumable)
uv run python -B scripts/batch_extract_all.py
```

The batch controller:
- Auto-discovers all PDFs in the library
- Dynamically allocates concurrency based on CPU, memory, and per-worker load
- Caches per-page, auto-resumes from breakpoint after restart
- Prints progress every 30 seconds (completed/in-progress/queued books, pages, ETA, resource usage)

The default batch script processes PDFs from `D:\BaiduNetdiskDownload\dabao` and outputs to `D:\农业百科全书-转文字\`. Edit `ROOT` and `OUTPUT` in `scripts/batch_extract_all.py` to match your library.

---

## Features

- **One-Click Rescue**: Single `rescue_pdf` tool automates the full pipeline: diagnose → plan → extract → audit
- **Three-Layer Runtime**: Business layer (isolated OCR, atomic page cache) + Supervision layer (SQLite lease, heartbeat/page-progress watchdog, portable process control) + Iteration layer (auditable advisory plans only)
- **Batch Processing**: Independent OCR workers keep MCP responsive; dynamic concurrency based on CPU threads, available memory, and per-worker utilization
- **Resume from Breakpoints**: Per-page caching, auto-resume from last checkpoint after interruption
- **Quality Auditing**: Low-confidence page detection, failed page logging, page evidence export
- **CPU/GPU Auto-Detection**: Automatically detects NVIDIA GPU acceleration; CPU mode with optimized thread count
- **Chinese Optimization**: Built-in term glossary, OCR post-processing, Chinese punctuation cleanup

## Recognition Modes

| Mode | DPI | Use Case | Speed (CPU) |
|------|-----|----------|-------------|
| `book-fast` | 180 | Quick preview, large batch processing | ~8-30s/page |
| `book-balanced` | 220 | Daily use (default) | ~15-45s/page |
| `book-quality` | 300 | High-quality output | ~30-90s/page |
| `book-forensic` | 300+ | Forensic level, low-quality scans | ~60-180s/page |

## Output Structure

```
<output-dir>/<book-name>-rescue-result/
├── 状态.json              # Real-time progress (pages, speed, ETA, engine)
├── 清单.yaml              # Processing manifest and config
├── 文本/
│   └── 全书.md            # Merged full text in Markdown
├── 数据/
│   ├── 页面.jsonl         # Per-page text + confidence + source
│   ├── 片段.jsonl         # Segment text
│   ├── 质量.json          # Quality report (low-confidence, failed page stats)
│   ├── 低置信页.jsonl     # Low-confidence page details
│   └── 失败页.jsonl       # Failed page details
├── 缓存/
│   └── 页面OCR/           # Per-page OCR cache (for resume)
├── 审计/
│   └── 审计.html          # Visual quality audit report
└── 日志/                  # Subprocess runtime logs
```

## CLI Usage

Besides MCP, you can use the CLI directly:

```bash
# Health check
uv run python -m pdf_rescue_mcp.cli 体检

# Inspect PDF
uv run python -m pdf_rescue_mcp.cli 检查 <pdf-path>

# Plan processing
uv run python -m pdf_rescue_mcp.cli 规划 <pdf-path>

# Extract single book
uv run python -m pdf_rescue_mcp.cli 提取 <pdf-path> --mode book-fast --output-dir <output-dir>

# Query status
uv run python -m pdf_rescue_mcp.cli 状态 <task-dir>

# Resume task
uv run python -m pdf_rescue_mcp.cli 恢复 <task-dir>

# Quality audit
uv run python -m pdf_rescue_mcp.cli 质检 <task-dir>

# Scan library
uv run python -m pdf_rescue_mcp.cli 书库扫描 <library-dir>

# Batch extract
uv run python -m pdf_rescue_mcp.cli 书库提取 <library-dir> --output-dir <output-dir> --mode book-fast
```

## Post-Processing: Entry Splitting

After OCR completes, use `scripts/split_into_entries_v2.py` to split the full text into individual encyclopedia entry Markdown files:

```bash
uv run python scripts/split_into_entries_v2.py \
  <rescue-result-dir> \
  <final-output-dir>
```

Output structure:
```
<final-output-dir>/<book-name>/
├── 前言/
│   └── 前言与凡例.md
├── 条目/
│   ├── 鳖甲.md
│   ├── 冰硼散.md
│   └── ... (hundreds of entry files)
└── 索引.md
```

---

## MCP Tools Reference

### Core Tools

| Tool | Description |
|------|-------------|
| `rescue_pdf` | **Primary entry point**: Auto diagnose → plan → extract → audit. Just pass the PDF path. |
| `extract_book_text` | Extract book text, runs in background subprocess, returns task directory immediately. |
| `get_job_status` | Query task progress (pages, speed, ETA, thread health). |
| `resume_job` | Resume interrupted task (breakpoint continuation). |
| `cancel_job` | Request a safe page-boundary stop. |
| `audit_job_quality` | Quality audit (low-confidence pages, failed pages, split heading detection). |
| `get_iteration_plan` | Produce a versioned quality/resource improvement plan, requires manual approval. |

### Batch Processing

| Tool | Description |
|------|-------------|
| `batch_extract_library` | Batch extract library, processes books sequentially in background. |
| `get_batch_status` | View completed/total books, current book pages, ETA, worker resources. |
| `stop_batch` | Stop batch (current book finishes, no new books started). |
| `scan_pdf_library` | Scan library directory, generate PDF manifest with recommended actions. |

### Diagnostics & Planning

| Tool | Description |
|------|-------------|
| `run_health_check` | Run health check (CPU/RAM/GPU/OCR dependencies). |
| `inspect_pdf_text_layer` | Inspect PDF text layer (scanned/native/hybrid/encrypted). |
| `diagnose_pdf` | Diagnose PDF type, garbling risk, scanned page ratio. |
| `plan_pdf_job` | Plan processing route (mode, estimated time, engine selection). |

### Evidence & Glossary

| Tool | Description |
|------|-------------|
| `get_page_evidence` | View recognized text, confidence, and blocks for a specific page. |
| `export_page_image_evidence` | Export rendered page image for verification. |
| `get_term_glossary` | View term glossary. |
| `update_term_glossary` | Add a term replacement rule. |

### OCR Capacity Profiling

| Tool | Description |
|------|-------------|
| `plan_ocr_capacity_profile` | Plan isolated 2/4/6/8-thread and multi-worker throughput benchmarks. |
| `start_ocr_capacity_profile` | Start planned profile in background (only when no production OCR is active). |
| `get_ocr_capacity_profile` | View page throughput, RSS, thread utilization, quality gates. |
| `activate_ocr_capacity_profile` | Activate recommendation for future workers only. |

### History

| Tool | Description |
|------|-------------|
| `get_processing_history` | View processing history. |
| `share_processing_history` | Generate shareable history records (JSON/Markdown/HTML). |

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                  VS Code / MCP Client                    │
└──────────────────────────┬──────────────────────────────┘
                           │ JSON-RPC (stdio)
┌──────────────────────────▼──────────────────────────────┐
│              FastMCP Server Process (Persistent)         │
│                                                         │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌─────────┐ │
│  │rescue_pdf│  │ Health/  │  │  Status  │  │  Batch  │ │
│  │          │  │ Diagnose │  │  Query   │  │ Manager │ │
│  └────┬─────┘  └──────────┘  └──────────┘  └────┬────┘ │
│       │                                         │      │
│  ┌────▼─────────────────────────────────────────▼────┐ │
│  │              _TaskManager / _BatchManager          │ │
│  │  ┌─────────────────────────────────────────────┐  │ │
│  │  │ Three Layers: Business → Monitor → Optimize │  │ │
│  │  │ 180s timeout → terminate → downgrade+retry  │  │ │
│  │  └─────────────────────────────────────────────┘  │ │
│  └────────────────────────┬──────────────────────────┘ │
└───────────────────────────┼─────────────────────────────┘
                            │ subprocess.Popen
┌───────────────────────────▼─────────────────────────────┐
│               Subprocess (Isolated OCR)                  │
│  python -u -m pdf_rescue_mcp.cli extract                 │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌─────────┐ │
│  │ Render   │→│ PaddleOCR │→│ Write    │→│ Cache   │ │
│  │ Page     │  │ Recognition│ │ Status   │  │ Page    │ │
│  └──────────┘  └──────────┘  └──────────┘  └─────────┘ │
└─────────────────────────────────────────────────────────┘
```

## 1.0 Three-Layer Runtime

| Layer | Mechanism | Parameters |
|-------|-----------|------------|
| **Business** | Isolated OCR, per-page cache, atomic status/JSONL commits | progress, speed, ETA, quality evidence |
| **Supervision** | SQLite WAL task ledger, fencing lease, heartbeat + page-progress signals, `psutil` process-tree cleanup | `WATCH_INTERVAL=5s`, `HEARTBEAT_TIMEOUT=90s`, `PROGRESS_TIMEOUT=600s` |
| **Iteration** | Versioned plans from audit results and supervision events; manual approval required | `get_iteration_plan`, `strategy_version=1.0.0` |

### Cross-platform runtime state

Supervision state uses OS-standard directories (Windows `%APPDATA%`, macOS `~/Library`, Linux XDG). Override with:

```bash
# Linux/macOS
export PDF_RESCUE_RUNTIME_ROOT=/path/to/pdf-rescue-runtime

# Windows PowerShell
$env:PDF_RESCUE_RUNTIME_ROOT = "D:\pdf-rescue-runtime"
```

> Keep the SQLite task database on a local disk, not a network or sync share.

### Optional: Streamable HTTP mode

For MCP clients that cannot launch stdio subprocesses. Loopback-only:

```powershell
$env:PDF_RESCUE_MCP_TRANSPORT = "streamable-http"
$env:PDF_RESCUE_MCP_HOST = "127.0.0.1"
$env:PDF_RESCUE_MCP_PORT = "8765"
uv run --locked --extra ocr python -B scripts/start_mcp.py
```

---

## Performance Optimization

### CPU Thread Configuration

- Auto-detects CPU core count, reserves 2 cores for system
- Normal batch starts remain conservative; capacity profiling can measure 2/4/6/8 thread and multi-worker combinations
- AMD Ryzen 7 5800H (8 cores/16 threads) tested: book-fast mode ~8-15s/page

### GPU Acceleration

- **NVIDIA GPU**: Install `ocr-gpu` extras for automatic CUDA acceleration (3-5x speedup)
- **AMD GPU**: PaddlePaddle doesn't support ROCm on Windows; Linux required
- **MKLDNN/oneDNN**: Disabled by default on AMD CPUs

---

## FAQ

### Q: Why is speed always 30-40 seconds per page?
A: This is normal for CPU mode. Try `book-fast` mode (DPI=180), or install NVIDIA GPU extras.

### Q: What if subprocesses restart frequently?
A: Check error logs in `logs/`. Common causes: out of memory, corrupted PDF (run `diagnose_pdf` first).

### Q: How does resume from breakpoints work?
A: After each page completes, OCR results are cached to `缓存/页面OCR/*.json`. On restart, cached pages are skipped automatically.

### Q: How to handle password-protected PDFs?
A: Pass the `password` parameter when using `rescue_pdf`. Passwords are never written to record files.

---

## Development

```bash
# Install dev dependencies
uv sync --extra ocr --extra dev

# Run tests
uv run pytest tests/ -v

# Lint
uv run ruff check src/

# Start MCP server (debug)
uv run python -B scripts/start_mcp.py
```

## License

GPL-3.0-or-later - see [LICENSE](LICENSE) for details.
