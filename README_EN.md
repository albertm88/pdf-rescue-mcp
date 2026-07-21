# Chinese PDF Book Rescue MCP

[![Python](https://img.shields.io/badge/Python-3.11+-blue.svg)](https://www.python.org/)
[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)
[![MCP](https://img.shields.io/badge/MCP-1.13.0+-purple.svg)](https://modelcontextprotocol.io/)

[中文](README.md) | English

A local MCP service and CLI tool for Chinese book PDFs. Supports OCR text extraction from scanned PDFs, quality auditing, breakpoint resume, and batch processing.

Current release line: **1.0.0** · [Architecture docs](docs/ARCHITECTURE_1.0.md)

---

## Table of Contents

- [Quick Start](#quick-start)
  - [Step 1: Prerequisites](#step-1-prerequisites)
  - [Step 2: Clone & Install](#step-2-clone--install)
  - [Step 3: Health Check](#step-3-health-check)
  - [Step 4: Configure MCP Client](#step-4-configure-mcp-client)
  - [Step 5: Start Using](#step-5-start-using)
- [Batch Processing](#batch-processing)
- [Recognition Modes](#recognition-modes)
- [Output Structure](#output-structure)
- [CLI Usage](#cli-usage)
- [Post-Processing: Entry Splitting](#post-processing-entry-splitting)
- [MCP Tools Reference](#mcp-tools-reference)
- [Architecture](#architecture)
- [Performance Optimization](#performance-optimization)
- [Configuration Reference](#configuration-reference)
- [FAQ](#faq)
- [Development](#development)

---

## Quick Start

### Step 1: Prerequisites

#### Required

| Requirement | Minimum | Notes |
|-------------|:------:|------|
| **Python** | 3.11 | [Download](https://www.python.org/downloads/), check "Add to PATH" during install |
| **uv** | Latest | Python package manager. [Install guide](https://docs.astral.sh/uv/), restart terminal after |
| **Network** | — | First run downloads Chinese OCR models (~50-100 MB); works offline thereafter |

```bash
# Verify
python --version   # should be ≥ 3.11
uv --version       # should print a version number
```

#### Platform Support

| Platform | Status |
|------|------|
| **Windows 10/11** | ✅ Full support, CPU + NVIDIA GPU |
| **macOS** | ✅ CPU mode; GPU acceleration needs separate verification |
| **Linux** | ✅ CPU mode; GPU requires manual CUDA driver installation |

#### GPU Acceleration (Optional)

| Condition | Requirement |
|------|------|
| **GPU** | NVIDIA GTX 10 series or newer (compute capability ≥ 6.1), ✅ GTX 1060 6G verified |
| **Driver** | NVIDIA driver installed, `nvidia-smi` outputs normally |
| **CUDA** | 11.8 (auto-installed by `uv sync --extra ocr-gpu`, no manual setup) |
| **VRAM** | ≥ 4 GB recommended (GTX 1060 6G tested: `book-fast` 3-5 s/page); falls back to CPU if insufficient |

> The `ocr-gpu` extras automatically install CUDA 11.8, cuDNN 8.9, and all NVIDIA dependencies on Windows. No separate Visual C++ runtime installation needed.

#### Optional External Tools

The health check detects these tools; missing them does not affect core functionality:

| Tool | Purpose | Install |
|------|------|------|
| **Tesseract** | Fallback OCR engine (when PaddleOCR unavailable) | `winget install tesseract` or [GitHub](https://github.com/UB-Mannheim/tesseract/wiki) |
| **Ghostscript** | Low-level PDF rendering and repair | `winget install ghostscript` or [website](https://ghostscript.com/) |
| **OCRmyPDF** | Text layer repair | `pip install ocrmypdf` |
| **qpdf** | PDF structure inspection | `winget install qpdf` or [GitHub](https://github.com/qpdf/qpdf) |
| **poppler (pdftoppm)** | Alternative PDF to image conversion | `winget install poppler` or [poppler](https://github.com/oschwartz10612/poppler-windows) |

#### First Run

On the first OCR extraction, PaddleOCR automatically downloads the PP-OCRv6 Chinese recognition model from the model repository. Keep the network connected; once downloaded, all subsequent runs work offline. Models are cached in `~/.paddleocr/`.

### Step 2: Clone & Install

```bash
git clone https://github.com/albertm88/pdf-rescue-mcp.git
cd pdf-rescue-mcp
```

| Scenario | Command | Notes |
|----------|---------|-------|
| Daily use (CPU) | `uv sync --extra ocr` | All platforms |
| NVIDIA GPU | `uv sync --extra ocr-gpu` | CUDA 11.8, 3-5× speedup |

### Step 3: Health Check

```bash
uv run python -m pdf_rescue_mcp.cli 体检
```

Should display CPU cores, available memory, OCR engine status, etc.

### Step 4: Configure MCP Client

> Generate a local config with absolute paths so clients can locate the launch script.

```bash
uv run python scripts/generate_mcp_config.py --client vscode     --output .vscode/mcp.json
uv run python scripts/generate_mcp_config.py --client claude     --output ~/claude-mcp.json
uv run python scripts/generate_mcp_config.py --client cursor     --output ~/cursor-mcp.json
uv run python scripts/generate_mcp_config.py --client anythingllm --output ~/anythingllm-mcp.json
uv run python scripts/generate_mcp_config.py --client trae       --output .trae/mcp.json
uv run python scripts/generate_mcp_config.py --client codex      --output ~/codex-mcp.toml
```

> **Manual config**: Copy templates from `examples/` and replace `{{PROJECT_ROOT}}` with the absolute project path. VS Code users can use `examples/mcp-config.vscode.json` directly (`${workspaceFolder}` needs no replacement).

### Step 5: Start Using

In your MCP client, simply tell the AI:

> Extract text from `D:\scanned-books\some-book.pdf`

The AI will automatically call `rescue_pdf` to complete the full pipeline: **Diagnose → Plan → OCR → Audit**.

📁 Results saved to: `<PDF-sibling-dir>/pdf_rescue_output/<book-name>-rescue-result/`

---

## Batch Processing

For large collections of PDFs:

```bash
# 1. Scan library
uv run python -m pdf_rescue_mcp.cli 书库扫描 <library-dir>

# 2. Start batch extraction (background, resumable)
uv run python -B scripts/batch_extract_all.py
```

**Batch controller capabilities:**

- 🔍 Auto-discovers all PDFs in the library
- 📊 Dynamically allocates concurrency based on CPU, memory, and per-worker load
- 💾 Per-page caching, auto-resumes from breakpoint on restart
- 📈 Progress report every 30s (completed/in-progress/queued, pages, ETA, resources)

**Custom library path:** Edit `ROOT` and `OUTPUT` in `scripts/batch_extract_all.py`.

---

## Recognition Modes

| Mode | DPI | Use Case | Speed (CPU) | Speed (GTX 1060 6G) |
|------|-----|----------|:-----------:|:------------------:|
| `book-fast` | 180 | Quick preview, large batches | 8-30 s/page | 3-5 s/page |
| `book-balanced` | 220 | Daily use ⭐ default | 15-45 s/page | 5-10 s/page |
| `book-quality` | 300 | High-quality output | 30-90 s/page | 10-20 s/page |
| `book-forensic` | 300+ | Forensic, low-quality scans | 60-180 s/page | 20-40 s/page |

---

## Output Structure

```
<output-dir>/<book-name>-rescue-result/
├── 状态.json              # Real-time progress (pages, speed, ETA, engine)
├── 清单.yaml              # Processing manifest and config
├── 文本/
│   └── 全书.md            # Merged full text in Markdown
├── 数据/
│   ├── 页面.jsonl         # Per-page text + confidence + source
│   ├── 质量.json          # Quality report
│   ├── 低置信页.jsonl     # Low-confidence page details
│   └── 失败页.jsonl       # Failed page details
├── 缓存/
│   └── 页面OCR/           # Per-page OCR cache (for resume)
├── 审计/
│   └── 审计.html          # Visual quality audit report
└── 日志/                  # Subprocess runtime logs
```

---

## CLI Usage

Besides MCP, you can use the CLI directly:

```bash
# ── Diagnostics ──
uv run python -m pdf_rescue_mcp.cli 体检                    # Health check
uv run python -m pdf_rescue_mcp.cli 检查 <pdf-path>          # Inspect PDF type
uv run python -m pdf_rescue_mcp.cli 规划 <pdf-path>          # Plan processing route

# ── Extraction ──
uv run python -m pdf_rescue_mcp.cli 提取 <pdf-path>          # Extract single book
    --mode book-fast --output-dir <output-dir>

# ── Management ──
uv run python -m pdf_rescue_mcp.cli 状态 <task-dir>           # Query progress
uv run python -m pdf_rescue_mcp.cli 恢复 <task-dir>           # Resume interrupted task
uv run python -m pdf_rescue_mcp.cli 质检 <task-dir>           # Quality audit

# ── Batch ──
uv run python -m pdf_rescue_mcp.cli 书库扫描 <library-dir>     # Scan library
uv run python -m pdf_rescue_mcp.cli 书库提取 <library-dir>     # Batch extract
    --output-dir <output-dir> --mode book-fast
```

---

## Post-Processing: Entry Splitting

After OCR completes, split the full text into individual encyclopedia entry Markdown files:

```bash
uv run python scripts/split_into_entries_v2.py <rescue-result-dir> <final-output-dir>
```

Output structure:

```
<final-output-dir>/<book-name>/
├── 前言/
│   └── 前言与凡例.md
├── 条目/
│   ├── 鳖甲.md
│   ├── 冰硼散.md
│   └── ...
└── 索引.md
```

---

## MCP Tools Reference

### Core Tools

| Tool | Description |
|------|-------------|
| `rescue_pdf` | ⭐ **Primary entry point**: Auto diagnose → plan → extract → audit. Just pass the PDF path. |
| `extract_book_text` | Extract book text, runs in background subprocess, returns task directory immediately. |
| `get_job_status` | Query task progress (pages, speed, ETA, thread health). |
| `resume_job` | Resume interrupted task (breakpoint continuation). |
| `cancel_job` | Request a safe page-boundary stop. |
| `audit_job_quality` | Quality audit (low-confidence pages, failed pages, split heading detection). |
| `get_iteration_plan` | Produce a versioned quality/resource improvement plan; requires manual approval. |

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
| `share_processing_history` | Generate shareable history records (JSON / Markdown / HTML). |

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

### 1.0 Three-Layer Runtime

| Layer | Mechanism | Parameters |
|-------|-----------|------------|
| **Business** | Isolated OCR, per-page cache, atomic status/JSONL commits | progress, speed, ETA, quality evidence |
| **Supervision** | SQLite WAL task ledger, fencing lease, heartbeat + page-progress signals, `psutil` process-tree cleanup | `WATCH_INTERVAL=5s`, `HEARTBEAT_TIMEOUT=90s`, `PROGRESS_TIMEOUT=600s` |
| **Iteration** | Versioned plans from audit results and supervision events; manual approval required | `get_iteration_plan`, `strategy_version=1.0.0` |

### Cross-platform runtime state

Supervision state uses OS-standard directories (Windows `%APPDATA%`, macOS `~/Library`, Linux XDG). Override with:

```bash
# Linux / macOS
export PDF_RESCUE_RUNTIME_ROOT=/path/to/pdf-rescue-runtime

# Windows PowerShell
$env:PDF_RESCUE_RUNTIME_ROOT = "D:\pdf-rescue-runtime"
```

> ⚠️ Keep the SQLite task database on a local disk, not a network or sync share.

### Optional: Streamable HTTP mode

For MCP clients that cannot launch stdio subprocesses (loopback only):

```powershell
$env:PDF_RESCUE_MCP_TRANSPORT = "streamable-http"
$env:PDF_RESCUE_MCP_HOST = "127.0.0.1"
$env:PDF_RESCUE_MCP_PORT = "8765"
uv run --locked --extra ocr python -B scripts/start_mcp.py
```

---

## Performance Optimization

- Auto-detects CPU core count, reserves 2 cores for system
- **NVIDIA GPU**: Install `ocr-gpu` extras for CUDA acceleration (3-5× speedup)

| Tested Platform | Specs | Mode | Speed |
|------|------|------|:------:|
| AMD Ryzen 7 5800H | 8C/16T · CPU | `book-fast` | 8-15 s/page |
| NVIDIA GTX 1060 | Laptop 6 GB · GPU | `book-fast` | **3-5 s/page** |

---

## Configuration Reference

The following environment variables and parameters allow fine-grained control over resource allocation and supervision behavior. Set them in the terminal before launching — no source code changes needed.

### Worker Threads & Concurrency

| Env / Parameter | Default | Description |
|:---|:---:|:---|
| `PDF_RESCUE_OCR_THREADS` | Auto (1–4) | OCR threads per worker. Auto-calculated from physical cores, capped at 4 |
| `PDF_RESCUE_MAX_WORKERS` | Auto (≤4) | Max parallel workers. Auto-calculated from CPU cores + memory |

**Auto thread allocation:** Physical cores ≥ 8 → 4 threads; 6–7 → 3; 4–5 → 2; ≤3 → 1. Always reserves 2 logical cores for the system.

**Worker thread budget (per page count):** <80 pages → 1 thread; ≥80 → 2; ≥200 → 3; ≥400 → 4.

```bash
# Example: force 2 threads per worker, max 3 parallel workers
$env:PDF_RESCUE_OCR_THREADS = "2"
$env:PDF_RESCUE_MAX_WORKERS = "3"
```

### Memory Control

| Env / Parameter | Default | Description |
|:---|:---:|:---|
| `PDF_RESCUE_RESERVE_MEMORY_GB` | 2.0 GB | Memory reserved for OS; no new workers below this threshold |
| `PDF_RESCUE_MEMORY_PER_WORKER_GB` | 2.0 GB | Estimated memory per worker, used for slot calculation |

Memory slots = `(available - reserved) ÷ per_worker`. Combined with CPU core constraints, the minimum determines actual concurrency.

```bash
# Example: large RAM machine, 4 GB per worker, 4 GB reserved
$env:PDF_RESCUE_RESERVE_MEMORY_GB = "4"
$env:PDF_RESCUE_MEMORY_PER_WORKER_GB = "4"
```

### Supervision Timeouts

| Parameter | Default | Description |
|:---|:---:|:---|
| `WATCH_INTERVAL` | 5 s | Task watchdog polling interval |
| `HEARTBEAT_TIMEOUT` | 90 s | Worker heartbeat timeout: no heartbeat → considered lost |
| `PROGRESS_TIMEOUT` | 600 s | Progress timeout: alive but no page progress → considered stuck |
| `STARTUP_TIMEOUT` | 120 s | Startup timeout: wait for worker's first heartbeat |
| `CANCEL_GRACE` | 45 s | Cancel grace period: wait for graceful shutdown after cancel signal |
| `MAX_AUTO_RESTART` | 1 | Max auto-restart attempts after abnormal exit |

### Batch Controller

| Parameter | Default | Description |
|:---|:---:|:---|
| `PAGE_RATE_SAMPLE_WINDOW` | 300 s | Page-rate sampling sliding window |
| `PAGE_RATE_MAX_SAMPLES` | 12 | Max page-rate samples retained |
| `OBSERVER_TAKEOVER_INTERVAL` | 5 s | Passive observer takeover polling interval |
| `CONTROLLER_LEASE_SECONDS` | 45 s | Batch controller local exclusive lease TTL |
| `LEASE_SECONDS` | 45 s | Single-task MCP adapter lease TTL |

### Capacity Tuning Gates

| Parameter | Default | Description |
|:---|:---:|:---|
| System CPU safety ceiling | 92% | Candidates rejected if CPU exceeds this during trials |
| Throughput improvement threshold | 5% | Multi-worker must beat best single-worker baseline by this ratio |
| Quality regression tolerance | 3% | Allowed low-confidence page ratio regression for multi-worker |

### Quality Thresholds

| Parameter | Default | Description |
|:---|:---:|:---|
| `LOW_CONFIDENCE_THRESHOLD` | 0.9 | Confidence below this triggers quality warning |
| `LOW_CONFIDENCE_RETRY_DPI` | 300 | Auto-retry DPI for low-confidence pages |
| `LOW_CONFIDENCE_MIN_TEXT_RATIO` | 0.85 | Minimum text ratio for low-confidence retry |

### Runtime Directory

| Env Variable | Default | Description |
|:---|:---|:---|
| `PDF_RESCUE_RUNTIME_ROOT` | OS standard dir | Supervision layer runtime persistence root (SQLite ledger, etc.) |

> ⚠️ Timeout, window, and other parameters above are currently hardcoded constants. Tuning requires editing class attributes in `_TaskManager` within `src/pdf_rescue_mcp/server.py`. Environment variable overrides will be supported in a future release.

---

## FAQ

<details>
<summary><b>Q: Why is speed always 30-40 seconds per page?</b></summary>

This is normal for CPU mode. Try `book-fast` mode (DPI=180), or install NVIDIA GPU extras (GTX 1060 6G tested: `book-fast` only 3-5 s/page).
</details>

<details>
<summary><b>Q: How does breakpoint resume work?</b></summary>

After each page completes, OCR results are cached to `缓存/页面OCR/*.json`. On restart, cached pages are skipped automatically.
</details>

<details>
<summary><b>Q: How to handle password-protected PDFs?</b></summary>

Pass the `password` parameter when using `rescue_pdf`. Passwords are never written to record files.
</details>

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

---

## License

GPL-3.0-or-later · see [LICENSE](LICENSE)
