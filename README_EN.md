# Chinese PDF Book Rescue MCP

A local MCP service and CLI tool for Chinese book PDFs, supporting OCR text extraction from scanned PDFs, quality auditing, resume from breakpoints, and batch processing.

[中文](README.md) | English

## Features

- **One-Click Rescue**: Single `rescue_pdf` tool automates the full pipeline: diagnose → plan → extract → audit
- **Three-Layer Monitoring**: Business layer (subprocess isolation) + Monitoring layer (10s heartbeat) + Optimization layer (auto-restart on stall)
- **Batch Processing**: Background thread iterates books sequentially, MCP server stays responsive with real-time progress
- **Resume from Breakpoints**: Per-page caching, auto-resume from last checkpoint after interruption
- **Quality Auditing**: Low-confidence page detection, failed page logging, page evidence export
- **CPU/GPU Auto-Detection**: Automatically detects NVIDIA GPU acceleration; CPU mode with optimized thread count
- **Chinese Optimization**: Built-in term glossary, OCR post-processing, Chinese punctuation cleanup

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
                            │
┌───────────────────────────▼─────────────────────────────┐
│                      Output Files                        │
│  文本/全书.md · 数据/页面.jsonl · 数据/质量.json · 缓存/  │
└─────────────────────────────────────────────────────────┘
```

## Installation

### Requirements

- Python ≥ 3.11
- [uv](https://docs.astral.sh/uv/) package manager
- Windows 10/11 (primary tested platform) or Linux

### Setup

```bash
# Clone repository
git clone <repo-url>
cd pdf-rescue-mcp

# Install base dependencies + OCR extras
uv sync --extra ocr

# For NVIDIA GPU acceleration (CUDA 11.8)
uv sync --extra ocr-gpu
```

### VS Code MCP Configuration

Add to `.vscode/mcp.json`:

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

## MCP Tools Reference

### Core Tools

| Tool | Description |
|------|-------------|
| `rescue_pdf` | **Primary entry point**: Auto diagnose → plan → extract → audit. Just pass the PDF path. |
| `extract_book_text` | Extract book text, runs in background subprocess, returns task directory immediately. |
| `get_job_status` | Query task progress (pages, speed, ETA, thread health). |
| `resume_job` | Resume interrupted task (breakpoint continuation). |
| `audit_job_quality` | Quality audit (low-confidence pages, failed pages, split heading detection). |

### Batch Processing

| Tool | Description |
|------|-------------|
| `batch_extract_library` | Batch extract library, processes books sequentially in background, returns immediately. |
| `get_batch_status` | View batch progress (total books, completed, current book, ETA). |
| `stop_batch` | Stop batch (current book will finish, no new books started). |
| `scan_pdf_library` | Scan library directory, generate PDF manifest with recommended actions. |

### Diagnostics & Planning

| Tool | Description |
|------|-------------|
| `run_health_check` | Run health check (CPU/RAM/GPU/OCR dependencies). Fast mode skips model loading. |
| `inspect_pdf_text_layer` | Inspect PDF text layer (scanned/native/hybrid/encrypted). |
| `diagnose_pdf` | Diagnose PDF type, garbling risk, scanned page ratio. |
| `plan_pdf_job` | Plan processing route (mode, estimated time, engine selection). |

### Evidence & Glossary

| Tool | Description |
|------|-------------|
| `get_page_evidence` | View recognized text, confidence, and blocks for a specific page. |
| `export_page_image_evidence` | Export rendered page image for verification. |
| `get_term_glossary` | View term glossary (book-specific typo replacement rules). |
| `update_term_glossary` | Add a term replacement rule. |

### History

| Tool | Description |
|------|-------------|
| `get_processing_history` | View processing history (status, page counts, quality metrics). |
| `share_processing_history` | Generate shareable history records (JSON/Markdown/HTML). |

## CLI Usage

Besides the MCP service, you can use the CLI directly:

```bash
# Health check
uv run python -m pdf_rescue_mcp.cli 体检

# Inspect PDF
uv run python -m pdf_rescue_mcp.cli 检查 <pdf-path>

# Plan processing
uv run python -m pdf_rescue_mcp.cli 规划 <pdf-path>

# Extract text
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

## Recognition Modes

| Mode | DPI | Use Case | Speed (CPU) |
|------|-----|----------|-------------|
| `book-fast` | 180 | Quick preview, large batch processing | ~8-30s/page |
| `book-balanced` | 220 | Daily use (default) | ~15-45s/page |
| `book-quality` | 300 | High-quality output | ~30-90s/page |
| `book-forensic` | 300+ | Forensic level, low-quality scans | ~60-180s/page |

## Three-Layer Monitoring

| Layer | Mechanism | Parameters |
|-------|-----------|------------|
| **Business** | Subprocess isolation (`subprocess.Popen`), OCR crashes don't affect MCP server | - |
| **Monitor** | Watcher thread every 10s: subprocess liveness (`poll()`) + status file freshness (`mtime`) | `WATCH_INTERVAL=10s` |
| **Optimize** | Page timeout 180s → `terminate()` → downgrade to `book-fast` + restart once | `PAGE_TIMEOUT=180s`, `MAX_AUTO_RESTART=1` |

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

## Performance Optimization

### CPU Thread Configuration

- Auto-detects CPU core count, reserves 2 cores for system
- Thread count capped at physical core count (typically 8 threads) to avoid HyperThreading contention
- AMD Ryzen 7 5800H (8 cores/16 threads) tested: book-fast mode ~8-15s/page

### GPU Acceleration

- NVIDIA GPU: Install `ocr-gpu` extras for automatic CUDA acceleration (3-5x speedup)
- AMD GPU: PaddlePaddle doesn't support ROCm on Windows; Linux required
- MKLDNN/oneDNN: Disabled by default due to compatibility issues on AMD CPUs (`NotImplementedError`)

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

## FAQ

### Q: Why is speed always 30-40 seconds per page?
A: This is normal for CPU mode. Try:
- Use `book-fast` mode (DPI=180)
- Ensure no other CPU-intensive programs are running
- NVIDIA GPU users: install `ocr-gpu` extras for 3-5x speedup

### Q: What if subprocesses restart frequently?
A: Check error logs in `logs/`. Common causes:
- Out of memory: Close other programs or use `book-fast-low-memory` mode
- MKLDNN compatibility: Already disabled by default on AMD CPUs
- Corrupted PDF: Run `diagnose_pdf` first

### Q: How does resume from breakpoints work?
A: After each page completes, OCR results are cached to `缓存/页面OCR/*.json`. On restart, cached pages are skipped automatically.

### Q: How to handle password-protected PDFs?
A: Pass the `password` parameter when using `rescue_pdf`. Passwords are never written to record files.

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
