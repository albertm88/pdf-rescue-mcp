param(
    [Parameter(Mandatory = $true)]
    [string]$任务目录,

    [Parameter(Mandatory = $true)]
    [string]$来源PDF,

    [int]$轮询秒数 = 120,
    [int]$最长等待秒数 = 21600,
    [int]$停滞秒数 = 900
)

$ErrorActionPreference = 'Stop'
$项目目录 = Split-Path -Parent $PSScriptRoot
$状态文件 = Join-Path $任务目录 '状态.json'
$日志目录 = Join-Path $项目目录 (Join-Path 'logs' (Get-Date -Format 'yyyy-MM-dd\HHmmss'))
$日志文件 = Join-Path $日志目录 '任务自动收尾.log'
$截止时间 = (Get-Date).AddSeconds($最长等待秒数)
$使用uv = $null -ne (Get-Command uv -ErrorAction SilentlyContinue)

New-Item -ItemType Directory -Force -Path $日志目录 | Out-Null
$env:PYTHONPATH = (Join-Path $项目目录 'src')

function 写日志([string]$内容) {
    "$(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') $内容" | Add-Content -LiteralPath $日志文件 -Encoding utf8
}

function 运行救援命令([string[]]$参数) {
    if ($使用uv) {
        & uv --directory $项目目录 run --locked python -B -m pdf_rescue_mcp.cli @参数 |
            Add-Content -LiteralPath $日志文件 -Encoding utf8
    } else {
        & python -B -m pdf_rescue_mcp.cli @参数 |
            Add-Content -LiteralPath $日志文件 -Encoding utf8
    }
    if ($LASTEXITCODE -ne 0) {
        throw "收尾命令执行失败，退出代码：$LASTEXITCODE"
    }
}

function 原OCR进程存在() {
    $候选进程 = Get-CimInstance Win32_Process -ErrorAction SilentlyContinue |
        Where-Object { $_.Name -in @('python.exe', 'uv.exe') }
    foreach ($进程 in $候选进程) {
        $命令行 = [string]$进程.CommandLine
        if ($命令行 -like '*pdf_rescue_mcp.cli*' -and
            $命令行.IndexOf($来源PDF, [System.StringComparison]::OrdinalIgnoreCase) -ge 0) {
            return $true
        }
    }
    return $false
}

写日志 '开始监测书籍任务。'

try {
    while ((Get-Date) -lt $截止时间) {
        if (-not (Test-Path -LiteralPath $状态文件)) {
            写日志 '尚未找到状态文件，继续等待。'
            Start-Sleep -Seconds $轮询秒数
            continue
        }

        $状态 = Get-Content -LiteralPath $状态文件 -Raw -Encoding utf8 | ConvertFrom-Json
        if ($状态.状态 -eq '完成') {
            写日志 '任务已完成，开始按最新规则刷新缓存。'
            运行救援命令 @('恢复', $任务目录, '--force', '--json')
            写日志 '缓存刷新完成，开始质量巡检。'
            运行救援命令 @('质量巡检', $任务目录, '--json')
            写日志 '自动收尾完成。'
            exit 0
        }
        if ($状态.状态 -in @('未完成', '需要OCR引擎', '需要提供密码', '需要先修复PDF')) {
            写日志 "任务状态为 $($状态.状态)，停止自动收尾。"
            exit 1
        }

        $更新时间 = $null
        try { $更新时间 = [datetime]::Parse([string]$状态.更新时间) } catch {}
        if ($null -eq $更新时间) {
            写日志 '状态更新时间无法解析，暂不自动恢复。'
        } elseif (((Get-Date) - $更新时间).TotalSeconds -ge $停滞秒数 -and -not (原OCR进程存在)) {
            写日志 "检测到任务已停滞超过 $停滞秒数 秒且原OCR进程不存在，开始自动恢复。"
            运行救援命令 @('恢复', $任务目录, '--json')
            写日志 '自动断点恢复命令已返回，继续检查最终状态。'
        } else {
            写日志 "任务仍在进行，当前已处理 $($状态.已处理页数) / $($状态.目标页数) 页。"
        }
        Start-Sleep -Seconds $轮询秒数
    }
    写日志 '等待超时，未执行自动收尾。'
    exit 2
} catch {
    写日志 "自动收尾失败：$($_.Exception.Message)"
    exit 3
}

