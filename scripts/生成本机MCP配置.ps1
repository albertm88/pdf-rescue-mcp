param(
    [Parameter(Mandatory = $true)]
    [string]$输出文件,

    [ValidateSet('auto', 'uv', 'python', 'py')]
    [string]$运行器 = 'auto'
)

$ErrorActionPreference = 'Stop'

if ($运行器 -eq 'auto') {
    if (Get-Command uv -ErrorAction SilentlyContinue) {
        $运行器 = 'uv'
    } elseif (Get-Command python -ErrorAction SilentlyContinue) {
        $运行器 = 'python'
    } elseif (Get-Command py -ErrorAction SilentlyContinue) {
        $运行器 = 'py'
    } else {
        throw '未找到 uv、python 或 py；请先安装运行环境。'
    }
}

switch ($运行器) {
    'uv' { $命令 = 'uv'; $参数 = @('run', '--locked', 'python', '-B', 'scripts/start_mcp.py') }
    'python' { $命令 = 'python'; $参数 = @('-B', 'scripts/start_mcp.py') }
    'py' { $命令 = 'py'; $参数 = @('-3', '-B', 'scripts/start_mcp.py') }
}

$配置 = @{
    mcpServers = @{
        中文PDF书籍救援 = @{
            command = $命令
            args = $参数
            cwd = '.'
            env = @{
                PYTHONUTF8 = '1'
                PYTHONIOENCODING = 'utf-8'
                PYTHONLEGACYWINDOWSSTDIO = '0'
            }
        }
    }
}

$目标 = [System.IO.Path]::GetFullPath($输出文件)
$目录 = Split-Path -Parent $目标
if ($目录) {
    New-Item -ItemType Directory -Force -Path $目录 | Out-Null
}
$配置 | ConvertTo-Json -Depth 6 | Set-Content -LiteralPath $目标 -Encoding utf8
Write-Output 'MCP 配置已生成。'

