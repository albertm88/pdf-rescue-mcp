from __future__ import annotations

from typer.testing import CliRunner

from pdf_rescue_mcp.cli import app


def test_root_help_uses_chinese_labels() -> None:
    result = CliRunner().invoke(app, ["--帮助"])

    assert result.exit_code == 0
    assert "用法：" in result.stdout
    assert "选项：" in result.stdout
    assert "命令：" in result.stdout
    assert "Usage:" not in result.stdout
    assert "Options" not in result.stdout
    assert "Commands" not in result.stdout
    assert "知识库索引" not in result.stdout


def test_command_help_translates_framework_messages() -> None:
    result = CliRunner().invoke(app, ["提取", "--帮助"])

    assert result.exit_code == 0
    assert "[必填]" in result.stdout
    assert "[默认值：书籍均衡]" in result.stdout
    assert "显示此帮助并退出。" in result.stdout
    assert "[required]" not in result.stdout
    assert "[default:" not in result.stdout
