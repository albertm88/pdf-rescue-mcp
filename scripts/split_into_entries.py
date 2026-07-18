"""
中国农业百科全书后处理脚本：将 OCR 提取的全文按「书籍-分支学科-条目」结构拆分。

输入：book_pipeline 输出的 全书.md（按页排列）
输出：
    D:\农业百科全书-转文字\
    └── 中国农业百科全书-养蜂卷\
        ├── 前言.md
        ├── 条目分类目录.md
        ├── 养蜂业总论\
        │   ├── 养蜂业.md
        │   ├── 中国养蜂史.md
        │   └── ...
        ├── 蜜蜂品种及遗传育种\
        │   ├── 蜜蜂品种.md
        │   └── ...
        └── ...
"""
from __future__ import annotations

import re
import shutil
from pathlib import Path
from typing import Optional


def _safe_filename(name: str) -> str:
    """将标题转换为安全的文件名。"""
    name = re.sub(r'[\\/:*?"<>|]', '_', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name[:80]  # 限制长度


def _parse_pages_from_markdown(md_path: Path) -> dict[int, str]:
    """从全书.md解析出页码->文本的映射。"""
    text = md_path.read_text(encoding='utf-8')
    pages: dict[int, str] = {}
    current_page: Optional[int] = None
    current_lines: list[str] = []
    
    for line in text.splitlines():
        m = re.match(r'^## 第 (\d+) 页$', line)
        if m:
            if current_page is not None:
                pages[current_page] = '\n'.join(current_lines).strip()
            current_page = int(m.group(1))
            current_lines = []
        elif current_page is not None:
            current_lines.append(line)
    
    if current_page is not None:
        pages[current_page] = '\n'.join(current_lines).strip()
    
    return pages


def _extract_front_matter(pages: dict[int, str]) -> dict[str, str]:
    """提取前置内容（前言、凡例、目录等）。"""
    result = {}
    # 前 16 页左右是前置内容
    front_pages = []
    for p in sorted(pages.keys()):
        if p < 17:  # 正文通常从 17 页开始
            front_pages.append(f"## 第 {p} 页\n\n{pages[p]}")
    
    if front_pages:
        result["前言与编辑说明"] = '\n\n'.join(front_pages)
    return result


def _detect_entry_starts(pages: dict[int, str]) -> list[tuple[int, str]]:
    """
    检测百科条目起始位置。
    百科条目标题通常是：
    1. 独占一行或一行开头的黑体/大号字
    2. 后面跟着英译名（括号内）
    3. 条目名称通常是 2-10 个汉字
    
    返回列表：(页码, 条目标题)
    """
    entries = []
    # 条目标题模式：行首的中文短语，可能后跟英文翻译
    entry_pattern = re.compile(
        r'^([\u4e00-\u9fff][\u4e00-\u9fffA-Za-z0-9·（）()]{1,15})\s*$'
    )
    # 排除常见的非条目行
    exclude_patterns = [
        re.compile(r'^\d+$'),  # 纯数字（页码）
        re.compile(r'^第[一二三四五六七八九十百千]+[章节部卷编]'),  # 章节标记
        re.compile(r'^(前言|目录|附录|索引|凡例|版权|书名|彩图)'),
        re.compile(r'^[A-Z][a-z]+(\s[A-Z][a-z]+)*$'),  # 纯英文
    ]
    
    for page_num in sorted(pages.keys()):
        if page_num < 17:  # 跳过前置页
            continue
        text = pages[page_num]
        lines = text.splitlines()
        for i, line in enumerate(lines):
            line = line.strip()
            if not line:
                continue
            # 检查是否是条目标题
            m = entry_pattern.match(line)
            if m and not any(p.match(line) for p in exclude_patterns):
                title = m.group(1)
                # 简单启发式：标题前后通常有空行，或者标题后紧跟英文翻译
                # 检查下一行是否是英文翻译（确认是条目）
                is_entry = False
                if i + 1 < len(lines):
                    next_line = lines[i + 1].strip()
                    if next_line and re.match(r'^[A-Za-z]', next_line):
                        is_entry = True
                # 或者标题是已知的短名词（2-6字）
                if 2 <= len(title) <= 8 and not is_entry:
                    is_entry = True
                if is_entry:
                    entries.append((page_num, title))
    
    return entries


def split_book_to_entries(
    rescue_dir: Path,
    output_root: Path,
    book_title: str,
) -> Path:
    """
    将一本书的提取结果拆分为条目结构。
    
    Args:
        rescue_dir: rescue-result 目录路径
        output_root: 输出根目录（如 D:\农业百科全书-转文字）
        book_title: 书名（如 "中国农业百科全书-养蜂卷"）
    
    Returns:
        输出目录路径
    """
    md_path = rescue_dir / "文本" / "全书.md"
    if not md_path.exists():
        raise FileNotFoundError(f"找不到全书.md: {md_path}")
    
    # 创建输出目录
    book_dir = output_root / book_title
    book_dir.mkdir(parents=True, exist_ok=True)
    
    # 解析页面
    pages = _parse_pages_from_markdown(md_path)
    print(f"  解析到 {len(pages)} 页")
    
    # 提取前置内容
    front = _extract_front_matter(pages)
    for title, content in front.items():
        out_path = book_dir / f"{_safe_filename(title)}.md"
        out_path.write_text(f"# {title}\n\n{content}\n", encoding='utf-8')
        print(f"  写入: {out_path.name}")
    
    # 检测条目
    entries = _detect_entry_starts(pages)
    print(f"  检测到 {len(entries)} 个条目")
    
    # 按条目拆分页面内容
    if entries:
        # 创建条目目录（百科全书没有明确的分支学科时，统一放在"正文条目"目录）
        entries_dir = book_dir / "正文条目"
        entries_dir.mkdir(exist_ok=True)
        
        for i, (start_page, title) in enumerate(entries):
            # 确定条目结束页
            if i + 1 < len(entries):
                end_page = entries[i + 1][0] - 1
            else:
                end_page = max(pages.keys())
            
            # 收集这个条目的所有页面文本
            entry_pages = []
            for p in range(start_page, end_page + 1):
                if p in pages:
                    entry_pages.append(f"## 第 {p} 页\n\n{pages[p]}")
            
            if entry_pages:
                content = '\n\n'.join(entry_pages)
                out_path = entries_dir / f"{_safe_filename(title)}.md"
                # 处理重名
                counter = 1
                while out_path.exists():
                    out_path = entries_dir / f"{_safe_filename(title)}_{counter}.md"
                    counter += 1
                out_path.write_text(f"# {title}\n\n{content}\n", encoding='utf-8')
    
    return book_dir


def process_all_books(
    rescue_root: Path,
    output_root: Path,
) -> None:
    """批量处理所有已提取的书籍。"""
    output_root.mkdir(parents=True, exist_ok=True)
    
    # 查找所有 rescue-result 目录
    rescue_dirs = list(rescue_root.rglob("*-rescue-result"))
    print(f"找到 {len(rescue_dirs)} 个已提取书籍目录")
    
    for rescue_dir in rescue_dirs:
        book_name = rescue_dir.name.replace("-rescue-result", "")
        print(f"\n处理: {book_name}")
        try:
            split_book_to_entries(rescue_dir, output_root, book_name)
        except Exception as e:
            print(f"  错误: {e}")


if __name__ == "__main__":
    # 默认路径
    rescue_root = Path(r"D:\BaiduNetdiskDownload\dabao")
    output_root = Path(r"D:\农业百科全书-转文字")
    
    import sys
    if len(sys.argv) >= 2:
        rescue_root = Path(sys.argv[1])
    if len(sys.argv) >= 3:
        output_root = Path(sys.argv[2])
    
    process_all_books(rescue_root, output_root)
