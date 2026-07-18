"""
将《中国农业百科全书》OCR输出的全书.md拆分为独立条目文件。

条目格式特征：
1. 条目标题：独立成行的中文名词，可带英文 (如 鳖甲(carapax trionycis))
2. 条目正文：紧接标题后的段落
3. 作者署名：以 (作者名) 结尾
4. 条目间无空行分隔

输出结构：
  D:\农业百科全书-整理\
    └── 中兽医卷/
        ├── 前言/
        │   ├── 凡例.md
        │   └── 序言.md
        ├── 条目/
        │   ├── 鳖甲.md
        │   ├── 冰硼散.md
        │   └── ...
        └── 索引.md
"""

from __future__ import annotations

import json
import re
from pathlib import Path


# 条目标题模式：中文开头，可带英文括号
ENTRY_TITLE_RE = re.compile(
    r'^([\u4e00-\u9fff][\u4e00-\u9fff\w\s]{1,30}?)(?:\(([a-zA-Z][a-zA-Z\s\.,;_\-]+)\))?\s*$'
)

# 作者署名模式：(中文姓名) 结尾
AUTHOR_RE = re.compile(r'^[（(]([\u4e00-\u9fff·]{2,6})[）)]\s*$')

# 页码标记
PAGE_MARKER_RE = re.compile(r'^## 第 \d+ 页\s*$')

# 前言/目录页关键词
FRONT_MATTER_KEYWORDS = [
    '中国农业百科全书', '编辑出版领导小组', '总编辑委员会',
    '编辑委员会', '编写组主编', '前言', '凡例', '目录',
    '索引', '笔画索引', '外文索引', '内容索引', '条目分类目录',
]


def is_entry_title(line: str) -> tuple[bool, str, str | None]:
    """判断一行是否是条目标题，返回 (是否标题, 标题, 英文名)。"""
    line = line.strip()
    if not line:
        return False, "", None
    
    # 排除太长的行（正文段落）
    if len(line) > 40:
        return False, "", None
    
    # 排除以标点开头/结尾的行
    if line[0] in '，。、；：？！""''（）【】《》…—·' or line[-1] in '，。、；：？！""''（）【】《》…—':
        return False, "", None
    
    # 排除纯数字/页码
    if re.match(r'^[\d\s·\-\.]+$', line):
        return False, "", None
    
    # 排除含多个连续标点的行
    if re.search(r'[，。、；：？！]{2,}', line):
        return False, "", None
    
    # 排除前言/目录关键词
    for kw in FRONT_MATTER_KEYWORDS:
        if kw in line and len(line) < 20:
            return False, "", None
    
    # 匹配标题模式
    m = ENTRY_TITLE_RE.match(line)
    if m:
        title = m.group(1).strip()
        english = m.group(2).strip() if m.group(2) else None
        # 标题至少2个中文字
        if len(title) >= 2 and any('\u4e00' <= c <= '\u9fff' for c in title):
            return True, title, english
    
    return False, "", None


def is_author_line(line: str) -> bool:
    """判断一行是否是作者署名。"""
    return bool(AUTHOR_RE.match(line.strip()))


def clean_text(text: str) -> str:
    """清理OCR文本中的噪声。"""
    lines = text.splitlines()
    cleaned = []
    for line in lines:
        line = line.rstrip()
        if line or (cleaned and cleaned[-1]):
            cleaned.append(line)
    while cleaned and not cleaned[0]:
        cleaned.pop(0)
    while cleaned and not cleaned[-1]:
        cleaned.pop()
    return '\n'.join(cleaned)


def split_entries(quan_shu_md: Path, output_dir: Path) -> dict:
    """将全书.md拆分为条目文件。"""
    content = quan_shu_md.read_text(encoding='utf-8')
    lines = content.splitlines()
    
    entries = []
    current_title = None
    current_english = None
    current_body_lines: list[str] = []
    current_start_page = 0
    current_page = 0
    
    front_matter_lines: list[str] = []
    in_front_matter = True
    
    for line in lines:
        stripped = line.strip()
        
        # 检测页码
        page_m = re.match(r'^## 第 (\d+) 页', stripped)
        if page_m:
            current_page = int(page_m.group(1))
            if current_title:
                current_body_lines.append('')
            continue
        
        if not stripped:
            if current_title:
                current_body_lines.append('')
            elif in_front_matter:
                front_matter_lines.append('')
            continue
        
        # 检测作者署名（条目结束标志）
        if current_title and is_author_line(stripped):
            author = AUTHOR_RE.match(stripped).group(1)
            current_body_lines.append(f"\n> 作者：{author}")
            entries.append({
                'title': current_title,
                'english': current_english,
                'body': clean_text('\n'.join(current_body_lines)),
                'start_page': current_start_page,
                'end_page': current_page,
                'author': author,
            })
            current_title = None
            current_english = None
            current_body_lines = []
            in_front_matter = False
            continue
        
        # 检测新条目标题
        is_title, title, english = is_entry_title(stripped)
        if is_title and not current_title:
            # 保存前言
            if in_front_matter and front_matter_lines:
                front_text = clean_text('\n'.join(front_matter_lines))
                if len(front_text) > 100:
                    entries.append({
                        'title': '前言与凡例',
                        'english': None,
                        'body': front_text,
                        'start_page': 1,
                        'end_page': current_page,
                        'author': None,
                        'is_front_matter': True,
                    })
                front_matter_lines = []
                in_front_matter = False
            
            # 保存未完成的条目
            if current_title and current_body_lines:
                entries.append({
                    'title': current_title,
                    'english': current_english,
                    'body': clean_text('\n'.join(current_body_lines)),
                    'start_page': current_start_page,
                    'end_page': current_page,
                    'author': None,
                })
            
            current_title = title
            current_english = english
            current_body_lines = []
            current_start_page = current_page
            continue
        
        if current_title:
            current_body_lines.append(line)
        elif in_front_matter:
            front_matter_lines.append(line)
    
    # 处理最后一个条目
    if current_title and current_body_lines:
        entries.append({
            'title': current_title,
            'english': current_english,
            'body': clean_text('\n'.join(current_body_lines)),
            'start_page': current_start_page,
            'end_page': current_page,
            'author': None,
        })
    
    # 写入文件
    entries_dir = output_dir / '条目'
    entries_dir.mkdir(parents=True, exist_ok=True)
    
    index_lines = ['# 条目索引\n']
    entry_count = 0
    
    for entry in entries:
        if entry.get('is_front_matter'):
            front_dir = output_dir / '前言'
            front_dir.mkdir(parents=True, exist_ok=True)
            (front_dir / f"{entry['title']}.md").write_text(
                f"# {entry['title']}\n\n{entry['body']}\n",
                encoding='utf-8'
            )
            continue
        
        title = entry['title']
        safe_title = re.sub(r'[\\/:*?"<>|]', '_', title)
        entry_file = entries_dir / f"{safe_title}.md"
        
        if entry_file.exists():
            entry_file = entries_dir / f"{safe_title}_{entry['start_page']}.md"
        
        header = f"# {title}"
        if entry['english']:
            header += f" ({entry['english']})"
        header += f"\n\n> 页码：{entry['start_page']}-{entry['end_page']}"
        if entry['author']:
            header += f" | 作者：{entry['author']}"
        header += "\n\n---\n\n"
        
        entry_file.write_text(header + entry['body'] + '\n', encoding='utf-8')
        entry_count += 1
        index_lines.append(f"- [{title}](条目/{safe_title}.md) (p.{entry['start_page']})")
    
    (output_dir / '索引.md').write_text('\n'.join(index_lines) + '\n', encoding='utf-8')
    
    return {
        '总条目数': entry_count,
        '输出目录': str(output_dir),
        '条目目录': str(entries_dir),
    }


def process_book(rescue_result_dir: Path, final_output_dir: Path) -> dict:
    """处理单本书的OCR输出，拆分为条目。"""
    quan_shu = rescue_result_dir / '文本' / '全书.md'
    if not quan_shu.exists():
        return {'错误': f'未找到全书.md: {quan_shu}'}
    
    book_name = rescue_result_dir.name.replace('-rescue-result', '')
    book_output = final_output_dir / book_name
    book_output.mkdir(parents=True, exist_ok=True)
    
    result = split_entries(quan_shu, book_output)
    result['书名'] = book_name
    return result


if __name__ == '__main__':
    import sys
    
    if len(sys.argv) < 3:
        print("用法: python split_into_entries.py <rescue_result_dir> <final_output_dir>")
        sys.exit(1)
    
    rescue_dir = Path(sys.argv[1])
    output_dir = Path(sys.argv[2])
    
    result = process_book(rescue_dir, output_dir)
    print(json.dumps(result, ensure_ascii=False, indent=2))
