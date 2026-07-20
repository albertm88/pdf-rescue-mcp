from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path

import pytest

import pdf_rescue_mcp.book_pipeline as book_pipeline

from pdf_rescue_mcp.book_pipeline import (
    audit_job_quality,
    add_term_glossary_replacement,
    export_page_image_evidence,
    get_page_evidence,
    get_term_glossary,
    read_job_status,
    resume_job,
    _ocr_throughput_metrics,
    _extract_ocr_pages_resumable,
    _chunk_page_text,
    _is_diagram_like_page,
    _is_illustration_mixed_page,
    _is_dense_index_page,
    _load_cached_page,
    _prepare_ocr_page,
    _retry_low_confidence_page,
    _write_cached_page,
    _write_status,
)
from pdf_rescue_mcp.models import PageRecord, PdfType, RecommendedAction, TextLayerInspection
from pdf_rescue_mcp.task_store import TaskStore


class FakeAdapter:
    pass


def test_partial_resume_keeps_cached_progress_when_cancelled(tmp_path: Path, monkeypatch) -> None:
    cache_dir = tmp_path / "缓存"
    status_path = tmp_path / "状态.json"
    for page_number in (1, 2):
        _write_cached_page(
            cache_dir,
            PageRecord(
                page_number=page_number,
                text=f"已缓存第 {page_number} 页",
                confidence=0.99,
                source="paddleocr",
            ),
        )

    class StopAtFirstBoundary:
        calls = 0

        def is_set(self) -> bool:
            self.calls += 1
            return self.calls >= 2

    monkeypatch.setattr(
        "pdf_rescue_mcp.book_pipeline.create_ocr_adapter",
        lambda model_size="small": ("paddleocr", FakeAdapter()),
    )

    with pytest.raises(book_pipeline._CancellationRequested):
        _extract_ocr_pages_resumable(
            Path("书.pdf"),
            cache_dir,
            status_path,
            target_pages=3,
            resume=True,
            dpi=180,
            stop_flag=StopAtFirstBoundary(),
        )

    status = json.loads(status_path.read_text(encoding="utf-8"))
    assert status["状态"] == "已取消"
    assert status["已处理页数"] == 2


def test_public_cancelled_resume_rebuilds_metrics_from_page_cache(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_pdf = tmp_path / "book.pdf"
    source_pdf.write_bytes(b"%PDF-1.4\n%%EOF\n")
    output_dir = tmp_path / "输出"
    job_dir = output_dir / "book-rescue-result"
    cache_dir = job_dir / "缓存" / "页面OCR"
    for page_number in (1, 2):
        _write_cached_page(
            cache_dir,
            PageRecord(
                page_number=page_number,
                text=f"已缓存第 {page_number} 页",
                confidence=0.99,
                source="paddleocr",
            ),
        )

    inspection = TextLayerInspection(
        path=str(source_pdf),
        page_count=3,
        inspected_pages=3,
        pdf_type=PdfType.IMAGE_ONLY_SCANNED,
        has_extractable_text=False,
        has_outline=False,
        has_toc_like_pages=False,
        text_layer_quality=0.0,
        garble_risk=0.0,
        coverage_score=0.0,
        scanned_page_ratio=1.0,
        text_page_ratio=0.0,
        recommended_action=RecommendedAction.FULL_BOOK_OCR,
        pages=[],
    )

    class AlreadyStopped:
        def is_set(self) -> bool:
            return True

    monkeypatch.setattr("pdf_rescue_mcp.book_pipeline.inspect_pdf_text_layer", lambda *_args, **_kwargs: inspection)
    monkeypatch.setattr("pdf_rescue_mcp.book_pipeline.available_ocr_engine", lambda: "paddleocr")

    result = book_pipeline.extract_book_text(
        source_pdf,
        output_dir=output_dir,
        mode="book-fast",
        resume=True,
        stop_flag=AlreadyStopped(),
    )

    status = json.loads((job_dir / "状态.json").read_text(encoding="utf-8"))
    assert result.status == "cancelled"
    assert status["状态"] == "已取消"
    assert status["已处理页数"] == 2


def test_partial_resume_starts_from_cache_and_ocrs_only_missing_page(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cache_dir = tmp_path / "缓存"
    status_path = tmp_path / "状态.json"
    for page_number in (1, 2):
        _write_cached_page(
            cache_dir,
            PageRecord(
                page_number=page_number,
                text=f"已缓存第 {page_number} 页",
                confidence=0.99,
                source="paddleocr",
            ),
        )

    calls: list[int] = []
    progress_counts: list[int] = []

    def fake_ocr_page(
        _pdf_path: Path,
        page_number: int,
        *,
        adapter: FakeAdapter,
        engine_name: str,
        dpi: int,
        password=None,
    ) -> tuple[str, PageRecord]:
        calls.append(page_number)
        return engine_name, PageRecord(
            page_number=page_number,
            text="新识别页面",
            confidence=0.99,
            source=engine_name,
        )

    monkeypatch.setattr(
        "pdf_rescue_mcp.book_pipeline.create_ocr_adapter",
        lambda model_size="small": ("paddleocr", FakeAdapter()),
    )
    monkeypatch.setattr("pdf_rescue_mcp.book_pipeline.ocr_pdf_page", fake_ocr_page)

    _engine, pages, _failures, _low = _extract_ocr_pages_resumable(
        Path("书.pdf"),
        cache_dir,
        status_path,
        target_pages=3,
        resume=True,
        dpi=180,
        progress_callback=lambda processed, _total, _pct, _message: progress_counts.append(processed),
    )

    status = json.loads(status_path.read_text(encoding="utf-8"))
    assert calls == [3]
    assert [page.page_number for page in pages] == [1, 2, 3]
    assert progress_counts[0] == 2
    assert status["已处理页数"] == 3


def test_ocr_throughput_window_uses_real_ocr_duration_samples_only() -> None:
    metrics = _ocr_throughput_metrics(
        [float(value) for value in range(1, 20)],
        thread_budget=6,
        warmup_pages=2,
    )

    # The rolling window contains the last 12 calls (8..19), not any coarse
    # whole-job average or cache/native-text timing supplied by a caller.
    assert metrics["OCR线程预算"] == 6
    assert metrics["OCR性能预热页数"] == 2
    assert metrics["OCR吞吐样本数"] == 12
    assert metrics["短窗OCR中位秒每页"] == 13.5
    assert metrics["最近OCR页秒数"] == 19.0
    assert metrics["短窗OCR页每分钟"] == round(60.0 / 13.5, 3)


def test_term_glossary_is_readable_for_agents() -> None:
    glossary = get_term_glossary()

    assert glossary["词表路径"].endswith("术语词表.yaml")
    assert any(rule["名称"] == "茶业卷常见错字" for rule in glossary["规则"])


def test_term_glossary_reloads_saved_updates(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    glossary_path = tmp_path / "术语词表.yaml"
    glossary_path.write_text("规则: []\n", encoding="utf-8")
    monkeypatch.setattr(book_pipeline, "TERM_GLOSSARY_PATH", glossary_path)

    assert book_pipeline.get_term_glossary()["规则"] == []

    glossary_path.write_text(
        "规则:\n  - 名称: 临时规则\n    书名包含: [茶]\n    替换: {错字: 正字}\n",
        encoding="utf-8",
    )

    assert book_pipeline.get_term_glossary()["规则"][0]["名称"] == "临时规则"


def test_invalid_term_glossary_is_reported_without_breaking_page_cleanup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    glossary_path = tmp_path / "术语词表.yaml"
    glossary_path.write_text("规则: [\n", encoding="utf-8")
    monkeypatch.setattr(book_pipeline, "TERM_GLOSSARY_PATH", glossary_path)
    page = PageRecord(page_number=8, text="茶\n业\n正文", confidence=0.98, source="paddleocr")

    _prepare_ocr_page(page, book_title="中国农业百科全书：茶业卷")

    assert page.text == "茶业\n正文"
    assert any("术语词表格式错误" in warning for warning in page.warnings)
    assert book_pipeline.get_term_glossary()["错误"] is not None


def test_map_caption_removes_isolated_scale_labels() -> None:
    page = PageRecord(
        page_number=1,
        text="\n".join(
            [
                "图3 世界茶区示意图",
                "60",
                "30",
                "0",
                "30",
                "60",
                "印度茶叶产量占世界总产量较大。",
                *[f"正文内容第{index}行，保留地图说明文字。" for index in range(20)],
            ]
        ),
        confidence=0.96,
        source="paddleocr",
    )

    _prepare_ocr_page(page, book_title="中国农业百科全书：茶业卷")

    assert "\n60\n" not in f"\n{page.text}\n"
    assert "印度茶叶产量占世界总产量较大" in page.text
    assert any("图表页孤立噪声" in warning for warning in page.warnings)


def test_agents_can_add_book_limited_glossary_replacements(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    glossary_path = tmp_path / "术语词表.yaml"
    glossary_path.write_text("规则: []\n", encoding="utf-8")
    monkeypatch.setattr(book_pipeline, "TERM_GLOSSARY_PATH", glossary_path)

    result = add_term_glossary_replacement("测试规则", ["测试书"], "错字", "正字")

    assert result["原正字"] is None
    glossary = book_pipeline.get_term_glossary()
    assert glossary["规则"][0]["替换"] == {"错字": "正字"}
    assert not glossary_path.with_suffix(".tmp").exists()
    with pytest.raises(ValueError, match="书名包含条件不能为空"):
        add_term_glossary_replacement("测试规则", [], "错字", "正字")


def test_chunk_page_text_keeps_clearly_labelled_entries_separate() -> None:
    page = PageRecord(
        page_number=119,
        text=(
            "上一词条的续文。\n"
            "惠利蚕(eri-silkworm)蓖麻蚕的音译名。\n"
            "惠利蚕正文。\n"
            "混合育(mixed batches rearing)育种饲养方式。\n"
            "混合育正文。"
        ),
        confidence=0.96,
        source="paddleocr",
    )

    chunks = _chunk_page_text(page, Path("蚕业卷.pdf"), "蚕业卷", max_chars=200)

    assert [chunk.chapter for chunk in chunks] == [None, "惠利蚕", "混合育"]
    assert "惠利蚕正文" in chunks[1].text
    assert "混合育" not in chunks[1].text
    assert "混合育正文" in chunks[2].text


def test_tea_volume_merges_split_entry_title() -> None:
    page = PageRecord(
        page_number=8,
        text=(
            "茶\n业\n王泽农\n研究，制订茶叶审评和检验方法。\n"
            "“三改技术。推厂和普及茶叶生产。中国广广州。\n"
            "栄西禅师。单产0).518吨。茶村修剪机。百页链板式烘干机。\n"
            "杀膏。其加I过程。般每盅。200)~400毫升。茶从枝丫。头、胸部及腹基部。\n"
            "20)°或15°。梯壁侧坡叮在。则量山坡坡度。士壤肥力。夏季干早。懈皮素和解皮苷。"
        ),
        confidence=0.98,
        source="paddleocr",
    )

    _prepare_ocr_page(page, book_title="中国农业百科全书：茶业卷")

    assert page.text.startswith("茶业\n王泽农")
    assert "研究、制订茶叶审评和检验方法" in page.text
    assert "“三改”技术。推广和普及茶叶生产。中国广州" in page.text
    assert "荣西禅师。单产0.518吨。茶树修剪机。百叶链板式烘干机" in page.text
    assert "杀青。其加工过程。一般每盅。200~400毫升。茶丛枝丫。头、胸部及腹部基部" in page.text
    assert "20°或15°。梯壁侧坡可在。测量山坡坡度。土壤肥力。夏季干旱。槲皮素和槲皮苷" in page.text
    assert any("茶业条题" in warning for warning in page.warnings)
    assert any("茶业卷常见错字" in warning for warning in page.warnings)


def test_sericulture_term_corrections_cover_sampled_book_pages() -> None:
    page = PageRecord(
        page_number=151,
        text=(
            "过人卵首先足根据洗清盐味，阴天叮用去湿器，进步除去杂物。\n"
            "1919年中华人民共和国建立后，中国1大蚕区疗桑树生长。\n"
            "头雌虫产卵数，卵期夏季4~7人，毕即死上介壳下。\n"
            "熟蛋吐丝结茧，阻止熟蚕叶丝孔。\n"
            "杂交第2代(F2)群体内由F基因分离。"
        ),
        confidence=0.96,
        source="paddleocr",
    )

    _prepare_ocr_page(page, book_title="中国农业百科全书：蚕业卷")

    assert "过大卵首先是根据洗净盐味，阴天可用去湿器，进一步除去杂物" in page.text
    assert "1949年中华人民共和国建立后，中国三大蚕区有桑树生长" in page.text
    assert "每头雌虫产卵数，卵期夏季4~7天，毕即死于介壳下" in page.text
    assert "熟蚕吐丝结茧，阻止熟蚕吐丝孔" in page.text
    assert "杂交第2代(F2)群体内由于基因分离" in page.text


def test_sericulture_term_corrections_cover_later_subjects() -> None:
    page = PageRecord(
        page_number=160,
        text=(
            "外周坤经近本壁，幼主神经系统的额炜经节与心则体相连。\n"
            "味觉器宫表现止趋光性。siikworm toxicosis由工！排放引起。\n"
            "泥坏含氟350~450pp，拌随中毒，污染桑经蚕吃下。\n"
            "垂桑研究所达到城高滴定浓度，胆伯醇来自咽侧休系白色球形小体。\n"
            "滞台激素由咽下神经古分泌，蛋卵滞育与微粒子病有关。\n"
            "随发台而渐增，木发现基囚控制，桑蚕眠性(moltinismn)。"
        ),
        confidence=0.96,
        source="paddleocr",
    )

    _prepare_ocr_page(page, book_title="中国农业百科全书：蚕业卷")

    assert "外周神经近体壁，幼虫神经系统的额神经节与心侧体相连" in page.text
    assert "味觉器官表现正趋光性。silkworm toxicosis由工业排放引起" in page.text
    assert "泥坯含氟350~450ppm，伴随中毒，污染桑叶经蚕吃下" in page.text
    assert "蚕桑研究所达到最高滴定浓度，胆甾醇来自咽侧体是白色球形小体" in page.text
    assert "滞育激素由咽下神经节分泌，蚕卵滞育与微孢子病有关" in page.text
    assert "随发育而渐增，未发现基因控制，桑蚕眠性(moltinism)" in page.text


def test_sericulture_term_corrections_cover_artificial_feed_and_silk_glands() -> None:
    page = PageRecord(
        page_number=166,
        text=(
            "桑叶，于50℃左右温度下尽快风于。脱酯大豆粉保存在低温、千燥和黑暗的场所。\n"
            "加入β-谷甾醇的乙酵，调制时务需注意。\n"
            "桑蚕数量性状遗传 inheritance of quantitative characters。\n"
            "遗传相关和表型相关的F\n负符号相反，一定有较人的环境相关。\n"
            "较大的止表型相关见于5龄起蚕重与全虽重，管径0).3毫米，内腹因丝腺。"
        ),
        confidence=0.96,
        source="paddleocr",
    )

    _prepare_ocr_page(page, book_title="中国农业百科全书：蚕业卷")

    assert "尽快风干。脱脂大豆粉保存在低温、干燥和黑暗的场所" in page.text
    assert "加入β-谷甾醇的乙醚，调制时务必注意" in page.text
    assert "桑蚕数量性状遗传 (inheritance of quantitative characters" in page.text
    assert "遗传相关和表型相关的正负符号相反，一定有较大的环境相关" in page.text
    assert "较大的正表型相关见于5龄起蚕重与全茧重，管径0.3毫米，内膜因丝腺" in page.text


def test_sericulture_term_corrections_cover_anatomy_and_sex_limited_traits() -> None:
    page = PageRecord(
        page_number=172,
        text=(
            "大蚕期成树技状。围心细胞白色扁乎而形状不规则，第8日前后行直接分裂。\n"
            "桑蚕限性遗传(sex-limited\ntance of mulberry silkworm)。\n"
            "桑蚕形态(mulberry silkworm morphlogy)。\n"
            "胸足主要在食桑和叶丝时使用，雌蚕较雄蚕人，腹面各有对乳白色。\n"
            "前-对称前生殖芽，这些生殖芽在人蚕期肉眼可见，赫氏宝。"
        ),
        confidence=0.96,
        source="paddleocr",
    )

    _prepare_ocr_page(page, book_title="中国农业百科全书：蚕业卷")

    assert "大蚕期成树枝状。围心细胞白色扁平而形状不规则，第8日前后进行直接分裂" in page.text
    assert "桑蚕限性遗传(sex-limited\ninheritance of mulberry silkworm)" in page.text
    assert "桑蚕形态(mulberry silkworm morphology)" in page.text
    assert "胸足主要在食桑和吐丝时使用，雌蚕较雄蚕大，腹面各有一对乳白色" in page.text
    assert "前一对称前生殖芽，这些生殖芽在大蚕期肉眼可见，赫氏腺" in page.text


def test_sericulture_term_corrections_cover_pests_and_diseases() -> None:
    page = PageRecord(
        page_number=180,
        text=(
            "斑纹限性、虽色限性。桑尺蠖(mulberrygeometrid)，学名Phtho\nnandria atrilineata。\n"
            "日中倚枝斜立。桑赤锈病的病原菌为桑锈抱锈菌，锈抱子抗寒力弱。\n"
            "与枝迹十分接近，温度高十30℃时发病抑制。"
        ),
        confidence=0.96,
        source="paddleocr",
    )

    _prepare_ocr_page(page, book_title="中国农业百科全书：蚕业卷")

    assert "斑纹限性、茧色限性。桑尺蠖(mulberry geometrid)，学名Phthonandria atrilineata" in page.text
    assert "日间倚枝斜立。桑赤锈病的病原菌为桑锈孢锈菌，锈孢子抗寒力弱" in page.text
    assert "与枝痕十分接近，温度高于30℃时发病抑制" in page.text


def test_sericulture_term_corrections_cover_mulberry_nursery_work() -> None:
    page = PageRecord(
        page_number=190,
        text=(
            "毒毛鳌伤后重度蟹伤，成落小蚕死亡。上壤含水量为最人持水量。\n"
            "不见桑了，出士，春播后除卓，防除病虫害。另一株桑树的枝于或根上。\n"
            "移裁苗池，接博支结孔，术质部分离，用细士覆盖。\n"
            "芽六正反面，硝木切弧形，简易萨接法，带-个芽。\n"
            "春期嫁接需20大以上，夏秋嫁接约15大左右，打插生根的坡适温度。\n"
            "粘性上容易生根，硬技扦插后置于屋边砂七中，经过25天左石。"
        ),
        confidence=0.96,
        source="paddleocr",
    )

    _prepare_ocr_page(page, book_title="中国农业百科全书：蚕业卷")

    assert "毒毛螯伤后重度螯伤，成批小蚕死亡。土壤含水量为最大持水量" in page.text
    assert "不见桑籽，出土，春播后除草，防除病虫害。另一株桑树的枝干或根上" in page.text
    assert "移栽苗地，接穗枝结缚，木质部分离，用细土覆盖" in page.text
    assert "芽片正反面，砧木切弧形，简易芽接法，带一个芽" in page.text
    assert "春期嫁接需20天以上，夏秋嫁接约15天左右，扦插生根的最适温度" in page.text
    assert "粘性土容易生根，硬枝扦插后置于屋边砂土中，经过25天左右" in page.text


def test_sericulture_term_corrections_cover_mulberry_breeding_and_weather() -> None:
    page = PageRecord(
        page_number=200,
        text=(
            "人上诱导多倍体，常用的足化学药剂-秋水仙碱处理法，易溶于洒精，堆溶于乙醚。\n"
            "忙藏时应避光，桑种子没种催芽，幼粮短而肥大，经过段时间后使用秋水仙碱济液。\n"
            "桑树台种方法符合选种月标，作为引变处理材料，剂量10~11于伦琴。\n"
            "30多乍来，特别足对一年代数多的害虫使用敌政畏乳油。\n"
            "树干由主于和支于组成，夏伐后的发条能力强，中柱内尤明显的中柱鞘。\n"
            "使灾害减低，犬气晴朗之日增施--次速效性肥料。"
        ),
        confidence=0.96,
        source="paddleocr",
    )

    _prepare_ocr_page(page, book_title="中国农业百科全书：蚕业卷")

    assert "人工诱导多倍体，常用的是化学药剂秋水仙碱处理法，易溶于酒精，微溶于乙醚" in page.text
    assert "贮藏时应避光，桑种子浸种催芽，幼苗短而肥大，经过一段时间后使用秋水仙碱溶液" in page.text
    assert "桑树育种方法符合选种目标，作为诱变处理材料，剂量10~11千伦琴" in page.text
    assert "30多年来，特别是对一年发生代数多的害虫使用敌敌畏乳油" in page.text
    assert "树干由主干和支干组成，夏伐后的发芽能力强，中柱内无明显的中柱鞘" in page.text
    assert "使灾害降低，天气晴朗之日增施一次速效性肥料" in page.text


def test_status_includes_runtime_estimates(tmp_path: Path) -> None:
    status_path = tmp_path / "状态.json"
    started_at = datetime.now() - timedelta(seconds=40)

    _write_status(
        status_path,
        source_pdf=Path("书.pdf"),
        status="进行中",
        target_pages=10,
        processed_pages=2,
        text_pages=2,
        blank_pages=0,
        failed_pages=0,
        low_confidence_pages=0,
        engine="paddleocr",
        total_pages=10,
        is_sample=False,
        started_at=started_at,
        mode="book-quality",
    )

    status = json.loads(status_path.read_text(encoding="utf-8"))
    assert status["开始时间"] == started_at.isoformat(timespec="seconds")
    assert status["已耗时秒"] >= 40
    assert status["平均每页秒"] >= 20
    assert status["预计剩余秒"] >= 160
    assert status["书籍名"] == "书"
    assert status["总处理页数"] == 10
    assert status["处理进度"] == 20.0
    assert status["处理进度文本"] == "20.0%"
    assert status["处理速度"] >= 20
    assert status["运行时间"].endswith("秒")
    assert status["剩余时间"].endswith("秒")
    assert status["PDF总页数"] == 10
    assert status["是否抽样"] is False
    assert status["模式"] == "书籍高质量模式"


def test_status_marks_stale_running_job_as_suspected_stalled(tmp_path: Path) -> None:
    status_path = tmp_path / "状态.json"
    _write_status(
        status_path,
        source_pdf=Path("书.pdf"),
        status="进行中",
        target_pages=10,
        processed_pages=2,
        text_pages=2,
        blank_pages=0,
        failed_pages=0,
        low_confidence_pages=0,
        engine="paddleocr",
    )
    status = json.loads(status_path.read_text(encoding="utf-8"))
    status["更新时间"] = (datetime.now() - timedelta(minutes=20)).isoformat(timespec="seconds")
    status_path.write_text(json.dumps(status, ensure_ascii=False), encoding="utf-8")

    result = read_job_status(tmp_path, stalled_after_seconds=600)

    assert result["状态新鲜度"]["疑似中断"] is True
    assert result["状态新鲜度"]["运行判断"] == "疑似中断"


def test_fresh_worker_heartbeat_prevents_false_stall_for_a_slow_page(tmp_path: Path) -> None:
    status_path = tmp_path / "状态.json"
    _write_status(
        status_path,
        source_pdf=Path("书.pdf"),
        status="进行中",
        target_pages=10,
        processed_pages=2,
        text_pages=2,
        blank_pages=0,
        failed_pages=0,
        low_confidence_pages=0,
        engine="paddleocr",
    )
    status = json.loads(status_path.read_text(encoding="utf-8"))
    status["更新时间"] = (datetime.now() - timedelta(minutes=20)).isoformat(timespec="seconds")
    status_path.write_text(json.dumps(status, ensure_ascii=False), encoding="utf-8")
    (tmp_path / "后台任务心跳.json").write_text(
        json.dumps({"状态": "运行中", "进程ID": 97531}, ensure_ascii=False), encoding="utf-8"
    )

    result = read_job_status(tmp_path, stalled_after_seconds=600)

    assert result["状态新鲜度"]["疑似中断"] is False
    assert result["状态新鲜度"]["运行判断"] == "活跃（工作进程心跳）"
    assert result["工作进程心跳"]["进程ID"] == 97531
    assert result["任务指标"]["书籍名"] == "书"
    assert result["任务指标"]["总处理页数"] == 10
    assert result["任务指标"]["处理进度"] == 20.0


def test_worker_heartbeat_publishes_page_boundaries_and_durable_progress(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = tmp_path / "runtime" / "tasks.sqlite3"
    store = TaskStore(database)
    task = store.claim_task(
        idempotency_key="heartbeat-book",
        source_path=tmp_path / "书.pdf",
        output_root=tmp_path / "out",
        mode="book-balanced",
    ).task
    attempt = store.start_attempt(task.job_id, supervisor_id="test")
    heartbeat_path = tmp_path / "任务" / "后台任务心跳.json"
    monkeypatch.setenv("PDF_RESCUE_HEARTBEAT_PATH", str(heartbeat_path))
    monkeypatch.setenv("PDF_RESCUE_TASK_DATABASE", str(database))
    monkeypatch.setenv("PDF_RESCUE_TASK_ATTEMPT_ID", attempt.attempt_id)
    heartbeat = book_pipeline._WorkerHeartbeat(tmp_path / "任务")
    page = PageRecord(page_number=1, text="已确认文本", confidence=0.97, source="paddleocr")

    heartbeat.start()
    heartbeat.set_total_pages(1)
    heartbeat.page_started(1)
    running = json.loads(heartbeat_path.read_text(encoding="utf-8"))
    heartbeat.page_completed(page)
    heartbeat.finish("已完成")
    finished = json.loads(heartbeat_path.read_text(encoding="utf-8"))

    assert running["当前页"] == 1
    assert running["最后完成页"] is None
    assert finished["当前页"] is None
    assert finished["最后完成页"] == 1
    assert store.get_task(task.job_id).completed_pages == 1
    assert store.get_task(task.job_id).last_completed_page == 1


def test_resume_job_reuses_stalled_job_configuration(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source_pdf = tmp_path / "书.pdf"
    source_pdf.write_bytes(b"%PDF-1.4\n%%EOF\n")
    status_path = tmp_path / "任务" / "状态.json"
    _write_status(
        status_path,
        source_pdf=source_pdf,
        status="进行中",
        target_pages=5,
        processed_pages=2,
        text_pages=2,
        blank_pages=0,
        failed_pages=0,
        low_confidence_pages=0,
        engine="paddleocr",
        total_pages=10,
        is_sample=True,
        mode="book-quality",
    )
    status = json.loads(status_path.read_text(encoding="utf-8"))
    status["更新时间"] = (datetime.now() - timedelta(minutes=20)).isoformat(timespec="seconds")
    status_path.write_text(json.dumps(status, ensure_ascii=False), encoding="utf-8")
    calls: list[dict] = []

    def fake_extract(path: Path, **kwargs: object) -> dict:
        calls.append({"path": path, **kwargs})
        return {"status": "ok", "job_dir": str(status_path.parent)}

    monkeypatch.setattr("pdf_rescue_mcp.book_pipeline.extract_book_text", fake_extract)

    result = resume_job(status_path.parent)

    assert result["动作"] == "已恢复并完成"
    assert calls[0]["mode"] == "book-quality"
    assert calls[0]["max_pages"] == 5
    assert calls[0]["resume"] is True


def test_export_page_image_evidence_writes_audit_image(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    job_dir = tmp_path / "任务"
    status_path = job_dir / "状态.json"
    pages_path = job_dir / "数据" / "页面.jsonl"
    source_pdf = tmp_path / "书.pdf"
    source_pdf.write_bytes(b"%PDF-1.4\n%%EOF\n")
    status_path.parent.mkdir(parents=True, exist_ok=True)
    status_path.write_text(
        json.dumps({"来源PDF": str(source_pdf)}, ensure_ascii=False),
        encoding="utf-8",
    )
    pages_path.parent.mkdir(parents=True, exist_ok=True)
    pages_path.write_text(
        json.dumps({"页码": 2, "文本": "页面正文", "置信度": 0.98}, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )

    def fake_render_pdf_page(pdf_path: Path, page_index: int, output_path: Path, dpi: int) -> None:
        assert pdf_path == source_pdf
        assert page_index == 1
        assert dpi == 120
        output_path.write_bytes(b"png")

    monkeypatch.setattr("pdf_rescue_mcp.book_pipeline.render_pdf_page", fake_render_pdf_page)

    result = export_page_image_evidence(job_dir, 2, dpi=120)

    assert Path(result["图像路径"]).exists()
    assert result["页面记录"]["文本"] == "页面正文"
    assert result["分辨率"] == 120


def test_export_page_image_evidence_reads_cache_before_pages_file_exists(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    job_dir = tmp_path / "任务"
    status_path = job_dir / "状态.json"
    source_pdf = tmp_path / "书.pdf"
    source_pdf.write_bytes(b"%PDF-1.4\n%%EOF\n")
    status_path.parent.mkdir(parents=True, exist_ok=True)
    status_path.write_text(
        json.dumps({"来源PDF": str(source_pdf)}, ensure_ascii=False),
        encoding="utf-8",
    )
    _write_cached_page(
        job_dir / "缓存" / "页面OCR",
        PageRecord(page_number=3, text="缓存正文", confidence=0.91, source="paddleocr"),
    )

    def fake_render_pdf_page(pdf_path: Path, page_index: int, output_path: Path, dpi: int) -> None:
        output_path.write_bytes(b"png")

    monkeypatch.setattr("pdf_rescue_mcp.book_pipeline.render_pdf_page", fake_render_pdf_page)

    result = export_page_image_evidence(job_dir, 3, dpi=120)

    assert Path(result["图像路径"]).exists()
    assert result["页面记录"]["文本"] == "缓存正文"
    assert result["页面记录"]["页码"] == 3


def test_get_page_evidence_reads_cache_before_pages_file_exists(tmp_path: Path) -> None:
    job_dir = tmp_path / "任务"
    _write_cached_page(
        job_dir / "缓存" / "页面OCR",
        PageRecord(page_number=4, text="运行中缓存正文", confidence=0.93, source="paddleocr"),
    )

    result = get_page_evidence(job_dir, 4)

    assert result["页码"] == 4
    assert result["文本"] == "运行中缓存正文"


def test_get_page_evidence_applies_latest_rules_to_cached_page(tmp_path: Path) -> None:
    job_dir = tmp_path / "任务"
    source_pdf = tmp_path / "中国农业百科全书：蚕业卷.pdf"
    source_pdf.write_bytes(b"%PDF-1.4\n%%EOF\n")
    _write_status(
        job_dir / "状态.json",
        source_pdf=source_pdf,
        status="进行中",
        target_pages=1,
        processed_pages=1,
        text_pages=1,
        blank_pages=0,
        failed_pages=0,
        low_confidence_pages=0,
        engine="paddleocr",
        total_pages=1,
        is_sample=False,
    )
    _write_cached_page(
        job_dir / "缓存" / "页面OCR",
        PageRecord(page_number=1, text="目\n录\n前言", confidence=0.95, source="paddleocr"),
    )

    result = get_page_evidence(job_dir, 1)

    assert "目录" in result["文本"]
    assert "\n目\n录\n" not in f"\n{result['文本']}\n"


def test_audit_job_quality_reads_running_cache_and_latest_rules(tmp_path: Path) -> None:
    job_dir = tmp_path / "任务"
    cache_dir = job_dir / "缓存" / "页面OCR"
    source_pdf = tmp_path / "中国农业百科全书：蚕业卷.pdf"
    source_pdf.write_bytes(b"%PDF-1.4\n%%EOF\n")
    _write_status(
        job_dir / "状态.json",
        source_pdf=source_pdf,
        status="进行中",
        target_pages=3,
        processed_pages=2,
        text_pages=2,
        blank_pages=0,
        failed_pages=0,
        low_confidence_pages=1,
        engine="paddleocr",
        total_pages=3,
        is_sample=False,
    )
    _write_cached_page(
        cache_dir,
        PageRecord(page_number=1, text="目\n录\n前言…\n凡例", confidence=0.95, source="paddleocr"),
    )
    _write_cached_page(
        cache_dir,
        PageRecord(
            page_number=2,
            text="\n".join(
                [
                    "瓶形，基部大，端部小",
                    "0",
                    "0",
                    "分生孢子",
                    "C",
                    "白僵菌在生长过程中能分泌毒素",
                    "0",
                    "C",
                    "C",
                    "气生菌丝",
                    "Ω",
                    "外皮",
                    "营养菌丝",
                    "图2 白僵菌生长发育及传染模式",
                    "1",
                    "2",
                    "3",
                    "图1 白僵菌形态",
                    "1. 分生孢子",
                    "2. 分生孢子发芽",
                    "3. 短菌丝",
                    "病症蚕感染白僵病初期，外观与健康蚕无异",
                ]
            ),
            confidence=0.88,
            source="paddleocr",
        ),
    )

    result = audit_job_quality(job_dir)

    assert result["页面来源"] == "逐页缓存"
    assert result["已巡检页数"] == 2
    assert result["尚未巡检页数"] == 1
    assert result["低置信页数"] == 1
    assert result["可自动刷新页数"] >= 2
    assert result["分栏重排页数"] == 0
    assert result["书内页码移除页数"] == 0
    assert result["页边噪声清理页数"] == 0
    assert result["图表噪声清理页数"] >= 1
    assert any(issue["页码"] == 2 and "页面置信度偏低" in issue["问题"] for issue in result["问题页"])


def test_audit_job_quality_accepts_blank_page_from_chinese_pages_file(tmp_path: Path) -> None:
    job_dir = tmp_path / "任务"
    source_pdf = tmp_path / "书.pdf"
    source_pdf.write_bytes(b"%PDF-1.4\n%%EOF\n")
    _write_status(
        job_dir / "状态.json",
        source_pdf=source_pdf,
        status="完成",
        target_pages=1,
        processed_pages=1,
        text_pages=0,
        blank_pages=1,
        failed_pages=0,
        low_confidence_pages=0,
        engine="paddleocr",
        total_pages=1,
        is_sample=False,
    )
    pages_path = job_dir / "数据" / "页面.jsonl"
    pages_path.parent.mkdir(parents=True, exist_ok=True)
    pages_path.write_text(
        json.dumps(
            {"页码": 1, "文本": "", "置信度": 1.0, "来源": "疑似空白页", "警告": ["疑似空白页"]},
            ensure_ascii=False,
        )
        + "\n",
        encoding="utf-8",
    )

    result = audit_job_quality(job_dir)

    assert result["无文本页数"] == 1
    assert result["仅警告可刷新页数"] == 0
    assert result["问题页数"] == 0


def test_audit_job_quality_separates_guarded_dense_index_low_confidence(tmp_path: Path) -> None:
    job_dir = tmp_path / "任务"
    cache_dir = job_dir / "缓存" / "页面OCR"
    source_pdf = tmp_path / "总目.pdf"
    source_pdf.write_bytes(b"%PDF-1.4\n%%EOF\n")
    _write_status(
        job_dir / "状态.json",
        source_pdf=source_pdf,
        status="完成",
        target_pages=1,
        processed_pages=1,
        text_pages=1,
        blank_pages=0,
        failed_pages=0,
        low_confidence_pages=1,
        engine="paddleocr",
        total_pages=1,
        is_sample=False,
    )
    _write_cached_page(
        cache_dir,
        PageRecord(
            page_number=11,
            text="\n".join(f"条目{i} …… {i}" for i in range(30)),
            confidence=0.82,
            source="paddleocr",
            warnings=[
                "页面平均置信度低于 0.90",
                "疑似目录或索引密集页，已按完整性优先处理",
                "高分辨率重跑文字明显变少，保留较完整原结果",
            ],
        ),
    )

    result = audit_job_quality(job_dir)

    assert result["低置信页数"] == 1
    assert result["密集索引保护低置信页数"] == 1
    assert result["问题页数"] == 0


def test_cached_low_confidence_page_is_retried_and_cache_updated(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cache_dir = tmp_path / "缓存"
    status_path = tmp_path / "状态.json"
    cached_page = PageRecord(
        page_number=1,
        text="模糊",
        confidence=0.5,
        source="paddleocr",
        warnings=["页面平均置信度低于 0.90"],
    )
    _write_cached_page(cache_dir, cached_page)
    calls: list[tuple[int, int]] = []

    def fake_create_adapter(model_size: str = "small") -> tuple[str, FakeAdapter]:
        return "paddleocr", FakeAdapter()

    def fake_ocr_page(
        pdf_path: Path,
        page_number: int,
        *,
        adapter: FakeAdapter,
        engine_name: str,
        dpi: int,
    ) -> tuple[str, PageRecord]:
        calls.append((page_number, dpi))
        return engine_name, PageRecord(
            page_number=page_number,
            text="更清楚的中文正文",
            confidence=0.96,
            source=engine_name,
            warnings=[],
        )

    monkeypatch.setattr("pdf_rescue_mcp.book_pipeline.create_ocr_adapter", fake_create_adapter)
    monkeypatch.setattr("pdf_rescue_mcp.book_pipeline.ocr_pdf_page", fake_ocr_page)

    engine, pages, failed_reports, low_reports = _extract_ocr_pages_resumable(
        Path("书.pdf"),
        cache_dir,
        status_path,
        target_pages=1,
        resume=True,
        dpi=220,
    )

    assert engine == "paddleocr"
    assert calls == [(1, 300)]
    assert pages[0].text == "更清楚的中文正文"
    assert pages[0].confidence == 0.96
    assert any("高分辨率重跑提升质量" in warning for warning in pages[0].warnings)
    assert failed_reports == []
    assert low_reports == []
    assert _load_cached_page(cache_dir, 1) == pages[0]


def test_successful_ocr_page_is_cached_for_a_later_resume(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cache_dir = tmp_path / "缓存"
    status_path = tmp_path / "状态.json"

    def fake_create_adapter(model_size: str = "small") -> tuple[str, FakeAdapter]:
        return "paddleocr", FakeAdapter()

    def fake_ocr_page(
        pdf_path: Path,
        page_number: int,
        *,
        adapter: FakeAdapter,
        engine_name: str,
        dpi: int,
    ) -> tuple[str, PageRecord]:
        return engine_name, PageRecord(
            page_number=page_number,
            text="这是可恢复的高置信度正文内容。" * 8,
            confidence=0.99,
            source=engine_name,
            warnings=[],
        )

    monkeypatch.setattr("pdf_rescue_mcp.book_pipeline.create_ocr_adapter", fake_create_adapter)
    monkeypatch.setattr("pdf_rescue_mcp.book_pipeline.ocr_pdf_page", fake_ocr_page)

    engine, pages, failed_reports, low_reports = _extract_ocr_pages_resumable(
        Path("书.pdf"),
        cache_dir,
        status_path,
        target_pages=1,
        resume=True,
        dpi=220,
    )

    assert engine == "paddleocr"
    assert failed_reports == []
    assert low_reports == []
    assert _load_cached_page(cache_dir, 1) == pages[0]

    def fail_create_adapter(model_size: str = "small") -> tuple[str, FakeAdapter]:
        raise AssertionError("已缓存的页面不应再次初始化OCR引擎")

    monkeypatch.setattr("pdf_rescue_mcp.book_pipeline.create_ocr_adapter", fail_create_adapter)
    resumed_engine, resumed_pages, resumed_failures, resumed_low = _extract_ocr_pages_resumable(
        Path("书.pdf"),
        cache_dir,
        status_path,
        target_pages=1,
        resume=True,
        dpi=220,
    )

    assert resumed_engine == "page_cache"
    assert resumed_pages[0].text == pages[0].text
    assert resumed_failures == []
    assert resumed_low == []


def test_already_retried_low_confidence_cache_is_not_retried(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cache_dir = tmp_path / "缓存"
    status_path = tmp_path / "状态.json"
    cached_page = PageRecord(
        page_number=1,
        text="仍然不够清楚",
        confidence=0.72,
        source="paddleocr",
        warnings=["页面平均置信度低于 0.90", "高分辨率重跑未改善，保留原结果"],
    )
    _write_cached_page(cache_dir, cached_page)

    def fail_create_adapter(model_size: str = "small") -> tuple[str, FakeAdapter]:
        raise AssertionError("不应初始化识别引擎")

    monkeypatch.setattr("pdf_rescue_mcp.book_pipeline.create_ocr_adapter", fail_create_adapter)

    engine, pages, failed_reports, low_reports = _extract_ocr_pages_resumable(
        Path("书.pdf"),
        cache_dir,
        status_path,
        target_pages=1,
        resume=True,
        dpi=220,
    )

    assert engine == "page_cache"
    assert pages[0].text == cached_page.text
    assert pages[0].warnings == [
        "页面平均置信度低于 0.90",
        "高分辨率重跑未改善，保留原结果",
        "本页识别文本过短，请复核是否为封面、空白页或漏识别",
        "疑似封面、扉页或版权页，短文本已保留并列入审计",
    ]
    assert failed_reports == []
    assert low_reports == [
        {
            "页码": 1,
            "置信度": 0.72,
            "字数": len("仍然不够清楚"),
            "警告": [
                "页面平均置信度低于 0.90",
                "高分辨率重跑未改善，保留原结果",
                "本页识别文本过短，请复核是否为封面、空白页或漏识别",
                "疑似封面、扉页或版权页，短文本已保留并列入审计",
            ],
        }
    ]


def test_hybrid_resume_keeps_native_text_page_and_ocr_only_scanned_page(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cache_dir = tmp_path / "缓存"
    status_path = tmp_path / "状态.json"
    native_page = PageRecord(
        page_number=1,
        text="这是原生文本页，直接保留。",
        confidence=1.0,
        source="pdf_text_layer",
    )
    calls: list[int] = []

    def fake_create_adapter(model_size: str = "small") -> tuple[str, FakeAdapter]:
        return "paddleocr", FakeAdapter()

    def fake_ocr_page(
        pdf_path: Path,
        page_number: int,
        *,
        adapter: FakeAdapter,
        engine_name: str,
        dpi: int,
    ) -> tuple[str, PageRecord]:
        calls.append(page_number)
        return engine_name, PageRecord(
            page_number=page_number,
            text="这是扫描页OCR结果。",
            confidence=0.96,
            source=engine_name,
            warnings=[],
        )

    monkeypatch.setattr("pdf_rescue_mcp.book_pipeline.create_ocr_adapter", fake_create_adapter)
    monkeypatch.setattr("pdf_rescue_mcp.book_pipeline.ocr_pdf_page", fake_ocr_page)

    engine, pages, failed_reports, low_reports = _extract_ocr_pages_resumable(
        Path("混合书.pdf"),
        cache_dir,
        status_path,
        target_pages=2,
        resume=True,
        dpi=220,
        direct_pages={1: native_page},
    )

    assert engine == "paddleocr_hybrid"
    assert calls == [2]
    assert [page.source for page in pages] == ["pdf_text_layer", "paddleocr"]
    assert pages[0].text == native_page.text
    assert failed_reports == []
    assert low_reports == []
    assert _load_cached_page(cache_dir, 1) == native_page


def test_retry_keeps_original_when_upgraded_text_is_much_shorter(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cache_dir = tmp_path / "缓存"
    status_path = tmp_path / "状态.json"
    original_text = "完整条目" * 40
    shorter_text = "清楚" * 20
    cached_page = PageRecord(
        page_number=1,
        text=original_text,
        confidence=0.82,
        source="paddleocr",
        warnings=["页面平均置信度低于 0.90"],
    )
    _write_cached_page(cache_dir, cached_page)

    def fake_create_adapter(model_size: str = "small") -> tuple[str, FakeAdapter]:
        return "paddleocr", FakeAdapter()

    def fake_ocr_page(
        pdf_path: Path,
        page_number: int,
        *,
        adapter: FakeAdapter,
        engine_name: str,
        dpi: int,
    ) -> tuple[str, PageRecord]:
        return engine_name, PageRecord(
            page_number=page_number,
            text=shorter_text,
            confidence=0.99,
            source=engine_name,
            warnings=[],
        )

    monkeypatch.setattr("pdf_rescue_mcp.book_pipeline.create_ocr_adapter", fake_create_adapter)
    monkeypatch.setattr("pdf_rescue_mcp.book_pipeline.ocr_pdf_page", fake_ocr_page)

    engine, pages, failed_reports, low_reports = _extract_ocr_pages_resumable(
        Path("书.pdf"),
        cache_dir,
        status_path,
        target_pages=1,
        resume=True,
        dpi=220,
    )

    assert engine == "paddleocr"
    assert pages[0].text == original_text
    assert any("文字明显变少" in warning for warning in pages[0].warnings)
    assert failed_reports == []
    assert low_reports == [
        {
            "页码": 1,
            "置信度": 0.82,
            "字数": len(original_text),
            "警告": ["页面平均置信度低于 0.90", "高分辨率重跑文字明显变少，保留较完整原结果"],
        }
    ]
    assert _load_cached_page(cache_dir, 1) == pages[0]


def test_detects_dense_index_page() -> None:
    text = "\n".join(f"农业条目{i}…………{i + 100}" for i in range(30))
    page = PageRecord(
        page_number=1,
        text=text,
        confidence=0.82,
        source="paddleocr",
    )

    assert _is_dense_index_page(page) is True


def test_dense_index_with_separate_page_numbers_is_not_cleaned_as_diagram() -> None:
    text = "\n".join(
        line
        for index in range(30)
        for line in (f"农业条目{index}…………", str(index + 100))
    )
    page = PageRecord(
        page_number=481,
        text=text,
        confidence=0.84,
        source="paddleocr",
    )

    assert _is_dense_index_page(page) is True
    assert _is_diagram_like_page(page) is False
    _prepare_ocr_page(page, book_title="中国农业百科全书：中兽医卷")
    assert "\n100\n" in f"\n{page.text}\n"
    assert any("目录或索引密集页" in warning for warning in page.warnings)
    assert not any("图表页孤立噪声" in warning for warning in page.warnings)


def test_detects_diagram_like_page() -> None:
    text = "\n".join(
        [
            "产生",
            "限制性内",
            "从mRNA",
            "基因的",
            "DNA",
            "机械剪刀",
            "片断",
            "切酶处理",
            "合成cDNA",
            "化学合成",
            "粘性末端",
            "平整末端",
            "人工接头",
            "连接",
            "连接",
            "连接",
            "宿主细胞",
            "选择",
            "遗传学",
            "免疫学",
            "重组DNA技术的基本技术路线",
            "(箭头表示常采取的技术路线)",
        ]
    )
    page = PageRecord(
        page_number=20,
        text=text,
        confidence=0.95,
        source="paddleocr",
    )

    assert _is_diagram_like_page(page) is True
    _prepare_ocr_page(page, book_title="中国农业百科全书：生物技术卷")
    assert any("图表或流程图" in warning for warning in page.warnings)


def test_illustration_mixed_page_is_audited_without_removing_number_labels() -> None:
    text = "\n".join(
        [
            "生两个极体，在卵的前端融合成一个的极核，逐渐增大成为副核团。",
            "精子与卵原核相遇于卵的后端成为结合核，这部分是为胚胎发育区。",
            "5",
            "卵的一些细胞质与副核团结合，形成滋养羊膜，围绕着胚胎区。",
            "5",
            "副核团分裂成两团，同时在其中的结合核进行两次分裂。",
            "6",
            "一个胚胎，其后分别发育成两个独立的胚胎。",
            "6",
            "另一种寄生蜂一个卵可产生多个胚胎。",
            "(a)",
            "(b)",
            "(c)",
            "2",
            "(d)",
            "(e)",
            "瘿蚊广腹细蜂胚胎发育图",
            "(a)受精卵；(b)雌、雄原核结合；(c)发育2天后；",
            "(d)发育3天后；(e)发育6天后的双胎期",
            "1. 营养羊膜；2. 胚胎区；3. 寄主组织；",
            "4. 卵原核；5. 副核团；6. 原核；7. 精子",
        ]
    )
    page = PageRecord(page_number=105, text=text, confidence=0.95, source="paddleocr")

    assert _is_illustration_mixed_page(page) is True
    assert _is_diagram_like_page(page) is False
    _prepare_ocr_page(page, book_title="中国农业百科全书：昆虫卷")
    assert "瘿蚊广腹细蜂胚胎发育图" in page.text
    assert "5" in page.text
    assert any("图文混排标注页" in warning for warning in page.warnings)


def test_diagram_page_is_not_misclassified_as_dense_index() -> None:
    page = PageRecord(
        page_number=32,
        text="\n".join(
            [
                *[str(index) for index in range(1, 24)],
                "88猪体表穴位",
                "89猫体表穴位",
                "此页由朱达美绘",
            ]
        ),
        confidence=0.98,
        source="paddleocr",
    )

    assert _is_diagram_like_page(page) is True
    assert _is_dense_index_page(page) is False


def test_short_editorial_list_is_not_misclassified_as_diagram() -> None:
    page = PageRecord(
        page_number=43,
        text="\n".join([f"编委姓名{index}" for index in range(30)]),
        confidence=0.99,
        source="paddleocr",
    )

    assert _is_diagram_like_page(page) is False


def test_stale_dense_index_warning_is_replaced_by_current_diagram_warning() -> None:
    page = PageRecord(
        page_number=40,
        text="\n".join(
            [
                *[str(index) for index in range(1, 24)],
                "97马的骨骼及主要穴位",
                "此页由李文光绘",
            ]
        ),
        confidence=0.98,
        source="paddleocr",
        warnings=["疑似目录或索引密集页，已按完整性优先处理"],
    )

    _prepare_ocr_page(page, book_title="中国农业百科全书：中兽医卷")

    assert not any("目录或索引密集页" in warning for warning in page.warnings)
    assert any("图表或流程图页面" in warning for warning in page.warnings)


def test_detects_table_and_diagram_page() -> None:
    text = "\n".join(
        [
            "0",
            "口",
            "口",
            "(a)",
            "(b)",
            "(c)",
            "间接血凝模式图",
            "(a) 醛化红细胞",
            "(b) 可溶性抗原",
            "部分血清学技术的灵敏度与用途",
            "在最佳条件下的灵敏度(ng/ml)",
            "用途",
            "类别",
            "抗原",
            "抗体",
            "定性",
            "定量",
            "定位",
            "放射免疫测定",
            "<0.05",
            "酶联免疫吸附测定",
            "0.05",
            "琼脂双扩散",
            "<500",
            "对流免疫电泳",
            "150",
        ]
    )
    page = PageRecord(page_number=140, text=text, confidence=0.87, source="paddleocr")

    assert _is_diagram_like_page(page) is True
    _prepare_ocr_page(page, book_title="中国农业百科全书：生物学卷分册-生物技术")
    assert any("图表或流程图" in warning for warning in page.warnings)


def test_diagram_like_page_removes_isolated_noise_symbols() -> None:
    text = "\n".join(
        [
            "瓶形，基部大，端部小",
            "0",
            "0",
            "分生孢子",
            "C",
            "生孢子，成熟后为葡萄串聚积",
            "0",
            "C",
            "C",
            "气生菌丝",
            "Ω",
            "外皮",
            "营养菌丝",
            "图2 白僵菌生长发育及传染模式",
            "1",
            "2",
            "3",
            "图1 白僵菌形态",
            "1. 分生孢子",
            "2. 分生孢子发芽",
            "3. 短菌丝",
            "病症蚕感染白僵病初期，外观与健康蚕无异",
            "图3 白僵病蚕症状",
        ]
    )
    page = PageRecord(page_number=23, text=text, confidence=0.88, source="paddleocr")

    _prepare_ocr_page(page, book_title="中国农业百科全书：蚕业卷")

    assert "\n0\n" not in f"\n{page.text}\n"
    assert "\nC\n" not in f"\n{page.text}\n"
    assert "\nΩ\n" not in f"\n{page.text}\n"
    assert "图1 白僵菌形态" in page.text
    assert any("孤立噪声符号" in warning for warning in page.warnings)
    assert any("图表或流程图" in warning for warning in page.warnings)


def test_mixed_text_diagram_page_removes_symbol_noise() -> None:
    text = "\n".join(
        [
            "瓶形，基部大，端部小，顶端细小处呈锯齿形弯曲，",
            "0",
            "0",
            "每一弯曲处延伸为一极短的小梗，在小梗上着生分",
            "0",
            "分生孢子",
            "C",
            "生孢子，成熟后为葡萄串聚积在气生菌丝上(图1)",
            "0",
            "白僵菌在生长过程中能分泌毒素，这种毒素属环",
            "C",
            "C",
            "状多肽类化合物，已发现有数种，其中白僵菌素Ⅱ对",
            "0",
            "0",
            "蚕的毒性较大，它是一种环状四肽。",
            "气生菌丝",
            "O",
            "外皮",
            "Ω",
            "体内",
            "营养菌丝",
            "c",
            "芽生孢",
            "C",
            "C",
            "图2 白僵菌生长发育及传染模式",
            "1",
            "2",
            "3",
            "图1 白僵菌形态",
            "1. 分生孢子 2. 分生孢子发芽 3. 短菌丝",
            "4. 分生孢子着生情况",
            "病症蚕感染白僵病初期，外观与健康蚕无异",
            "图3 白僵病蚕症状",
            "病斑出现部位不定，形状不规则，这种病斑的出现",
            "是由于菌的侵入引起几丁质外皮变性所致。",
        ]
    )
    page = PageRecord(page_number=23, text=text, confidence=0.8796, source="paddleocr")

    assert _is_diagram_like_page(page) is True
    _prepare_ocr_page(page, book_title="中国农业百科全书：蚕业卷")

    assert "\n0\n" not in f"\n{page.text}\n"
    assert "\nC\n" not in f"\n{page.text}\n"
    assert "\nΩ\n" not in f"\n{page.text}\n"
    assert "白僵菌在生长过程中能分泌毒素" in page.text
    assert "图2 白僵菌生长发育及传染模式" in page.text
    assert any("孤立噪声符号" in warning for warning in page.warnings)


def test_sericulture_domain_terms_are_corrected() -> None:
    page = PageRecord(
        page_number=23,
        text="\n".join(
            [
                "图2 白量菌生长发育及传染模式",
                "白僵菌分生抱子被蚕食下",
                "营养闲丝",
                "5龄末期乃至族中、茧中病死",
                "户体硬化后明显干瘪缩小",
                "一20℃的低温中可达4年之久",
                "白僵病后，约经3~7口死去",
                "熟蚕上族前后需要管理",
                "上簇室的要求基本与蚕室相似",
                "自僵蛹可作为白僵蚕代用品",
                "烘茧、煮虽和缫丝技术",
                "黄素为180毫克/下克，生产井岗霉素",
                "牛膝、七牛膝、筋骨草及露水卓",
                "百日青甾围，口本称梅托普伦",
                "拌匀，稍于后，在簇中迅速叶丝营茧",
                "2化性品种，5龄词育温度",
                "温度范围人致为7~40℃，一-般生长较好",
                "…般在5龄中期，生长极度时达最高点",
                "1 meteo-\nrological environments",
            ]
        ),
        confidence=0.91,
        source="paddleocr",
    )

    _prepare_ocr_page(page, book_title="中国农业百科全书：蚕业卷")

    assert "白僵菌生长发育" in page.text
    assert "分生孢子被蚕食下" in page.text
    assert "营养菌丝" in page.text
    assert "蔟中、茧中病死" in page.text
    assert "尸体硬化后明显干瘪缩小" in page.text
    assert "-20℃的低温中" in page.text
    assert "约经3~7日死去" in page.text
    assert "熟蚕上蔟前后" in page.text
    assert "上蔟室的要求" in page.text
    assert "白僵蛹可作为" in page.text
    assert "煮茧和缫丝" in page.text
    assert "毫克/千克" in page.text
    assert "井冈霉素" in page.text
    assert "土牛膝" in page.text
    assert "露水草" in page.text
    assert "甾酮" in page.text
    assert "日本称" in page.text
    assert "稍干后" in page.text
    assert "蔟中迅速吐丝营茧" in page.text
    assert "二化性品种" in page.text
    assert "饲育温度" in page.text
    assert "范围大致为" in page.text
    assert "一般生长较好" in page.text
    assert "一般在5龄中期" in page.text
    assert "生长极盛时" in page.text
    assert "meteorological environments" in page.text
    assert any("蚕业专业词" in warning for warning in page.warnings)


def test_low_confidence_large_lowercase_margin_artifact_is_removed() -> None:
    regular_blocks = [
        {
            "text": f"正文第{index}行",
            "confidence": 0.98,
            "bbox": [[100, 220 + index * 40], [700, 220 + index * 40], [700, 250 + index * 40], [100, 250 + index * 40]],
        }
        for index in range(30)
    ]
    page = PageRecord(
        page_number=82,
        text="ams\n" + "\n".join(block["text"] for block in regular_blocks),
        confidence=0.97,
        source="paddleocr",
        blocks=[
            {
                "text": "ams",
                "confidence": 0.51,
                "bbox": [[170, 120], [400, 120], [400, 180], [170, 180]],
            },
            *regular_blocks,
        ],
    )

    _prepare_ocr_page(page, book_title="中国农业百科全书：蚕业卷")

    assert "ams" not in page.text
    assert "正文第9行" in page.text
    assert any("页边低置信图像噪声" in warning for warning in page.warnings)

    latin_artifact_page = PageRecord(
        page_number=92,
        text="ROCTE\n" + "\n".join(block["text"] for block in regular_blocks),
        confidence=0.97,
        source="paddleocr",
        blocks=[
            {
                "text": "ROCTE",
                "confidence": 0.32,
                "bbox": [[170, 120], [430, 120], [430, 180], [170, 180]],
            },
            *regular_blocks,
        ],
    )

    _prepare_ocr_page(latin_artifact_page, book_title="中国农业百科全书：中兽医卷")

    assert "ROCTE" not in latin_artifact_page.text
    assert "正文第9行" in latin_artifact_page.text
    assert any("页边低置信图像噪声" in warning for warning in latin_artifact_page.warnings)

    chinese_artifact_page = PageRecord(
        page_number=100,
        text="脉\n" + "\n".join(block["text"] for block in regular_blocks),
        confidence=0.97,
        source="paddleocr",
        blocks=[
            {
                "text": "脉",
                "confidence": 0.1,
                "bbox": [[160, 120], [380, 120], [380, 170], [160, 170]],
            },
            *regular_blocks,
        ],
    )

    _prepare_ocr_page(chinese_artifact_page, book_title="中国农业百科全书：蚕业卷")

    assert "脉" not in chinese_artifact_page.text
    assert any("页边低置信图像噪声" in warning for warning in chinese_artifact_page.warnings)


def test_top_pinyin_index_header_is_removed_without_affecting_body() -> None:
    regular_blocks = [
        {
            "text": f"正文第{index}行",
            "confidence": 0.98,
            "bbox": [[100, 240 + index * 40], [700, 240 + index * 40], [700, 270 + index * 40], [100, 270 + index * 40]],
        }
        for index in range(30)
    ]
    page = PageRecord(
        page_number=441,
        text="zhong 中\n中 zhong\n多duo\nT t\nB\n" + "\n".join(block["text"] for block in regular_blocks),
        confidence=0.98,
        source="paddleocr",
        blocks=[
            {
                "text": "zhong 中",
                "confidence": 0.96,
                "bbox": [[120, 100], [350, 100], [350, 156], [120, 156]],
            },
            {
                "text": "中 zhong",
                "confidence": 0.96,
                "bbox": [[1200, 100], [1430, 100], [1430, 156], [1200, 156]],
            },
            {
                "text": "多duo",
                "confidence": 0.96,
                "bbox": [[650, 100], [900, 100], [900, 156], [650, 156]],
            },
            {
                "text": "T t",
                "confidence": 0.96,
                "bbox": [[900, 100], [1010, 100], [1010, 156], [900, 156]],
            },
            {
                "text": "B",
                "confidence": 0.96,
                "bbox": [[500, 160], [600, 160], [600, 260], [500, 260]],
            },
            *regular_blocks,
        ],
    )

    _prepare_ocr_page(page, book_title="中国农业百科全书：中兽医卷")

    assert "zhong 中" not in page.text
    assert "中 zhong" not in page.text
    assert "多duo" not in page.text
    assert "T t" not in page.text
    assert "B" not in page.text
    assert "正文第9行" in page.text
    assert any("拼音索引页眉" in warning for warning in page.warnings)


def test_printed_page_number_is_recorded_and_removed_from_body() -> None:
    blocks = [
        {
            "text": f"正文第{index}行",
            "confidence": 0.98,
            "bbox": [[150, 200 + index * 80], [760, 200 + index * 80], [760, 230 + index * 80], [150, 230 + index * 80]],
        }
        for index in range(12)
    ]
    blocks.append(
        {
            "text": "62",
            "confidence": 0.99,
            "bbox": [[150, 2050], [190, 2050], [190, 2080], [150, 2080]],
        }
    )
    page = PageRecord(
        page_number=82,
        text="\n".join(block["text"] for block in blocks),
        confidence=0.98,
        source="paddleocr",
        blocks=blocks,
    )

    _prepare_ocr_page(page, book_title="中国农业百科全书：蚕业卷")

    assert page.printed_page == "62"
    assert "\n62\n" not in f"\n{page.text}\n"
    assert any("书内页码" in warning for warning in page.warnings)


def test_mixed_text_diagram_removes_repeated_isolated_noise() -> None:
    text_lines = [f"这是图文混排页面的中等长度正文第{index}行" for index in range(40)]
    page = PageRecord(
        page_number=93,
        text="\n".join(
            [
                *text_lines,
                "0 0",
                "0",
                "O",
                "C",
                "○",
                "短标签甲",
                "短标签乙",
                "短标签丙",
                "短标签丁",
                "短标签戊",
                "图1 赤僵菌形态",
                "图2 赤僵病病蚕症状",
            ]
        ),
        confidence=0.95,
        source="paddleocr",
    )

    _prepare_ocr_page(page, book_title="中国农业百科全书：蚕业卷")

    assert "\n0 0\n" not in f"\n{page.text}\n"
    assert "\n0\n" not in f"\n{page.text}\n"
    assert "图1 赤僵菌形态" in page.text
    assert any("图表页孤立噪声" in warning for warning in page.warnings)


def test_photo_plate_removes_only_low_confidence_latin_noise() -> None:
    blocks = [
        {"text": "△1《元亨疗马集》等中兽医著作", "confidence": 0.99},
        {"text": "李文光摄", "confidence": 0.99},
        {"text": "4 1976年方城汉墓出土的走马图", "confidence": 0.98},
        {"text": "△2清代名兽医", "confidence": 0.99},
        {"text": "3 中、外文中兽医书刊", "confidence": 0.98},
        {"text": "sowns", "confidence": 0.52},
        {"text": "KAESASAY", "confidence": 0.52},
        {"text": "TEINARN AC.RNCE", "confidence": 0.61},
        {"text": "平二月吉日", "confidence": 0.54},
        {"text": "ABSTRACTS", "confidence": 0.95},
    ]
    page = PageRecord(
        page_number=11,
        text="\n".join(block["text"] for block in blocks),
        confidence=0.81,
        source="paddleocr",
        blocks=blocks,
    )

    _prepare_ocr_page(page, book_title="中国农业百科全书：中兽医卷")

    assert "sowns" not in page.text
    assert "KAESASAY" not in page.text
    assert "TEINARN AC.RNCE" not in page.text
    assert "△1《元亨疗马集》等中兽医著作" in page.text
    assert "平二月吉日" in page.text
    assert "ABSTRACTS" in page.text
    assert any("图版页低置信乱码" in warning for warning in page.warnings)


def test_veterinary_photo_caption_markers_and_terms_are_corrected() -> None:
    page = PageRecord(
        page_number=20,
        text="∇32口色与舌苔\n>57乌头\nD19教师正在授课\n√14家畜针麻技术\n比较值>57%时需复核",
        confidence=0.98,
        source="paddleocr",
    )

    _prepare_ocr_page(page, book_title="中国农业百科全书：中兽医卷")

    assert "△32舌色与舌苔" in page.text
    assert "△57乌头" in page.text
    assert "△19教师正在授课" in page.text
    assert "△14家畜针麻技术" in page.text
    assert "比较值>57%时需复核" in page.text
    assert any("规范图版编号" in warning for warning in page.warnings)
    assert any("中兽医卷常见错字" in warning for warning in page.warnings)


def test_veterinary_herb_index_terms_are_corrected() -> None:
    page = PageRecord(
        page_number=50,
        text="吴茱英 秦花 葛蒲 苦棟皮 贯仲 胡卢巴 斑蚕 曲孽散 草薛分清饮 鹃脉 走骗与斗兽图 口色绛红 海蝶蛸 治疗疗疽、湿疹 子官及肠管 川棟子 肉从蓉 元享疗马集 痈疽疗毒 晒于 米泄水 尿受惊，或滚跌 子官痉挛 平滑机 瓜萎 阴于法 当归从蓉汤 晴生翳障 本草草经集注\n本草草经\n集注 当归灰蓉汤 肉灰蓉 居惨 芜花 香蕾散 胃肠活 肽俞",
        confidence=0.96,
        source="paddleocr",
    )

    _prepare_ocr_page(page, book_title="中国农业百科全书：中兽医卷")

    for expected in (
        "吴茱萸",
        "秦艽",
        "菖蒲",
        "苦楝皮",
        "贯众",
        "胡芦巴",
        "斑蝥",
        "曲蘖散",
        "萆薢分清饮",
        "颈脉",
        "走马与斗兽图",
        "舌色绛红",
        "海螵蛸",
        "治疗痈疽、湿疹",
        "子宫及肠管",
        "川楝子",
        "肉苁蓉",
        "元亨疗马集",
        "痈疽疔毒",
        "晒干",
        "米泔水",
        "马受惊，或滚跌",
        "子宫痉挛",
        "平滑肌",
        "瓜蒌",
        "阴干法",
        "当归苁蓉汤",
        "睛生翳障",
        "本草经集注",
        "本草经\n集注",
        "当归苁蓉汤",
        "肉苁蓉",
        "居髎",
        "芫花",
        "香薷散",
        "胃肠炎",
        "脾俞",
    ):
        assert expected in page.text
    assert any("中兽医卷常见错字" in warning for warning in page.warnings)


def test_beekeeping_index_terms_are_corrected() -> None:
    page = PageRecord(
        page_number=14,
        text="蜂具设备 除瞒器 蜂蜡加エ 威阳市5月中下句，陕北5\n月旬花尊平\n行内氨酸以后义重新《巾帼王朝)(英\n语文法》白刺花末期常有死蜂现象 蜂上申加100克加人1克每隔二三四人喂1次定数量下群唄觉“1作”上蜂日龄天采蜜最高达24次天采蜜\n最高达24次个双箱体采蜜群…日间姿式伸人花冠内推挤人左右蜜蜂飞总在花采集花蜜花粉粑上案集伸人巢房吐人少量叶脉伸人齿端草木辉水上保持水上保\n持汇西日香草木樨广广泛耐于早一30℃40--50天30-40大由J纬度不间，开花迟早不同6月下句花瓣之问。·年生90)朵义比较抗旱干早而瘠薄的士地40)~50天3C~40人。各地黄上高原泌蜜时间约2人。",
        confidence=0.96,
        source="paddleocr",
    )

    _prepare_ocr_page(page, book_title="中国农业百科全书：养蜂卷")

    assert "除螨器" in page.text
    assert "蜂蜡加工" in page.text
    for expected in (
        "咸阳市",
        "5月中下旬，陕北5\n月下旬",
        "花萼平\n行",
        "丙氨酸",
        "以后又重新",
        "《巾帼王朝》",
        "《英\n语文法》",
        "白刺花末花期常有死蜂现象",
        "蜂王",
        "中加100克",
        "加入1克",
        "每隔二三日喂1次",
        "一定数量",
        "千群",
        "嗅觉",
        "“工作”",
        "工蜂日龄",
        "每天采蜜最高达24次",
        "每天采蜜\n最高达24次",
        "一个双箱体",
        "采蜜群一日间",
        "姿势",
        "伸入花冠内",
        "推挤入左右",
        "蜜蜂飞悬在花上采集花蜜",
        "花粉耙上聚集",
        "伸入巢房",
        "吐入少量",
        "叶脉伸入齿端",
        "草木樨",
        "水土保持",
        "水土保\n持",
        "广西",
        "白香草木樨",
        "广泛",
        "耐干旱",
        "-30℃",
        "40~50天",
        "30~40天",
        "由纬度",
        "不同，开花迟早不同",
        "6月下旬",
        "花瓣之间",
        "一年生",
        "90朵",
        "又比较抗旱",
        "干旱而瘠薄的土地",
        "黄土高原",
        "泌蜜时间约2天。",
    ):
        assert expected in page.text
    assert any("养蜂卷常见错字" in warning for warning in page.warnings)


def test_beekeeping_wax_foundation_and_comb_terms_are_corrected() -> None:
    page = PageRecord(
        page_number=40,
        text=(
            "注人其下两辊筒之问最后生产出所\n大小的巢础只需人操作7作效率巢础生产线的同。"
            "辊简中必蘸蜡有-3毫米高45℃左，以45℃左不消毒锅的些液浸人12毫米深分为.工蜂房的E台"
            "呈止六边形每-个菱形个巢房的\n房底另\n个是同个巢房房璧内径不，中蜂雄锋房脾卜部"
            "同上蜂房相比形状略人雄野房1.蜂房的大小用干培育雄蜂封盖天气孔"
        ),
        confidence=0.96,
        source="paddleocr",
    )

    _prepare_ocr_page(page, book_title="中国农业百科全书：养蜂卷")

    for expected in (
        "注入其下",
        "两辊筒之间",
        "最后生产出所\n需大小的巢础",
        "只需1人操作",
        "工作效率",
        "巢础生产线相同。",
        "辊筒",
        "中以蘸蜡",
        "有1~3毫米高",
        "45℃左右，以",
        "45℃左右",
        "消毒锅的蜡液",
        "浸入12毫米深",
        "分为工蜂房",
        "的王台",
        "呈正六边形",
        "每一个菱形",
        "一个巢房的\n房底",
        "另一个是",
        "同一巢房",
        "房壁",
        "内径不同，中蜂",
        "雄蜂房",
        "脾上部",
        "同工蜂房相比",
        "形状略大",
        "工蜂房的大小",
        "用于培育雄蜂",
        "封盖无气孔",
    ):
        assert expected in page.text
    assert any("养蜂卷常见错字" in warning for warning in page.warnings)


def test_beekeeping_comb_storage_terms_are_corrected() -> None:
    page = PageRecord(
        page_number=44,
        text=(
            "蜜蜂自已白然脾白\n然脾山人们尺寸划巢础律是1蜂房房基巢牌\n大小致而目绝大部分是[蜂房山蜜蜂"
            "号白色，半透光送到□\n器咀嚼趣长厚薄、人小小蜜峰意蜂台\n虫区春夏\n李常造含有人量雄蜂房"
            "巢牌蜂了蜜在1000克左6粉圈脾中卜部清除十净放人继箱继箱擦成一垛次用药使第层继箱"
            "燃烧期问窗日观察熏杀次，连续用小电炉引燃硫磺杀出安放--个上盖块铁片均地撒"
        ),
        confidence=0.96,
        source="paddleocr",
    )

    _prepare_ocr_page(page, book_title="中国农业百科全书：养蜂卷")

    for expected in (
        "蜜蜂自己",
        "自然脾",
        "自\n然脾",
        "由人们",
        "尺寸一样",
        "巢础均是工蜂房房基",
        "巢脾\n大小一致",
        "而且绝大部分是工蜂房",
        "呈白色，半透明",
        "送到口器咀嚼",
        "越长",
        "厚薄、大小",
        "小蜜蜂",
        "意蜂育虫区",
        "春夏季常筑造含有大量雄蜂房",
        "蜂子",
        "蜜在1000克左右",
        "⑥粉圈脾",
        "中下部",
        "清除干净",
        "放入继箱",
        "继箱摞成一垛",
        "一次用药即可",
        "第一层继箱",
        "燃烧期间",
        "窗口观察",
        "熏杀一次，连续",
        "用小电炉引燃硫磺杀虫",
        "安放一个",
        "上盖一块铁片",
        "均匀地撒",
    ):
        assert expected in page.text
    assert any("养蜂卷常见错字" in warning for warning in page.warnings)


def test_beekeeping_queen_and_sensillum_terms_are_corrected() -> None:
    page = PageRecord(
        page_number=47,
        text=(
            "蜂土处女上上预一-俟儿小时另-只腹部伸人，用汇蜂堆处女工不宜儿丁质由圈非常薄"
            "一从纤毛4从重力感觉纤\n毛口1器"
        ),
        confidence=0.96,
        source="paddleocr",
    )

    _prepare_ocr_page(page, book_title="中国农业百科全书：养蜂卷")

    for expected in (
        "蜂王",
        "处女王",
        "上颚",
        "一俟",
        "几小时",
        "另一只",
        "腹部伸入，用",
        "工蜂堆",
        "处女王不宜",
        "几丁质",
        "由一圈非常薄",
        "一丛纤毛",
        "4丛重力感觉纤\n毛",
        "口器",
    ):
        assert expected in page.text
    assert any("养蜂卷常见错字" in warning for warning in page.warnings)


def test_audit_reports_photo_plate_noise_cleanup(tmp_path: Path) -> None:
    job_dir = tmp_path / "任务"
    source_pdf = tmp_path / "中国农业百科全书：中兽医卷.pdf"
    source_pdf.write_bytes(b"%PDF-1.4\n%%EOF\n")
    _write_status(
        job_dir / "状态.json",
        source_pdf=source_pdf,
        status="完成",
        target_pages=1,
        processed_pages=1,
        text_pages=1,
        blank_pages=0,
        failed_pages=0,
        low_confidence_pages=0,
        engine="paddleocr",
        total_pages=1,
        is_sample=False,
    )
    blocks = [
        {"text": "△1中兽医著作", "confidence": 0.99},
        {"text": "4 汉墓出土走马图", "confidence": 0.98},
        {"text": "△2清代名兽医", "confidence": 0.99},
        {"text": "sowns", "confidence": 0.52},
    ]
    _write_cached_page(
        job_dir / "缓存" / "页面OCR",
        PageRecord(
            page_number=11,
            text="\n".join(block["text"] for block in blocks),
            confidence=0.96,
            source="paddleocr",
            blocks=blocks,
        ),
    )

    result = audit_job_quality(job_dir)

    assert result["图版乱码清理页数"] == 1
    assert result["图版乱码清理页样例"] == [11]
    assert result["可自动刷新页数"] == 1


def test_table_values_and_formula_denominators_are_not_diagram_noise() -> None:
    page = PageRecord(
        page_number=95,
        text="\n".join(
            [
                *[f"这是包含表格和公式的中等长度正文第{index}行" for index in range(40)],
                "图1 合法插图",
                "图2 另一幅合法插图",
                "90",
                "0",
                "2",
                "2",
            ]
        ),
        confidence=0.98,
        source="paddleocr",
    )

    _prepare_ocr_page(page, book_title="中国农业百科全书：蚕业卷")

    assert page.text.splitlines()[-4:] == ["90", "0", "2", "2"]
    assert not any("图表页孤立噪声" in warning for warning in page.warnings)


def test_sericulture_page_image_verified_terms_are_corrected() -> None:
    page = PageRecord(
        page_number=93,
        text=(
            "图1 赤世菌形态\n愕蚕与篦麻蚕的血缘很近，具行不规则斑点，杂种可台。\n"
            "感染后2~3大死亡，感染后4~6人死，熟蚕叶丝结蚕，茁灰褐色，虽重约3克。\n"
            "母虽壳、乌柏、小柏蚕；保持蚕座消洁，改善眠座坏境，消除2次传染。"
        ),
        confidence=0.95,
        source="paddleocr",
    )

    _prepare_ocr_page(page, book_title="中国农业百科全书：蚕业卷")

    for expected in (
        "赤僵菌",
        "樗蚕与蓖麻蚕",
        "具有不规则",
        "杂种可育",
        "2~3天死亡",
        "4~6天死",
        "吐丝结茧",
        "茧灰褐色",
        "茧重约3克",
        "母茧壳",
        "乌桕",
        "小椿蚕",
        "蚕座清洁",
        "眠座环境",
        "消除二次传染",
    ):
        assert expected in page.text


def test_repeated_sericulture_shape_confusions_are_corrected() -> None:
    page = PageRecord(
        page_number=64,
        text=(
            "食下量随之增人，人部分时间静止；司一龄中可占休腔容积。\n"
            "吐丝结虽，采虽后调查虽层，多角休中央染色。\n"
            "中肠起白第2胸节，外观呈粗人的长简形；怀形细胞呈届平形。\n"
            "母次眠中更新，固食膜固定在责门瓣，主要成份有儿丁质。\n"
            "形成坏状瓣膜，脉博随发育面减少，血液中含定的缓冲体系。\n"
            "在个龄期中，末端以言管告终，呈洲管状，有定的途径。\n"
            "士壤含水率变化，为害更人；广广东试点，卵期约10灭，哦体羽化。\n"
            "收购种虽后种虽收购完成，蚕品种虽长椭圆。"
        ),
        confidence=0.98,
        source="paddleocr",
    )

    _prepare_ocr_page(page, book_title="中国农业百科全书：蚕业卷")

    for expected in (
        "增大",
        "大部分",
        "同一龄",
        "体腔",
        "结茧",
        "采茧",
        "茧层",
        "多角体",
        "起自第2胸节",
        "粗大的长筒形",
        "杯形细胞呈扁平形",
        "每次眠中",
        "围食膜固定在贲门瓣",
        "几丁质",
        "环状瓣膜",
        "脉搏随发育而减少",
        "含一定的缓冲体系",
        "在各龄期中",
        "盲管告终",
        "圆管状",
        "有一定的途径",
        "土壤含水率",
        "为害更大",
        "广东试点",
        "10天",
        "蛾体羽化",
        "收购种茧",
        "种茧收购完成",
        "蚕品种茧长椭圆",
    ):
        assert expected in page.text


def test_sericulture_lifecycle_and_disease_terms_are_corrected() -> None:
    page = PageRecord(
        page_number=118,
        text=(
            "杯业上使用，并人量增殖，侵入脂肪组织和-部分肌肉。\n"
            "黄叫虫在十下越冬，在浙江下次年化蛹，8月下句孵化，长的可达32大。\n"
            "卵产在士表或1块裂隙中，幼虫多在1:表活动，5月中句后出口密度减少。\n"
            "蚕病之，包被着层灰粉，病原属丛梗抱科，学名Sp carin Sp。\n"
            "有3个发台阶段，分生抱了形成芽生泡了，着生飘形小梗和抱子链。\n"
            "一年即尖去活力，出现人形黑褐色病斑，人蚕感病与白僵病-样，厂体犹如裹上一么灰粉。\n"
            "日本人引范麻蚕种；种饲养方式之.混在1个词育区饲养杂交青种，均采用混含育。"
        ),
        confidence=0.95,
        source="paddleocr",
    )

    _prepare_ocr_page(page, book_title="中国农业百科全书：蚕业卷")

    for expected in (
        "蚕业上",
        "并大量增殖",
        "和一部分肌肉",
        "黄叶虫在土下越冬",
        "在浙江于次年化蛹",
        "8月下旬孵化",
        "32天",
        "土表或土块裂隙",
        "在土表活动",
        "5月中旬后虫口密度",
        "蚕病之一",
        "包被着一层灰粉",
        "丛梗孢科",
        "Spicaria sp.",
        "发育阶段",
        "分生孢子形成芽生孢子",
        "瓶形小梗和孢子链",
        "失去活力",
        "大形黑褐色病斑",
        "大蚕感病与白僵病一样",
        "尸体犹如裹上一层灰粉",
        "引蓖麻蚕种",
        "一种饲养方式之一",
        "饲育区饲养杂交育种",
        "混合育",
    ):
        assert expected in page.text


def test_cocoon_testing_terms_are_corrected() -> None:
    page = PageRecord(
        page_number=122,
        text=(
            "吐出后即成-根茧丝，从庄口虽量抽样，为缴丝工艺设计。\n"
            "-个庄口由子品种和族\n中环境不同，茁粒有差异，每包干虽抽样，使样虽保持致。\n"
            "抽样数量--般为1%，作中间性试缴；检验虽质，次虽和供试虽各100粒。\n"
            "测量虽幅，清除虽腔杂质，计算蚕虽出丝率；约10克左有，洗涤儿次，计算白分率。\n"
            "萤层检验为原料茧并生设计，烘验尤水干量，调整煮茧上艺并词时进煮。\n"
            "每添绪次连续缫出一根虽丝，计算原料虽量，煮茧T艺稳定，采用公定阿潮率。\n"
            "按卜茧标准；下虽凡不能缫丝，其它黄分等含穿虽，零星虽量一律采用人样茧和光虽量。"
        ),
        confidence=0.96,
        source="paddleocr",
    )

    _prepare_ocr_page(page, book_title="中国农业百科全书：蚕业卷")

    for expected in (
        "成一根茧丝",
        "庄口茧量",
        "为缫丝工艺",
        "一个庄口由于品种",
        "蔟\n中环境",
        "茧粒",
        "干茧",
        "样茧保持致",
        "一般为1%",
        "试缫",
        "检验茧质",
        "次茧和供试茧",
        "茧幅",
        "茧腔",
        "蚕茧出丝率",
        "克左右",
        "几次",
        "百分率",
        "茧层检验",
        "原料茧并庄设计",
        "无水干量",
        "煮茧工艺并同时进煮",
        "每添绪一次",
        "一根茧丝",
        "原料茧量",
        "公定回潮率",
        "按下茧标准",
        "下茧凡",
        "其它茧分等含穿茧",
        "零星茧量一律采用入样茧和光茧量",
    ):
        assert expected in page.text


def test_sericulture_science_and_seed_terms_are_corrected() -> None:
    page = PageRecord(
        page_number=140,
        text=(
            "尾虫病毒研究通过凋节卵细胞，应用民虫激素增产蚕药，使蚕虽产量提高。\n"
            "绿偶菌由宙氏蛾霉引起，学名murea rileyi Farlow.，飘形的分生他子小梗上有病虫！体。\n"
            "芽管货穿体壁，血色上常，病程迟绥约10大发病，形成阅形病斑，线债病可防。\n"
            "浓核病(densovirus discase)是毒病的-种，196~970年称之谓小型病毒，densovirns直径100S左石。\n"
            "圆简形细胞儿乎正常，ij杯形细胞尚尤它例，症状比较单，以多中症蚕体璧允满病液，用面清学诊断。\n"
            "Spng Eo\n杂交益种以定数量放长形铅框，密布一，约22.000粒，自留利时期催旨收蚁。"
        ),
        confidence=0.95,
        source="paddleocr",
    )

    _prepare_ocr_page(page, book_title="中国农业百科全书：蚕业卷")

    for expected in (
        "昆虫病毒",
        "调节卵细胞",
        "昆虫激素增产蚕茧",
        "蚕茧产量",
        "绿僵菌由雷氏蛾霉",
        "Nomuraea rileyi Farlow.",
        "瓶形的分生孢子小梗",
        "病虫尸体",
        "芽管贯穿体壁",
        "血色正常",
        "病程迟缓约10天发病",
        "圆形病斑",
        "绿僵病可防",
        "densovirus disease",
        "病毒病的一种",
        "1969~1970年",
        "称之为小型病毒",
        "densovirus直径100S左右",
        "圆筒形细胞几乎正常",
        "而杯形细胞尚无它例",
        "症状比较单一，以多数症蚕体壁充满病液",
        "血清学诊断",
        "杂交蚕种以一定数量放置长形铅框",
        "密布一层，约22,000粒",
        "自留种时期催青收蚁",
    ):
        assert expected in page.text
    assert "Spng Eo" not in page.text


def test_sericulture_domain_terms_do_not_apply_to_other_books() -> None:
    page = PageRecord(
        page_number=1,
        text="白量菌 分生抱子 上族",
        confidence=0.91,
        source="paddleocr",
    )

    _prepare_ocr_page(page, book_title="中国农业百科全书：总目")

    assert "白量菌" in page.text
    assert "分生抱子" in page.text
    assert "上族" in page.text


def test_front_matter_split_labels_are_merged_through_page_ten() -> None:
    page = PageRecord(
        page_number=7,
        text="\n".join(
            [
                "目",
                "录",
                "前言…",
                "凡例",
                "条目分类目录…",
                "索引",
            ]
        ),
        confidence=0.91,
        source="paddleocr",
    )

    _prepare_ocr_page(page, book_title="中国农业百科全书：蚕业卷")

    assert "目录" in page.text
    assert "\n目\n录\n" not in f"\n{page.text}\n"
    assert any("分裂标题" in warning for warning in page.warnings)

    long_page = PageRecord(
        page_number=6,
        text="\n".join(
            [
                "凡",
                "例",
                "一、全书以农业科学各学科知识体系为基础设卷。",
                "二、条目按条题第一个字的汉语拼音字母顺序排列。",
                "三、大多数条题后附有对应的英文。",
                "四、各卷正文前设本卷条目的分类目录，供读者了解内容全貌。",
                "五、有些条目的释文后附有参考书目，供读者选读。",
                "六、一个条目的内容涉及到其他条目，采用参见方式。",
            ]
        ),
        confidence=0.99,
        source="paddleocr",
    )

    _prepare_ocr_page(long_page, book_title="中国农业百科全书：蚕业卷")

    assert "凡例" in long_page.text
    assert "\n凡\n例\n" not in f"\n{long_page.text}\n"
    assert any("分裂标题" in warning for warning in long_page.warnings)

    staff_page = PageRecord(
        page_number=13,
        text="\n".join(
            [
                "中国农业百科全书编务委员会",
                "总编辑蔡盛林",
                "委",
                "员（以姓氏笔画为序）",
                "特约编辑王天玲",
                "编",
                "辑郭何生",
                "印",
                "制王祖炎杨顺根高岚",
            ]
        ),
        confidence=0.98,
        source="paddleocr",
    )

    _prepare_ocr_page(staff_page, book_title="中国农业百科全书：蚕业卷")

    assert "委员（以姓氏笔画为序）" in staff_page.text
    assert "编辑郭何生" in staff_page.text
    assert "印制王祖炎杨顺根高岚" in staff_page.text
    assert not any(marker in f"\n{staff_page.text}\n" for marker in ("\n委\n员", "\n编\n辑", "\n印\n制"))


def test_role_labels_are_merged_late_in_book() -> None:
    page = PageRecord(
        page_number=310,
        text="\n".join(
            [
                "昆虫卷编辑委员会",
                "主",
                "任",
                "吴福桢",
                "委",
                "员",
                "(按姓氏笔画顺序)",
                "总",
                "主编吴福桢副主编管致和",
                "秘",
                "书陈小龙",
            ]
        ),
        confidence=0.99,
        source="paddleocr",
    )

    _prepare_ocr_page(page, book_title="中国农业百科全书：总目")

    assert "主任" in page.text
    assert "委员" in page.text
    assert "总主编吴福桢副主编管致和" in page.text
    assert "秘书陈小龙" in page.text
    assert not any(marker in f"\n{page.text}\n" for marker in ("\n主\n任", "\n委\n员", "\n秘\n书"))


def test_short_front_matter_gets_specific_warning(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cache_dir = tmp_path / "缓存"
    status_path = tmp_path / "状态.json"

    def fake_create_adapter(model_size: str = "small") -> tuple[str, FakeAdapter]:
        return "paddleocr", FakeAdapter()

    def fake_ocr_page(
        pdf_path: Path,
        page_number: int,
        *,
        adapter: FakeAdapter,
        engine_name: str,
        dpi: int,
    ) -> tuple[str, PageRecord]:
        return engine_name, PageRecord(
            page_number=page_number,
            text="中兽医卷",
            confidence=0.95,
            source=engine_name,
            warnings=[],
        )

    monkeypatch.setattr("pdf_rescue_mcp.book_pipeline.create_ocr_adapter", fake_create_adapter)
    monkeypatch.setattr("pdf_rescue_mcp.book_pipeline.ocr_pdf_page", fake_ocr_page)

    _, pages, _, _ = _extract_ocr_pages_resumable(
        Path("书.pdf"),
        cache_dir,
        status_path,
        target_pages=1,
        resume=True,
        dpi=220,
    )

    assert any("封面、扉页或版权页" in warning for warning in pages[0].warnings)


def test_front_matter_corrects_book_title_from_filename() -> None:
    page = PageRecord(
        page_number=1,
        text="中国农业科全书\n兽医卷\n上",
        confidence=0.89,
        source="paddleocr",
    )

    _prepare_ocr_page(page, book_title="中国农业百科全书：兽医卷上卷")

    assert "中国农业百科全书" in page.text
    assert "中国农业科全书" not in page.text
    assert any("书名信息校正" in warning for warning in page.warnings)


def test_front_matter_corrects_common_copyright_errors() -> None:
    page = PageRecord(
        page_number=2,
        text="787×1092 毫米 16月开本\n1093年6月第1版1993年6月上海第1法印到",
        confidence=0.86,
        source="paddleocr",
    )

    _prepare_ocr_page(page, book_title="中国农业百科全书：养蜂卷")

    assert "1993年6月第1版1993年6月上海第1次印刷" in page.text
    assert "16 开本" in page.text
    assert any("版权页常见OCR错字" in warning for warning in page.warnings)


def test_front_matter_repairs_split_volume_title() -> None:
    page = PageRecord(
        page_number=1,
        text="中国农业百科全书\n兽医卷\n上\n农业出版社",
        confidence=0.91,
        source="paddleocr",
    )

    _prepare_ocr_page(page, book_title="中国农业百科全书：兽医卷上卷")

    assert "兽医卷上卷" in page.text
    assert "兽医卷\n上" not in page.text
    assert any("卷册标题" in warning for warning in page.warnings)


def test_front_matter_repairs_single_character_volume_title() -> None:
    page = PageRecord(
        page_number=1,
        text="中国农业百科全书\n总\n农业出版社",
        confidence=0.92,
        source="paddleocr",
    )

    _prepare_ocr_page(page, book_title="中国农业百科全书：总目")

    assert "总目" in page.text
    assert any("卷册标题" in warning for warning in page.warnings)


def test_front_matter_removes_low_confidence_noise_lines() -> None:
    page = PageRecord(
        page_number=1,
        text="<\n中国农业百科全书\n农业化学卷\n衣业\n农业出版社",
        confidence=0.84,
        source="paddleocr",
        blocks=[
            {"text": "<", "confidence": 0.43},
            {"text": "中国农业百科全书", "confidence": 0.99},
            {"text": "农业化学卷", "confidence": 0.99},
            {"text": "衣业", "confidence": 0.44},
            {"text": "农业出版社", "confidence": 0.99},
        ],
    )

    _prepare_ocr_page(page, book_title="中国农业百科全书：农业化学卷")

    assert "<" not in page.text
    assert "衣业" not in page.text
    assert "农业化学卷" in page.text
    assert len(page.blocks) == 5
    assert any("噪声行" in warning for warning in page.warnings)


def test_front_matter_repairs_split_short_volume_title_from_subtitle() -> None:
    page = PageRecord(
        page_number=2,
        text="Hvy\ny-112/15\n《中国农业百科全书·生物学》卷分册\n生\n术\n物\n技\n农业出版社",
        confidence=0.95,
        source="paddleocr",
        blocks=[
            {"text": "Hvy", "confidence": 0.68},
            {"text": "y-112/15", "confidence": 0.78},
            {"text": "《中国农业百科全书·生物学》卷分册", "confidence": 0.99},
            {"text": "生", "confidence": 1.0},
            {"text": "术", "confidence": 0.99},
            {"text": "物", "confidence": 1.0},
            {"text": "技", "confidence": 1.0},
            {"text": "农业出版社", "confidence": 0.99},
        ],
    )

    _prepare_ocr_page(page, book_title="中国农业百科全书：生物学卷分册-生物技术")

    assert "生物技术" in page.text
    assert page.text.count("生物技术") == 1
    assert "\n生\n" not in page.text
    assert "Hvy" not in page.text
    assert "y-112/15" not in page.text
    assert any("卷册标题" in warning for warning in page.warnings)
    assert any("噪声行" in warning for warning in page.warnings)


def test_front_matter_removes_library_stamp_noise() -> None:
    page = PageRecord(
        page_number=1,
        text="11583\n中国农业百科全书\n蚕业卷\n图书\n藏书\n农业出版社",
        confidence=0.95,
        source="paddleocr",
        blocks=[
            {"text": "11583", "confidence": 0.99},
            {"text": "中国农业百科全书", "confidence": 0.99},
            {"text": "蚕业卷", "confidence": 0.99},
            {"text": "图书", "confidence": 0.99},
            {"text": "藏书", "confidence": 0.99},
            {"text": "农业出版社", "confidence": 0.99},
        ],
    )

    _prepare_ocr_page(page, book_title="中国农业百科全书：蚕业卷")

    assert "11583" not in page.text
    assert "图书" not in page.text
    assert "藏书" not in page.text
    assert "蚕业卷" in page.text
    assert any("噪声行" in warning for warning in page.warnings)


def test_front_matter_merges_split_labels() -> None:
    page = PageRecord(
        page_number=4,
        text="前\n言\n中国农业百科全书\n委\n员（按姓氏笔画顺序）\n主\n任何康\n目\n录",
        confidence=0.99,
        source="paddleocr",
    )

    _prepare_ocr_page(page, book_title="中国农业百科全书：蚕业卷")

    assert "前言" in page.text
    assert "委员（按姓氏笔画顺序）" in page.text
    assert "主任何康" in page.text
    assert "目录" in page.text
    assert "前\n言" not in page.text
    assert any("分裂标题或职务标签" in warning for warning in page.warnings)


def test_front_matter_deduplicates_repeated_volume_title() -> None:
    page = PageRecord(
        page_number=2,
        text="《中国农业百科全书·生物学》卷分册\n生物技术\n生物技术\n生物技术\n农业出版社",
        confidence=0.95,
        source="paddleocr",
    )

    _prepare_ocr_page(page, book_title="中国农业百科全书：生物学卷分册-生物技术")

    assert page.text.count("生物技术") == 1
    assert any("合并重复卷册标题" in warning for warning in page.warnings)


def test_balanced_mode_skips_high_dpi_retry_for_dense_index_page() -> None:
    text = "\n".join(f"农业条目{i}…………{i + 100}" for i in range(30))
    page = PageRecord(
        page_number=1,
        text=text,
        confidence=0.82,
        source="paddleocr",
        warnings=["页面平均置信度低于 0.90"],
    )

    result = _retry_low_confidence_page(
        Path("书.pdf"),
        page,
        adapter=FakeAdapter(),
        engine_name="paddleocr",
        dpi=220,
    )

    assert result == page
    assert any("已跳过高分辨率重跑" in warning for warning in result.warnings)


def test_low_confidence_page_tries_rotated_orientation(monkeypatch: pytest.MonkeyPatch) -> None:
    rotations: list[int] = []

    def fake_ocr_page(
        pdf_path: Path,
        page_number: int,
        *,
        adapter: object,
        engine_name: str,
        dpi: int,
        rotation: int = 0,
    ) -> tuple[str, PageRecord]:
        rotations.append(rotation)
        if rotation == 90:
            return engine_name, PageRecord(
                page_number=page_number,
                text="旋转后完整表格正文 " * 20,
                confidence=0.94,
                source=engine_name,
            )
        return engine_name, PageRecord(
            page_number=page_number,
            text="方向错误的乱码 " * 8,
            confidence=0.52,
            source=engine_name,
        )

    monkeypatch.setattr("pdf_rescue_mcp.book_pipeline.ocr_pdf_page", fake_ocr_page)
    page = PageRecord(
        page_number=95,
        text="方向错误的乱码 " * 8,
        confidence=0.52,
        source="paddleocr",
    )

    result = _retry_low_confidence_page(
        Path("书.pdf"),
        page,
        adapter=FakeAdapter(),
        engine_name="paddleocr",
        dpi=220,
    )

    assert rotations == [0, 90, 270]
    assert "旋转后完整表格正文" in result.text
    assert any("90 度旋转页面" in warning for warning in result.warnings)
