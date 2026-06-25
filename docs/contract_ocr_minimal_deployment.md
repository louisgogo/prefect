# contract_ocr 最小化独立部署文档

> 目标：把现有的 `contract_ocr_flow` 改造为一个**不依赖 Prefect** 的命令行执行脚本，方便放到任意服务器上直接运行。

---

## 一、最终目录结构

```text
contract_ocr_minimal/
├── run_contract_ocr.py          # 入口脚本（命令行）
├── ocr/
│   ├── __init__.py              # 空文件，标识为包
│   ├── file_processor.py        # PDF/图片读取、图片页渲染
│   ├── llm_extractor.py         # 调用 OneAPI 提取合同/报关单信息
│   └── markdown_writer.py       # 保存 Markdown 结果
├── prompts/
│   ├── contract_prompt.txt      # 合同提取 prompt
│   └── customs_prompt.txt       # 报关单提取 prompt
├── requirements.txt             # Python 依赖
├── .env.example                 # 环境变量示例
└── README.md                    # 使用说明
```

---

## 二、依赖

```text
pymupdf>=1.23.0
Pillow>=9.0.0
httpx>=0.24.0
python-dotenv>=0.19.0
```

- `pymupdf` / `Pillow`：PDF 解析与图片处理。
- `httpx`：调用 OneAPI / OpenAI 兼容接口。
- `python-dotenv`：从 `.env` 读取配置。

> 不需要 `prefect`、`pandas`、`psycopg2`、`sqlalchemy`、`openpyxl`、`numpy`。

---

## 三、环境变量 `.env`

```bash
# 必填
OCR_LLM_API_KEY=你的OneAPI密钥

# 可选，默认 https://oneapi.xgd.com/v1
OCR_LLM_BASE_URL=https://oneapi.xgd.com/v1

# 可选，留空则自动选择：
#   有图片页 -> qwen3.7-plus
#   纯文本   -> kimi-k2.6-team
# OCR_LLM_MODEL=

# 可选，默认 200 秒
# OCR_LLM_TIMEOUT=200
```

---

## 四、各文件完整代码

### 4.1 `ocr/__init__.py`

空文件即可。

---

### 4.2 `ocr/file_processor.py`

```python
"""文件处理：PDF 文本提取、图片页渲染、图片加载。"""
import base64
import io
import os
from pathlib import Path
from typing import Any, Dict, List

import fitz  # PyMuPDF
from PIL import Image

# 当一页提取到的文本字符数小于该阈值时，认为该页是扫描/图片页
_MIN_TEXT_CHARS_PER_PAGE = 100
# 渲染图片 DPI
_RENDER_DPI = 200
# 单张图片最大边长
_MAX_IMAGE_SIZE = 2048


def _resize_image(image: Image.Image, max_size: int = _MAX_IMAGE_SIZE) -> Image.Image:
    """等比缩放图片，使长边不超过 max_size。"""
    width, height = image.size
    if max(width, height) <= max_size:
        return image
    ratio = max_size / max(width, height)
    new_size = (int(width * ratio), int(height * ratio))
    return image.resize(new_size, Image.Resampling.LANCZOS)


def _image_to_base64(image: Image.Image, fmt: str = "PNG") -> str:
    """将 PIL Image 转为 base64 字符串。"""
    buffer = io.BytesIO()
    image.save(buffer, format=fmt)
    return base64.b64encode(buffer.getvalue()).decode("utf-8")


def _load_image_base64(file_path: str) -> str:
    """加载图片文件并返回 base64。"""
    with Image.open(file_path) as img:
        if img.mode != "RGB":
            img = img.convert("RGB")
        img = _resize_image(img)
        return _image_to_base64(img, fmt="PNG")


def load_image(file_path: str) -> Dict[str, Any]:
    """加载图片文件。"""
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"图片文件不存在: {file_path}")
    return {
        "file_path": file_path,
        "text": "",
        "image_pages": [
            {
                "page_no": 1,
                "base64": f"data:image/png;base64,{_load_image_base64(file_path)}",
            }
        ],
    }


def extract_pdf_content(
    file_path: str, render_all_pages: bool = False
) -> Dict[str, Any]:
    """提取 PDF 文本，并将疑似扫描页渲染为图片。"""
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"PDF 文件不存在: {file_path}")

    doc = fitz.open(file_path)
    all_text_parts: List[str] = []
    image_pages: List[Dict[str, Any]] = []

    for page_no in range(len(doc)):
        page = doc.load_page(page_no)
        text = page.get_text("text") or ""
        text_stripped = text.strip()
        all_text_parts.append(f"--- 第 {page_no + 1} 页 ---\n{text_stripped}")

        if render_all_pages or len(text_stripped) < _MIN_TEXT_CHARS_PER_PAGE:
            try:
                pix = page.get_pixmap(dpi=_RENDER_DPI)
                image = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
                image = _resize_image(image)
                image_base64 = _image_to_base64(image, fmt="PNG")
                image_pages.append(
                    {
                        "page_no": page_no + 1,
                        "base64": f"data:image/png;base64,{image_base64}",
                    }
                )
                if render_all_pages:
                    print(f"  PDF 第 {page_no + 1} 页已渲染为图片用于识别")
                else:
                    print(f"  PDF 第 {page_no + 1} 页文本较少，已渲染为图片用于识别")
            except Exception as e:
                print(f"  PDF 第 {page_no + 1} 页渲染失败: {e}")

    doc.close()

    merged_text = "\n\n".join(all_text_parts)
    print(
        f"  PDF 处理完成: 共 {len(all_text_parts)} 页, "
        f"总文本字符 {len(merged_text)}, 图片页 {len(image_pages)} 页"
    )
    return {
        "file_path": file_path,
        "text": merged_text,
        "image_pages": image_pages,
    }


def resolve_file_content(
    file_path: str, render_all_pages: bool = False
) -> Dict[str, Any]:
    """根据扩展名自动选择 PDF 或图片处理方式。"""
    ext = Path(file_path).suffix.lower()
    if ext == ".pdf":
        return extract_pdf_content(file_path, render_all_pages=render_all_pages)
    if ext in {".png", ".jpg", ".jpeg", ".bmp", ".gif", ".webp"}:
        return load_image(file_path)
    raise ValueError(f"不支持的文件类型: {ext}，仅支持 pdf/png/jpg/jpeg")
```

---

### 4.3 `ocr/llm_extractor.py`

```python
"""LLM 调用与报关单/合同信息提取。"""
import json
import logging
import os
import re
from typing import Any, Dict, List

import httpx

logger = logging.getLogger(__name__)

_DEFAULT_BASE_URL = "https://oneapi.xgd.com/v1"
_DEFAULT_TEXT_MODEL = "kimi-k2.6-team"
_DEFAULT_VISION_MODEL = "qwen3.7-plus"
_DEFAULT_TIMEOUT = 200
_MAX_IMAGE_PAGES = 10


def _prompts_dir() -> str:
    return os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "prompts"
    )


def _load_contract_prompt() -> str:
    path = os.path.join(_prompts_dir(), "contract_prompt.txt")
    with open(path, "r", encoding="utf-8") as f:
        return f.read().strip()


def _load_customs_prompt() -> str:
    path = os.path.join(_prompts_dir(), "customs_prompt.txt")
    with open(path, "r", encoding="utf-8") as f:
        return f.read().strip()


def _build_messages(
    text: str,
    image_pages: List[Dict[str, Any]],
    system_content: str,
    user_prompt: str,
) -> List[Dict[str, Any]]:
    system_message = {"role": "system", "content": system_content}

    user_content: List[Dict[str, Any]] = [
        {
            "type": "text",
            "text": f"以下是从文档中提取到的文本内容：\n\n{text}\n\n{user_prompt}",
        }
    ]

    for img in image_pages[:_MAX_IMAGE_PAGES]:
        user_content.append(
            {"type": "image_url", "image_url": {"url": img["base64"], "detail": "high"}}
        )

    return [system_message, {"role": "user", "content": user_content}]


def _extract_json(text: str) -> Dict[str, Any]:
    match = re.search(r"```(?:json)?\s*(\{.*\})\s*```", text, re.DOTALL)
    if match:
        json_str = match.group(1)
    else:
        start = text.find("{")
        end = text.rfind("}")
        if start == -1 or end == -1 or end <= start:
            raise ValueError(f"未在模型输出中找到 JSON: {text[:200]}")
        json_str = text[start : end + 1]

    try:
        return json.loads(json_str)
    except json.JSONDecodeError as e:
        raise ValueError(f"JSON 解析失败: {e}\n文本片段: {json_str[:500]}")


def _extract_model_code(spec: str) -> str:
    """从规格型号字符串中提取型号代码。"""
    if not spec or not spec.strip():
        return "未明确"

    spec = spec.split("附：")[0].strip()
    spec = spec.rstrip("|")

    parts = [p.strip() for p in spec.split("|") if p.strip()]
    if not parts:
        return "未明确"

    for part in parts:
        if "型" in part:
            return part

    for i, part in enumerate(parts):
        if "牌" in part and i + 1 < len(parts):
            return parts[i + 1]

    for part in parts:
        if re.search(r"[A-Za-z0-9]", part):
            return part

    return "未明确"


def _clean_customs_item(item: Dict[str, Any]) -> Dict[str, Any]:
    keys = [
        "项号",
        "商品编号",
        "商品名称",
        "规格型号",
        "数量",
        "单位",
        "单价",
        "总价",
        "币制",
        "最终目的国",
        "原产国",
    ]
    cleaned: Dict[str, Any] = {}
    for key in keys:
        cleaned[key] = str(item.get(key, "未明确")).strip() or "未明确"
    cleaned["规格型号"] = _extract_model_code(cleaned["规格型号"])
    return cleaned


def _call_llm(
    text: str,
    image_pages: List[Dict[str, Any]],
    system_content: str,
    user_prompt: str,
    task_name: str,
) -> Dict[str, Any]:
    api_key = os.environ.get("OCR_LLM_API_KEY")
    base_url = os.environ.get("OCR_LLM_BASE_URL", _DEFAULT_BASE_URL).rstrip("/")
    has_images = bool(image_pages)
    default_model = _DEFAULT_VISION_MODEL if has_images else _DEFAULT_TEXT_MODEL
    model = os.environ.get("OCR_LLM_MODEL") or default_model
    timeout = int(os.environ.get("OCR_LLM_TIMEOUT", _DEFAULT_TIMEOUT))

    if not api_key:
        raise ValueError("OCR_LLM_API_KEY 环境变量未配置")

    messages = _build_messages(text, image_pages, system_content, user_prompt)

    payload = {
        "model": model,
        "messages": messages,
        "temperature": 0.1,
        "max_tokens": 4000,
    }

    logger.info(
        f"调用 LLM [{task_name}]: model={model}, base_url={base_url}, "
        f"text_chars={len(text)}, image_pages={len(image_pages[:_MAX_IMAGE_PAGES])}"
    )

    try:
        response = httpx.post(
            f"{base_url}/chat/completions",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json=payload,
            timeout=timeout,
        )
        response.raise_for_status()
    except httpx.HTTPStatusError as e:
        raise RuntimeError(
            f"LLM API 请求失败: {e.response.status_code} - {e.response.text[:500]}"
        ) from e
    except httpx.RequestError as e:
        raise RuntimeError(f"LLM API 网络错误: {e}") from e

    result = response.json()
    assistant_message = (
        result.get("choices", [{}])[0].get("message", {}).get("content", "")
    )

    if not assistant_message:
        raise ValueError("LLM 返回空内容")

    logger.info(f"LLM 原始输出长度: {len(assistant_message)}")
    return _extract_json(assistant_message)


def extract_contract_info(content: Dict[str, Any]) -> Dict[str, Any]:
    text = content.get("text", "")
    image_pages = content.get("image_pages", [])

    extracted = _call_llm(
        text=text,
        image_pages=image_pages,
        system_content="你是一个严谨的合同信息提取助手，只输出合法 JSON。",
        user_prompt=_load_contract_prompt(),
        task_name="extract_contract_info",
    )

    for field in ["协议抬头", "甲方", "乙方", "签约日期", "履约日期", "内容概述", "备注"]:
        extracted.setdefault(field, "未明确")
    return extracted


def extract_customs_info(content: Dict[str, Any]) -> Dict[str, Any]:
    text = content.get("text", "")
    image_pages = content.get("image_pages", [])

    extracted = _call_llm(
        text=text,
        image_pages=image_pages,
        system_content="你是一个严谨的海关报关单信息提取助手，只输出合法 JSON。",
        user_prompt=_load_customs_prompt(),
        task_name="extract_customs_info",
    )

    for field in ["海关编号", "境外收货人", "成交方式", "运费", "保费"]:
        extracted.setdefault(field, "未明确")
    extracted.setdefault("商品明细", [])

    if not isinstance(extracted.get("商品明细"), list):
        extracted["商品明细"] = []

    extracted["商品明细"] = [
        _clean_customs_item(item)
        for item in extracted["商品明细"]
        if isinstance(item, dict)
    ]
    return extracted
```

---

### 4.4 `ocr/markdown_writer.py`

```python
"""将 OCR 提取结果保存为 Markdown 文件。"""
import logging
import os
from datetime import datetime
from typing import Any, Dict

logger = logging.getLogger(__name__)


def save_contract_markdown(
    file_path: str, extracted: Dict[str, Any], output_dir: str
) -> str:
    if not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)

    base_name = os.path.splitext(os.path.basename(file_path))[0]
    output_path = os.path.join(output_dir, f"{base_name}.md")

    markdown = f"""# 合同 OCR 识别结果

## 文件信息
- 原文件：`{file_path}`
- 识别时间：{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

## 提取结果

| 字段 | 内容 |
|---|---|
| 协议抬头 | {extracted.get('协议抬头', '未明确')} |
| 甲方 | {extracted.get('甲方', '未明确')} |
| 乙方 | {extracted.get('乙方', '未明确')} |
| 签约日期 | {extracted.get('签约日期', '未明确')} |
| 履约日期 | {extracted.get('履约日期', '未明确')} |

## 内容概述

{extracted.get('内容概述', '未明确')}

## 备注

{extracted.get('备注', '无')}
"""

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(markdown)

    logger.info(f"Markdown 已保存: {output_path}")
    print(f"OCR 结果已保存: {output_path}")
    return output_path


def save_customs_markdown(
    file_path: str, extracted: Dict[str, Any], output_dir: str
) -> str:
    if not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)

    base_name = os.path.splitext(os.path.basename(file_path))[0]
    output_path = os.path.join(output_dir, f"{base_name}.md")

    海关编号 = extracted.get("海关编号", "未明确")
    境外收货人 = extracted.get("境外收货人", "未明确")
    成交方式 = extracted.get("成交方式", "未明确")
    运费 = extracted.get("运费", "未明确")
    保费 = extracted.get("保费", "未明确")
    items = extracted.get("商品明细", [])

    items_md = ""
    if items:
        for idx, item in enumerate(items, start=1):
            items_md += f"""### 第 {idx} 项

- 项号：{item.get('项号', '未明确')}
- 商品编号：{item.get('商品编号', '未明确')}
- 商品名称：{item.get('商品名称', '未明确')}
- 规格型号：{item.get('规格型号', '未明确')}
- 数量：{item.get('数量', '未明确')}
- 单位：{item.get('单位', '未明确')}
- 单价：{item.get('单价', '未明确')}
- 总价：{item.get('总价', '未明确')}
- 币制：{item.get('币制', '未明确')}

"""
    else:
        items_md = "暂无商品明细\n\n"

    markdown = f"""# 报关单 OCR 识别结果

## 文件信息
- 原文件：`{file_path}`
- 识别时间：{datetime.now().strftime("%Y-%m-%d %H:%M:%S")}

## 提取结果

海关编号：{海关编号}
境外收货人：{境外收货人}
成交方式：{成交方式}
运费：{运费}
保费：{保费}

## 商品明细

{items_md}"""

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(markdown)

    logger.info(f"Markdown 已保存: {output_path}")
    print(f"OCR 结果已保存: {output_path}")
    return output_path
```

---

### 4.5 `prompts/contract_prompt.txt`

```text
你是一名专业的合同信息提取助手。请仔细阅读用户提供的合同文本和图片内容，提取以下关键信息，并以 JSON 格式返回。

请提取的字段：
1. 协议抬头：合同正文顶部的正式标题，不是文件名。
2. 甲方：合同中的甲方名称，填写全称。
3. 乙方：合同中的乙方名称，填写全称。
4. 签约日期：合同正式签署的日期，格式化为 YYYY-MM-DD；如果不确定具体日期，请填写"未明确"并说明原因。
5. 履约日期：合同约定的服务/合作开始和结束日期，或合同有效期；格式化为"YYYY-MM-DD 至 YYYY-MM-DD"；如果是长期/无固定期限，请说明。
6. 内容概述：用 300 字以内概括合同核心内容，包括合作范围、主要权利义务、结算方式等。
7. 备注：其他值得注意的信息，如金额、付款方式、违约责任、争议解决等；没有则填"无"。

输出要求：
- 必须返回合法的 JSON，不要包含 markdown 代码块标记。
- JSON 字段名固定为：协议抬头、甲方、乙方、签约日期、履约日期、内容概述、备注。
- 如果某项无法从合同中确定，值填写"未明确"，并在备注中说明。
```

---

### 4.6 `prompts/customs_prompt.txt`

```text
你是一名专业的中国海关出口报关单信息提取助手。请仔细阅读用户提供的报关单文本和图片内容，提取以下关键信息，并以合法的 JSON 格式返回。

请提取的字段：

1. 海关编号：报关单顶部的海关编号，通常为 18 位数字。
2. 境外收货人：境外收货人名称，如 NEXGO GLOBAL LIMITED。
3. 成交方式：如 FOB、CIF、CFR、CPT、CIP、FCA、EXW 等。
4. 运费：如果成交方式不是 FOB，通常会有运费字段。格式通常为"币制/数值/费率类型"，请提取中间的数值（如 EUR/151/3 → 151）。如果没有填"未明确"。
5. 保费：如果成交方式不是 FOB，通常会有保费字段。格式通常为"币制/数值/费率类型"，请提取中间的数值（如 EUR/0.61/3 → 0.61）。如果没有填"未明确"。
6. 商品明细列表：这是一个数组，每个元素是一个商品项，包含：
   - 项号：商品序号
   - 商品编号：HS 编码
   - 商品名称：商品名称及规格型号中 "|" 分隔的第一段有效商品名称（如 "销售点终端出纳机"、"磁头"）
   - 规格型号：商品名称及规格型号中 "|" 分隔的型号代码部分（如 "P300型"、"N82"、"ME-20"）。只返回型号代码本身，不要包含品牌、用途、分隔符 "|" 或 "附：" 等附加内容。如果无法提取，填"未明确"。
   - 数量：数量数值（如 "3060"）
   - 单位：数量单位（如 "台"）
   - 单价：单价数值
   - 总价：总价数值
   - 币制：如 美元、欧元、日本元 等
   - 最终目的国（地区）：目的国家/地区
   - 原产国（地区）：原产国家/地区

输出要求：
- 必须返回合法的 JSON，不要包含 markdown 代码块标记。
- JSON 字段名固定为：海关编号、境外收货人、成交方式、运费、保费、商品明细。
- 商品明细数组中每个对象的字段名固定为：项号、商品编号、商品名称、规格型号、数量、单位、单价、总价、币制、最终目的国、原产国。
- 如果某项无法从报关单中确定，值填写"未明确"。
- 运费/保费如果存在但格式不是"币制/数值/费率类型"，请尽量提取数值部分。
- 数量及单位如果格式为"数量|单位"，请拆分为数量（数值）和单位（字符串）；如果格式为"3060台"等连续字符串，请尽量拆分。
- 规格型号只保留型号代码，例如：
  - "1|0|用于销售终端刷卡结算|NEXGO牌|P300型|||附：适配器、电池、操作手册" → "P300型"
  - "3|0|用于销售终端刷卡结算|CONLUX|ME-20" → "ME-20"
  - "用于销售终端刷卡结算|NEXGO牌|N82|||附：开关电源、数据线、操作手册" → "N82"
- 如果一个报关单包含多个商品项，请在"商品明细"数组中列出所有项，每个项一行。
```

---

### 4.7 `run_contract_ocr.py`

```python
"""合同 / 报关单 OCR 执行脚本（无需 Prefect）。

用法示例：
    python run_contract_ocr.py --doc_type customs --input_dir "/path/to/pdfs" --limit 10
    python run_contract_ocr.py --doc_type contract --file_path "/path/to/contract.pdf"
"""
import argparse
import logging
import os
import sys
from pathlib import Path
from typing import List, Optional

from dotenv import load_dotenv

from ocr.file_processor import resolve_file_content
from ocr.llm_extractor import extract_contract_info, extract_customs_info
from ocr.markdown_writer import save_contract_markdown, save_customs_markdown

# 默认知识库根目录
_KNOWLEDGE_BASE_DIR = "/mnt/xgd_share/11-业务报表/财务部知识库"
_DEFAULT_OUTPUT_DIR = os.path.join(_KNOWLEDGE_BASE_DIR, "OCR结果")
_DEFAULT_CUSTOMS_INPUT_DIR = os.path.join(
    _KNOWLEDGE_BASE_DIR, "税务资料", "出口报关单整理"
)
_DEFAULT_CONTRACT_TEST_FILE = os.path.join(
    _KNOWLEDGE_BASE_DIR,
    "嘉联合同",
    "应收系统合同",
    "001-20220707-A0520220622014- 雅安市商业银行股份有限公司&四川分公司银行卡收单（含条码支付）业务合作协议-2022.07.05.pdf",
)
_IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".bmp", ".gif", ".webp"}


def _setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%H:%M:%S",
    )


def _list_files(
    file_path: Optional[str], input_dir: Optional[str], limit: Optional[int]
) -> List[str]:
    if file_path:
        return [file_path]

    if input_dir:
        if not os.path.isdir(input_dir):
            raise NotADirectoryError(f"目录不存在: {input_dir}")
        all_files = [
            os.path.join(input_dir, f)
            for f in sorted(os.listdir(input_dir))
            if Path(os.path.join(input_dir, f)).suffix.lower()
            in ({".pdf"} | _IMAGE_EXTENSIONS)
        ]
        return all_files[:limit] if limit else all_files

    return []


def main() -> None:
    parser = argparse.ArgumentParser(description="合同/报关单 OCR 执行脚本")
    parser.add_argument(
        "--doc_type",
        choices=["contract", "customs"],
        default="contract",
        help="文档类型：contract（合同）或 customs（报关单）",
    )
    parser.add_argument("--file_path", help="单个文件路径")
    parser.add_argument("--input_dir", help="待处理文件目录")
    parser.add_argument("--output_dir", help="Markdown 输出目录")
    parser.add_argument("--limit", type=int, help="最多处理前 N 个文件")
    parser.add_argument(
        "--render_all_pages",
        action="store_true",
        help="强制将 PDF 每一页渲染为图片（报关单默认已开启）",
    )
    args = parser.parse_args()

    _setup_logging()
    load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))

    is_contract = args.doc_type == "contract"

    # 默认输出目录
    output_dir = args.output_dir
    if output_dir is None:
        output_dir = (
            _DEFAULT_OUTPUT_DIR
            if is_contract
            else os.path.join(_DEFAULT_OUTPUT_DIR, "报关单")
        )

    # 默认输入
    file_path = args.file_path
    input_dir = args.input_dir
    if file_path is None and input_dir is None:
        if is_contract:
            file_path = _DEFAULT_CONTRACT_TEST_FILE
        else:
            input_dir = _DEFAULT_CUSTOMS_INPUT_DIR

    if file_path and input_dir:
        raise ValueError("file_path 和 input_dir 不能同时指定")

    files = _list_files(file_path, input_dir, args.limit)
    if not files:
        print("没有待处理的文件")
        return

    render_all_pages = args.render_all_pages or not is_contract
    flow_display_name = "合同OCR识别" if is_contract else "报关单OCR识别"

    print("=" * 60)
    print(f"{flow_display_name}启动")
    print(f"待处理文件数: {len(files)}")
    for fp in files:
        print(f"  - {fp}")
    print(f"输出目录: {output_dir}")
    print("=" * 60)

    output_paths: List[str] = []
    errors: List[str] = []

    for idx, fp in enumerate(files, start=1):
        print(f"\n{'=' * 60}")
        print(f"【{idx}/{len(files)}】处理: {fp}")
        print("=" * 60)

        try:
            if not os.path.exists(fp):
                raise FileNotFoundError(f"文件不存在: {fp}")

            ext = Path(fp).suffix.lower()
            if ext not in ({".pdf"} | _IMAGE_EXTENSIONS):
                raise ValueError(f"不支持的文件类型: {ext}")

            print("\n【阶段1】解析文件内容...")
            content = resolve_file_content(fp, render_all_pages=render_all_pages)

            if is_contract:
                print("\n【阶段2】调用大模型提取合同信息...")
                extracted = extract_contract_info(content)
                print("\n【阶段3】保存 OCR 结果...")
                output_path = save_contract_markdown(fp, extracted, output_dir)
            else:
                print("\n【阶段2】调用大模型提取报关单信息...")
                extracted = extract_customs_info(content)
                print("\n【阶段3】保存 OCR 结果...")
                output_path = save_customs_markdown(fp, extracted, output_dir)

            output_paths.append(output_path)

        except Exception as e:
            error_msg = f"处理 {fp} 失败: {e}"
            print(f"\n[ERROR] {error_msg}")
            errors.append(error_msg)
            continue

    print("\n" + "=" * 60)
    print(f"{flow_display_name}完成")
    print(f"成功: {len(output_paths)} 个")
    print(f"失败: {len(errors)} 个")
    for op in output_paths:
        print(f"  输出: {op}")
    if errors:
        print("失败列表:")
        for err in errors:
            print(f"  - {err}")
    print("=" * 60)


if __name__ == "__main__":
    main()
```

---

### 4.8 `requirements.txt`

```text
pymupdf>=1.23.0
Pillow>=9.0.0
httpx>=0.24.0
python-dotenv>=0.19.0
```

---

### 4.9 `.env.example`

```bash
# 必填
OCR_LLM_API_KEY=your_api_key_here

# 可选，默认 https://oneapi.xgd.com/v1
# OCR_LLM_BASE_URL=https://oneapi.xgd.com/v1

# 可选，留空则自动选择模型
# OCR_LLM_MODEL=

# 可选，默认 200 秒
# OCR_LLM_TIMEOUT=200
```

---

## 五、部署步骤

1. 在目标服务器创建目录，复制上述文件。
2. 创建虚拟环境并安装依赖：
   ```bash
   python -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```
3. 复制环境变量文件并填写密钥：
   ```bash
   cp .env.example .env
   # 编辑 .env，填入 OCR_LLM_API_KEY
   ```
4. 确认 `/mnt/xgd_share/11-业务报表/财务部知识库/...` 已挂载，或运行时手动指定路径。
5. 运行：
   ```bash
   # 报关单（默认目录，前 10 个）
   python run_contract_ocr.py --doc_type customs --limit 10

   # 报关单（指定目录）
   python run_contract_ocr.py --doc_type customs --input_dir "/your/path" --output_dir "/your/output"

   # 单个合同
   python run_contract_ocr.py --doc_type contract --file_path "/your/contract.pdf"
   ```

---

## 六、注意事项

- 本包**完全移除 Prefect**，仅保留 OCR 核心能力。
- 默认路径中的共享盘 `\\10.19.9.192\openclaw` 在新服务器上要挂载到 `/mnt/xgd_share/...`。
- 报关单默认会把 PDF 每一页渲染成图片传给 vision 模型，因此调用的是 `qwen3.7-plus`；合同默认使用 `kimi-k2.6-team`。
- 单文件失败不会中断整体流程，失败文件会打印在最后的失败列表中。
- 如果不需要 Hermes 回调，可以直接删掉相关逻辑；当前实现里已不包含 Hermes 通知。
