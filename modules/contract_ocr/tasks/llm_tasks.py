"""合同 OCR - LLM 调用 Tasks

使用 httpx 调用 OneAPI / OpenAI 兼容接口，支持文本 + vision 图片输入。
"""
import json
import os
import re
from datetime import datetime
from typing import Any, Dict, List

import httpx

from prefect import get_run_logger, task

_DEFAULT_BASE_URL = "https://oneapi.xgd.com/v1"
# 默认文本模型（适用于纯文本 PDF）
_DEFAULT_TEXT_MODEL = "kimi-k2.6-team"
# 默认 vision 模型（适用于含图片页/扫描页的 PDF）
_DEFAULT_VISION_MODEL = "qwen3.7-plus"
_DEFAULT_TIMEOUT = 200
_MAX_IMAGE_PAGES = 10  # 防止一次性传太多图片


def _load_prompt() -> str:
    """加载合同提取 prompt。"""
    prompt_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "prompts",
        "contract_prompt.txt",
    )
    with open(prompt_path, "r", encoding="utf-8") as f:
        return f.read().strip()


def _build_messages(text: str, image_pages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """构造 OpenAI 兼容 messages。"""
    system_message = {
        "role": "system",
        "content": "你是一个严谨的合同信息提取助手，只输出合法 JSON。",
    }

    user_content: List[Dict[str, Any]] = []

    # 文本部分
    user_content.append(
        {
            "type": "text",
            "text": f"以下是从合同中提取到的文本内容：\n\n{text}\n\n{_load_prompt()}",
        }
    )

    # 图片部分：只取前 N 页，避免超出模型上下文
    for img in image_pages[:_MAX_IMAGE_PAGES]:
        user_content.append(
            {
                "type": "image_url",
                "image_url": {"url": img["base64"], "detail": "high"},
            }
        )

    return [system_message, {"role": "user", "content": user_content}]


def _extract_json(text: str) -> Dict[str, Any]:
    """从模型返回文本中提取 JSON。"""
    # 优先匹配 markdown json 代码块
    match = re.search(r"```(?:json)?\s*(\{.*\})\s*```", text, re.DOTALL)
    if match:
        json_str = match.group(1)
    else:
        # 否则找第一个 { 到最后一个 }
        start = text.find("{")
        end = text.rfind("}")
        if start == -1 or end == -1 or end <= start:
            raise ValueError(f"未在模型输出中找到 JSON: {text[:200]}")
        json_str = text[start : end + 1]

    try:
        return json.loads(json_str)
    except json.JSONDecodeError as e:
        raise ValueError(f"JSON 解析失败: {e}\n文本片段: {json_str[:500]}")


@task(name="extract_contract_info", log_prints=True, retries=2, retry_delay_seconds=5)
def extract_contract_info_task(content: Dict[str, Any]) -> Dict[str, Any]:
    """调用大模型提取合同关键信息。

    Args:
        content: resolve_file_content_task 返回的字典

    Returns:
        {
            "协议抬头": "...",
            "甲方": "...",
            "乙方": "...",
            "签约日期": "...",
            "履约日期": "...",
            "内容概述": "...",
            "备注": "..."
        }
    """
    logger = get_run_logger()
    text = content.get("text", "")
    image_pages = content.get("image_pages", [])

    api_key = os.environ.get("OCR_LLM_API_KEY")
    base_url = os.environ.get("OCR_LLM_BASE_URL", _DEFAULT_BASE_URL).rstrip("/")
    has_images = bool(image_pages)
    default_model = _DEFAULT_VISION_MODEL if has_images else _DEFAULT_TEXT_MODEL
    model = os.environ.get("OCR_LLM_MODEL") or default_model
    timeout = int(os.environ.get("OCR_LLM_TIMEOUT", _DEFAULT_TIMEOUT))

    if not api_key:
        raise ValueError("OCR_LLM_API_KEY 环境变量未配置")

    messages = _build_messages(text, image_pages)

    payload = {
        "model": model,
        "messages": messages,
        "temperature": 0.1,
        "max_tokens": 4000,
    }

    logger.info(
        f"调用 LLM: model={model}, base_url={base_url}, "
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
    assistant_message = result.get("choices", [{}])[0].get("message", {}).get("content", "")

    if not assistant_message:
        raise ValueError("LLM 返回空内容")

    logger.info(f"LLM 原始输出长度: {len(assistant_message)}")
    extracted = _extract_json(assistant_message)

    # 确保必要字段存在
    required_fields = [
        "协议抬头",
        "甲方",
        "乙方",
        "签约日期",
        "履约日期",
        "内容概述",
        "备注",
    ]
    for field in required_fields:
        extracted.setdefault(field, "未明确")

    return extracted


@task(name="save_ocr_markdown", log_prints=True)
def save_ocr_markdown_task(
    file_path: str,
    extracted: Dict[str, Any],
    output_dir: str,
) -> str:
    """将提取结果保存为 Markdown 文件。"""
    logger = get_run_logger()

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
