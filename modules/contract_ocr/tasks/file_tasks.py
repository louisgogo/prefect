"""合同 OCR - 文件处理 Tasks

负责：
- PDF 文本提取
- 扫描页 / 图片页检测与渲染
- 图片文件加载
"""
import base64
import io
import os
from pathlib import Path
from typing import Any, Dict, List

import fitz  # PyMuPDF
from PIL import Image

from prefect import task

# 当一页提取到的文本字符数小于该阈值时，认为该页是扫描/图片页，需要渲染为图片
_MIN_TEXT_CHARS_PER_PAGE = 100
# 渲染图片 DPI（越高越清晰，但 base64 越大）
_RENDER_DPI = 200
# 单张图片最大边长（防止过大）
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
        # 统一转换为 RGB，避免 CMYK 等格式问题
        if img.mode != "RGB":
            img = img.convert("RGB")
        img = _resize_image(img)
        return _image_to_base64(img, fmt="PNG")


@task(name="load_image", log_prints=True)
def load_image_task(file_path: str) -> Dict[str, Any]:
    """加载图片文件。

    Returns:
        {
            "file_path": str,
            "text": "",
            "image_pages": [
                {"page_no": 1, "base64": "data:image/png;base64,..."}
            ]
        }
    """
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


@task(name="extract_pdf_content", log_prints=True)
def extract_pdf_content_task(file_path: str) -> Dict[str, Any]:
    """提取 PDF 文本，并将疑似扫描页渲染为图片。

    Returns:
        {
            "file_path": str,
            "text": "合并后的 PDF 文本",
            "image_pages": [
                {"page_no": int, "base64": "data:image/png;base64,..."}
            ]
        }
    """
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

        # 如果该页文本太少，认为是扫描页/图片页，渲染为图片
        if len(text_stripped) < _MIN_TEXT_CHARS_PER_PAGE:
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
                print(f"PDF 第 {page_no + 1} 页文本较少，已渲染为图片用于识别")
            except Exception as e:
                print(f"PDF 第 {page_no + 1} 页渲染失败: {e}")

    doc.close()

    merged_text = "\n\n".join(all_text_parts)
    print(
        f"PDF 处理完成: 共 {len(all_text_parts)} 页, "
        f"总文本字符 {len(merged_text)}, 图片页 {len(image_pages)} 页"
    )
    return {
        "file_path": file_path,
        "text": merged_text,
        "image_pages": image_pages,
    }


@task(name="resolve_file_content", log_prints=True)
def resolve_file_content_task(file_path: str) -> Dict[str, Any]:
    """根据扩展名自动选择 PDF 或图片处理方式。"""
    ext = Path(file_path).suffix.lower()
    if ext == ".pdf":
        return extract_pdf_content_task.fn(file_path)
    if ext in {".png", ".jpg", ".jpeg", ".bmp", ".gif", ".webp"}:
        return load_image_task.fn(file_path)
    raise ValueError(f"不支持的文件类型: {ext}，仅支持 pdf/png/jpg/jpeg")
