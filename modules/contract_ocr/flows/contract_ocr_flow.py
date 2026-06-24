"""主流程 - 文件 OCR 识别

对财务部知识库中的合同 PDF/图片进行 OCR，提取关键信息并输出 Markdown。
"""
import os
import sys
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv

from prefect import flow

# 加载项目根目录 .env 文件到环境变量
_flow_file = os.path.abspath(__file__)
_root_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(_flow_file))))
load_dotenv(os.path.join(_root_dir, ".env"))

sys.path.append(_root_dir)

from modules.common.tasks.notify_hermes_task import (
    hermes_flow_completed,
    hermes_flow_failed,
    hermes_flow_started,
)
from modules.contract_ocr.tasks.file_tasks import resolve_file_content_task
from modules.contract_ocr.tasks.llm_tasks import extract_contract_info_task, save_ocr_markdown_task

# 默认知识库根目录
_KNOWLEDGE_BASE_DIR = "/mnt/xgd_share/11-业务报表/财务部知识库"
# 默认 OCR 结果输出目录
_DEFAULT_OUTPUT_DIR = os.path.join(_KNOWLEDGE_BASE_DIR, "OCR结果")
# 本次用于验证的第一个合同文件
_DEFAULT_TEST_FILE = os.path.join(
    _KNOWLEDGE_BASE_DIR,
    "嘉联合同",
    "应收系统合同",
    "001-20220707-A0520220622014- 雅安市商业银行股份有限公司&四川分公司银行卡收单（含条码支付）业务合作协议-2022.07.05.pdf",
)


@flow(name="contract_ocr_flow", log_prints=True)
def contract_ocr_flow(
    file_path: Optional[str] = None,
    output_dir: Optional[str] = None,
) -> str:
    """合同 OCR 识别主流程。

    Args:
        file_path: 待识别的合同文件路径（pdf/png/jpg/jpeg）。
                   默认为嘉联合同/应收系统合同下的第一个 PDF。
        output_dir: OCR 结果 Markdown 输出目录，默认保存到知识库/OCR结果。

    Returns:
        生成的 Markdown 文件路径。
    """
    file_path = file_path or _DEFAULT_TEST_FILE
    output_dir = output_dir or _DEFAULT_OUTPUT_DIR

    print("=" * 60)
    print("合同 OCR 识别流程启动")
    print(f"目标文件: {file_path}")
    print(f"输出目录: {output_dir}")
    print("=" * 60)

    # 发送开始通知
    hermes_flow_started(flow_name="合同OCR识别")

    try:
        # 校验文件
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"文件不存在: {file_path}")

        ext = Path(file_path).suffix.lower()
        if ext not in {".pdf", ".png", ".jpg", ".jpeg", ".bmp", ".gif", ".webp"}:
            raise ValueError(f"不支持的文件类型: {ext}")

        # 阶段1：解析文件内容（PDF 提取文本 + 图片页渲染）
        print("\n【阶段1】解析文件内容...")
        content = resolve_file_content_task(file_path=file_path)

        # 阶段2：调用 LLM 提取合同关键信息
        print("\n【阶段2】调用大模型提取合同信息...")
        extracted = extract_contract_info_task(content=content)

        print("\n【阶段2】提取结果:")
        for key, value in extracted.items():
            print(f"  {key}: {value}")

        # 阶段3：保存 Markdown 结果
        print("\n【阶段3】保存 OCR 结果...")
        output_path = save_ocr_markdown_task(
            file_path=file_path,
            extracted=extracted,
            output_dir=output_dir,
        )

        print("\n" + "=" * 60)
        print("合同 OCR 识别流程完成")
        print(f"输出文件: {output_path}")
        print("=" * 60)

        # 发送完成通知
        hermes_flow_completed(
            flow_name="合同OCR识别",
            payload={
                "file_path": file_path,
                "output_path": output_path,
                "extracted": extracted,
                "summary": f"已提取 {len(extracted)} 个字段，输出至 {output_path}",
            },
        )

        return output_path

    except Exception as e:
        print(f"\n[ERROR] 合同 OCR 识别流程失败: {e}")
        hermes_flow_failed(
            flow_name="合同OCR识别",
            error_message=str(e),
            error_type=type(e).__name__,
            payload={"file_path": file_path},
        )
        raise


if __name__ == "__main__":
    # 本地直接运行测试
    contract_ocr_flow()
