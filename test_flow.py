"""直接测试 Flow（不使用 Prefect Server）"""
import sys
import os

# 添加当前目录到路径
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(current_dir)

from modules import business_line_profit_flow

if __name__ == "__main__":
    print("=" * 60)
    print("业务线数据计算流程 - 直接测试")
    print("=" * 60)
    print("\n注意：此方式直接运行，不会在 Prefect UI 中显示")
    print("适合快速测试代码逻辑\n")
    
    try:
        # 测试处理 2025 年 12 月数据
        # 可以根据需要修改参数
        print("开始执行...")
        business_line_profit_flow(year=2025, month=12)
        print("\n" + "=" * 60)
        print("测试完成！")
        print("=" * 60)
    except Exception as e:
        print(f"\n测试失败: {e}")
        import traceback
        traceback.print_exc()
