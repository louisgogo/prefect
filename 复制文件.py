"""自动复制 flows、tasks、utils 文件夹到部署包"""
import shutil
import os


def copy_folders():
    """复制必要的文件夹到部署包"""
    # 获取当前脚本所在目录（部署包目录）
    package_dir = os.path.dirname(os.path.abspath(__file__))
    # 获取父目录（业务线核算目录）
    parent_dir = os.path.dirname(package_dir)

    folders_to_copy = ['flows', 'tasks', 'utils']

    print("=" * 60)
    print("正在复制文件到部署包...")
    print("=" * 60)

    for folder in folders_to_copy:
        src = os.path.join(parent_dir, folder)
        dst = os.path.join(package_dir, folder)

        if os.path.exists(src):
            # 如果目标文件夹存在，先删除
            if os.path.exists(dst):
                shutil.rmtree(dst)
                print(f"已删除旧的 {folder}/")

            # 复制文件夹
            shutil.copytree(src, dst)
            print(
                f"✓ 已复制 {folder}/ -> {os.path.basename(package_dir)}/{folder}/")
        else:
            print(f"✗ 未找到源文件夹: {src}")

    print("=" * 60)
    print("文件复制完成！")
    print("=" * 60)
    print(f"\n部署包位置: {package_dir}")
    print("\n包含内容：")
    print("  ✓ flows/ (Flow 定义)")
    print("  ✓ tasks/ (Task 定义)")
    print("  ✓ utils/ (工具函数)")
    print("  ✓ deploy_local.py (本地测试部署)")
    print("  ✓ deploy_production.py (生产环境部署)")
    print("  ✓ test_flow.py (直接测试)")
    print("  ✓ requirements.txt (依赖包)")
    print("  ✓ README_部署说明.md (部署说明)")


if __name__ == "__main__":
    copy_folders()
