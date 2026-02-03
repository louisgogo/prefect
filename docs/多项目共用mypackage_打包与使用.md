# 多项目共用 mypackage：如何打包与使用

多个程序（Prefect、其他脚本/服务）都依赖同一个 `mypackage` 时，推荐**把 mypackage 做成可安装的 Python 包**，各项目用 `pip install` 安装，统一维护、避免重复代码和 PYTHONPATH 配置。

---

## 一、推荐做法概览

| 步骤 | 说明 |
|------|------|
| 1. 打包 mypackage | 给 mypackage 加上 `pyproject.toml`（或 `setup.py`），做成标准 Python 包 |
| 2. 单独仓库或目录 | 把 mypackage 放在独立 Git 仓库或公司内共享目录 |
| 3. 各项目安装 | Prefect 等项目在 `requirements.txt` 里写对 mypackage 的安装方式（本地路径 / Git 地址 / 私有 PyPI） |
| 4. 服务器部署 | 服务器上 clone/同步 mypackage，在各项目 venv 里 `pip install` 即可，无需 PYTHONPATH |

这样：**一个包、多处安装**，版本统一、部署简单。

---

## 二、mypackage 目录结构示例

保证包名是 `mypackage`，且能被 `import mypackage.utilities`、`import mypackage.mapping` 使用。示例结构：

```text
mypackage/                    # 项目根目录（可单独建 Git 仓库）
├── pyproject.toml            # 包声明与依赖（推荐）
├── setup.py                  # 若无 pyproject.toml 则保留
├── mypackage/                # 包名目录，与 import mypackage 对应
│   ├── __init__.py
│   ├── utilities.py          # connect_to_db, cal_person_weight, add_data, val_dist, delete_data_add_data_by_DateRange 等
│   └── mapping.py            # combined_table_mapping, combined_column_mapping
├── README.md
└── requirements.txt          # 可选：mypackage 自己的依赖
```

说明：

- 最外层 `mypackage/` 是**项目根**（可随意命名，如 `mypackage-repo`）。
- 内层 `mypackage/` 是**包名**，必须与 `import mypackage` 一致，且含有 `__init__.py`。

---

## 三、打包方式一：pyproject.toml（推荐）

在 **mypackage 项目根目录** 新建 `pyproject.toml`：

```toml
[build-system]
requires = ["setuptools>=61.0"]
build-backend = "setuptools.build_meta"

[project]
name = "mypackage"
version = "0.1.0"
description = "内部公共工具与映射"
readme = "README.md"
requires-python = ">=3.8"

[project.optional-dependencies]
dev = ["pytest"]

[tool.setuptools.packages.find]
where = ["."]
include = ["mypackage*"]
```

若包就在当前目录下的 `mypackage` 文件夹，上面配置即可。打安装包：

```bash
cd /path/to/mypackage-repo
pip install build
python -m build
```

会生成 `dist/mypackage-0.1.0.tar.gz`，可供 `pip install dist/mypackage-0.1.0.tar.gz` 或上传私有 PyPI。

---

## 四、打包方式二：setup.py（传统方式）

在 **mypackage 项目根目录** 新建 `setup.py`：

```python
from setuptools import setup, find_packages

setup(
    name="mypackage",
    version="0.1.0",
    packages=find_packages(),
    python_requires=">=3.8",
    install_requires=[
        "pandas>=1.5.0",
        "psycopg2-binary>=2.9.0",
        "sqlalchemy>=1.4.0",
        # 按需补充 mypackage 的依赖
    ],
)
```

安装到当前环境（开发时可编辑安装）：

```bash
cd /path/to/mypackage-repo
pip install -e .
```

---

## 五、各项目如何引用 mypackage

多个程序都用同一个包时，**不要在各自仓库里拷贝代码**，而是通过依赖声明安装。

### 1. 从本地路径安装（开发或服务器同机部署）

若 mypackage 在服务器上的路径为 `/root/mypackage`（即该目录下有 `mypackage` 子包和 `setup.py`/`pyproject.toml`）：

在 **Prefect 项目**的 `requirements.txt` 里加一行：

```text
# 其他依赖...
prefect>=2.0.0
pandas>=1.5.0
# ...

# 内部包：本地路径（服务器上需存在该路径）
-e /root/mypackage
```

或安装时直接：

```bash
pip install -e /root/mypackage
```

其他项目同理，把 `/root/mypackage` 换成各自环境里 mypackage 的路径即可。

### 2. 从 Git 仓库安装（多项目、多机器统一）

若 mypackage 在 Git 仓库里：

```text
# requirements.txt 中
git+https://your-git-server/your-org/mypackage.git@main
# 或指定 tag
# git+https://your-git-server/your-org/mypackage.git@v0.1.0
```

或：

```bash
pip install "git+https://your-git-server/your-org/mypackage.git@main"
```

这样 Prefect 和其他项目都写同一行依赖，版本由 Git 分支/tag 控制。

### 3. 从打好的 wheel/sdist 安装（内网或私有 PyPI）

若已执行 `python -m build` 得到 `dist/mypackage-0.1.0.tar.gz`：

- 拷贝到服务器后：`pip install /path/to/mypackage-0.1.0.tar.gz`
- 或上传到私有 PyPI 后：`pip install mypackage==0.1.0`

各项目在 `requirements.txt` 里写 `mypackage==0.1.0` 即可。

---

## 六、Prefect 项目中的具体用法

1. **在 Prefect 的 requirements.txt 里声明 mypackage**（任选一种）：
   - 本地可编辑安装：`-e /path/to/mypackage`
   - Git：`git+https://.../mypackage.git@main`
   - 私有 PyPI：`mypackage==0.1.0`

2. **服务器部署时**：
   - 若用 Git：先 clone mypackage 到服务器，再在 Prefect 的 venv 里 `pip install -e /root/mypackage` 或 `pip install -r requirements.txt`（若 requirements 里已写 Git 地址）。
   - 若用本地路径：保证该路径存在，然后 `pip install -r requirements.txt`。

3. **不再需要**在 systemd 或脚本里设置 `PYTHONPATH`，只要 venv 里正确安装了 mypackage 即可。

---

## 七、小结

| 场景 | 做法 |
|------|------|
| 多项目共用 mypackage | 把 mypackage 做成标准包（pyproject.toml 或 setup.py），各项目用 pip 安装 |
| 打包 mypackage | 在 mypackage 项目根目录加 `pyproject.toml` 或 `setup.py`，`pip install -e .` 或 `python -m build` |
| 各项目安装方式 | requirements.txt 里写：本地 `-e /path/to/mypackage`，或 Git `git+https://...`，或版本号 `mypackage==0.1.0` |
| 服务器 | 同步 mypackage 代码或 Git clone，在对应 venv 里 `pip install -r requirements.txt`，无需 PYTHONPATH |

按上述方式打包并在各项目中用 pip 引用，即可在多程序间统一使用同一包。
