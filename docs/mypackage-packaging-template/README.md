# mypackage 打包模板说明

将本目录下的 `pyproject.toml` 或 `setup.py` 复制到**你自己的 mypackage 项目根目录**（与 `mypackage` 包目录同级），然后：

- 开发/本地安装：`pip install -e .`
- 打安装包：`pip install build && python -m build`，得到 `dist/mypackage-0.1.0.tar.gz`

各项目在 requirements.txt 里写：

- 本地路径：`-e /path/to/mypackage`
- Git：`git+https://.../mypackage.git@main`
- 或安装包：`mypackage==0.1.0`

详见：`多项目共用mypackage_打包与使用.md`
