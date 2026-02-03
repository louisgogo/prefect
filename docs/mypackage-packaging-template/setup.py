# 若不想用 pyproject.toml，可只用此文件。复制到 mypackage 项目根目录。
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
    ],
)
