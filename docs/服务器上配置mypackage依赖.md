# 服务器上配置 mypackage 依赖（解决 ModuleNotFoundError: No module named 'mypackage'）

## 重要：必须装在 systemd 用的那个环境里

Prefect 的 systemd 服务（如 `prefect.service` 或 `prefect-workers.service`）里写的是 **`/root/prefect/venv/bin/python`**。  
**mypackage 必须安装进这个 venv**，不能装在别的环境（例如你 SSH 登录后没 `source venv/bin/activate` 就 `pip install`，会装到系统 Python，服务仍然找不到）。

正确做法：用该 venv 的 pip 安装（任选一种）：

```bash
# 用 wheel 安装（推荐，速度快）
/root/prefect/venv/bin/pip install /path/to/mypackage-0.1.0-py3-none-any.whl

# 或用源码目录可编辑安装
/root/prefect/venv/bin/pip install -e /path/to/mypackage
```

安装完成后执行：`sudo systemctl restart prefect`（或你实际使用的服务名）。

---

## 报错原因

日志中的：

```text
ModuleNotFoundError: No module named 'mypackage'
```

表示 **Python 在服务器上找不到 `mypackage` 这个包**。

- Prefect 项目里的多个 task（如 `data_preparation_tasks`、`data_import_tasks`、`profit_tasks` 等）都依赖 `mypackage.utilities`、`mypackage.mapping`。
- `mypackage` 是你们自己的业务包，**不在** Prefect 仓库里，也不在 `requirements.txt` 里。
- 若服务器上没有部署 `mypackage` 或没有把它加入 Python 路径，启动 Workers 时就会在导入阶段报上述错误。

---

## 解决思路

在**服务器**上让 Python 能导入 `mypackage`，任选其一即可。

### 方式一：把 mypackage 放到服务器并加入 PYTHONPATH（推荐）

1. 在服务器上准备好 `mypackage` 的代码（例如单独一个目录，或和 prefect 同级的目录）：
   - 若 `mypackage` 在另一个 Git 仓库，可在服务器上 clone 到固定路径，例如：`/root/mypackage`。
   - 目录结构要保证 `import mypackage` 能找到，即该路径下要有 `mypackage` 包（含 `__init__.py` 等）。

2. 在 **prefect-workers** 的 systemd 里把该路径加入 `PYTHONPATH`，例如 mypackage 在 `/root/mypackage` 的**上一级**（即 `/root` 下有 `mypackage` 目录）：

   ```ini
   Environment=PREFECT_API_URL=http://127.0.0.1:4200/api
   Environment=PYTHONPATH=/root
   ```

   若 mypackage 本身就在 `/root/mypackage`（即包名就是目录名），则应把其**父目录**加入 PYTHONPATH：

   ```ini
   Environment=PYTHONPATH=/root
   ```

   这样 `import mypackage` 会去 `/root` 下找 `mypackage`。

3. 重载并重启 Workers：

   ```bash
   sudo systemctl daemon-reload
   sudo systemctl restart prefect-workers
   ```

### 方式二：用 pip 以“可编辑”方式安装 mypackage

若 `mypackage` 有 `setup.py` 或 `pyproject.toml`，在服务器上可以：

```bash
cd /root/prefect
source venv/bin/activate
pip install -e /path/to/mypackage
```

把 `/path/to/mypackage` 换成服务器上 mypackage 源码所在路径。这样 venv 里就能 `import mypackage`，无需改 systemd。

### 方式三：和 Prefect 同仓库且同目录

若你们把 `mypackage` 放在 Prefect 项目根目录下（例如 `/root/prefect/mypackage`），则 Workers 的 `WorkingDirectory=/root/prefect` 已在该目录下执行，理论上当前目录会在 `sys.path` 里。若仍报错，可在 systemd 里显式设置：

```ini
Environment=PYTHONPATH=/root/prefect
```

---

## 小结

| 情况 | 做法 |
|------|------|
| mypackage 在服务器单独目录（如 `/root/mypackage`） | 在 prefect-workers 的 unit 里加 `Environment=PYTHONPATH=/root`（或包含 mypackage 的父目录），然后 `systemctl daemon-reload && systemctl restart prefect-workers` |
| mypackage 有 setup.py | 在服务器 venv 里 `pip install -e /path/to/mypackage` |
| mypackage 在 prefect 项目目录下 | 设置 `PYTHONPATH=/root/prefect` 或保证运行目录为项目根目录 |

按上述任一种方式配置后，再启动 prefect-workers，`ModuleNotFoundError: No module named 'mypackage'` 就会消失。
