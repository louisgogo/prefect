# Prefect 流程部署说明

## 部署方式

### 1. 本地开发部署（`deploy_local.py`）

用于本地测试，连接到本地 Prefect Server。

**使用步骤：**
```bash
# 1. 启动本地 Prefect Server
prefect server start

# 2. 运行部署脚本
python deploy_local.py
```

**特点：**
- 连接到本地 Prefect Server (http://127.0.0.1:4200)
- 适合开发和测试
- 流程会持续运行，保持连接

### 2. 生产环境部署（`deploy_production.py`）

用于生产环境，连接到生产 Prefect Server。

**使用步骤：**
```bash
# 1. 配置生产环境 Prefect Server 地址
prefect config set PREFECT_API_URL=http://your-production-server:4200/api

# 2. 运行部署脚本
python deploy_production.py
```

**特点：**
- 连接到生产 Prefect Server
- 带默认参数和计划执行
- 适合生产环境自动化

### 3. 从本地推送到远程服务器（`deploy_to_server.py`）

从本地编辑器推送流程到远程 Prefect Server。

**使用步骤：**

#### 方式 A：使用环境变量
```bash
# Windows (PowerShell)
$env:PREFECT_API_URL="http://your-server:4200/api"
python deploy_to_server.py

# Windows (CMD)
set PREFECT_API_URL=http://your-server:4200/api
python deploy_to_server.py

# Linux/Mac
export PREFECT_API_URL=http://your-server:4200/api
python deploy_to_server.py
```

#### 方式 B：使用 Prefect 配置
```bash
# 设置 Prefect API URL
prefect config set PREFECT_API_URL=http://your-server:4200/api

# 运行部署脚本
python deploy_to_server.py
```

#### 方式 C：在代码中直接指定
修改 `deploy_to_server.py`，在脚本开头添加：
```python
import os
os.environ["PREFECT_API_URL"] = "http://your-server:4200/api"
```

**特点：**
- 从本地编辑器直接推送
- 代码版本控制在本地
- 灵活部署，随时更新

## 配置说明

### Prefect API URL 配置

Prefect 会按以下顺序查找 API URL：

1. **环境变量** `PREFECT_API_URL`
2. **Prefect 配置文件** `~/.prefect/config.toml`
3. **默认值** `http://127.0.0.1:4200/api`（本地）

### 查看当前配置

```bash
# 查看当前 API URL
prefect config view

# 查看所有配置
prefect config get PREFECT_API_URL
```

### 设置配置

```bash
# 设置 API URL
prefect config set PREFECT_API_URL=http://your-server:4200/api

# 重置为默认值
prefect config unset PREFECT_API_URL
```

## 网络要求

从本地推送到远程服务器需要：

1. **网络连通性**：本地机器可以访问远程 Prefect Server
2. **防火墙**：确保端口 4200（或自定义端口）开放
3. **认证**：如果服务器需要认证，配置相应的认证信息

## 验证部署

部署完成后，可以通过以下方式验证：

1. **Prefect UI**：访问 `http://your-server:4200`，查看流程是否已注册
2. **命令行**：
   ```bash
   prefect flow ls
   prefect deployment ls
   ```

## 注意事项

1. **代码同步**：确保本地代码与服务器环境兼容
2. **依赖安装**：确保服务器上已安装所有必要的 Python 包
3. **路径配置**：确保服务器上的文件路径（如 Excel 文件路径）正确
4. **数据库连接**：确保服务器可以访问数据库

## 常见问题

### Q: 如何知道流程是否成功推送到服务器？

A: 在 Prefect UI 中查看流程列表，如果看到你的流程，说明推送成功。

### Q: 推送后如何更新流程？

A: 修改本地代码后，重新运行部署脚本即可。Prefect 会更新流程定义。

### Q: 可以同时连接到多个服务器吗？

A: 可以，但需要切换 `PREFECT_API_URL` 环境变量或配置。

### Q: 流程推送后在哪里执行？

A: 流程在 Prefect Server 上注册，但实际执行在配置的 Worker 上。需要确保 Worker 已启动并连接到同一个 Server。
