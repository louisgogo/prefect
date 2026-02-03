# 浏览器提示 "Can't connect to Server API at http://127.0.0.1:4200/api" 的修复

## 原因

- 你在浏览器里访问 **http://10.18.8.191:4200**，但页面里的 API 被配置成 **http://127.0.0.1:4200/api**。
- **127.0.0.1** 在浏览器所在电脑上指“本机”，不是服务器，所以浏览器连不到 API，报错。
- 需要在**启动 Prefect Server 时**设置 **PREFECT_UI_API_URL**，让 UI 使用**服务器 IP** 作为 API 地址。

---

## 立即修复（在服务器上执行）

### 1. 改 Prefect Server 服务里的 UI API 地址

编辑你用来跑 **Prefect Server**（`prefect server start`）的那个服务，例如：

```bash
sudo vim /etc/systemd/system/prefect.service
# 或
sudo vim /etc/systemd/system/prefect-server.service
```

在 `[Service]` 里**增加**（若已有可改）这一行（把 `10.18.8.191` 换成你的服务器 IP）：

```ini
Environment=PREFECT_UI_API_URL=http://10.18.8.191:4200/api
```

若是旧版或之前按 0.0.0.0 配置的，把错误地址改成上面这行即可。

保存退出。

### 2. 重载并重启服务

```bash
sudo systemctl daemon-reload
sudo systemctl restart prefect
sudo systemctl status prefect
```

### 3. 确认防火墙已放行 4200

```bash
sudo firewall-cmd --permanent --add-port=4200/tcp
sudo firewall-cmd --reload
```

### 4. 浏览器重新访问

打开：**http://10.18.8.191:4200**（不要加 `/dashboard`，根路径即可）。

---

## 以后重装/新机：用配置文件

在 `prefect-service.conf` 里加上（与 `setup-prefect-service.sh` 同目录）：

```bash
PREFECT_SERVER_PUBLIC_HOST="10.18.8.191"
```

再执行一次 `setup-prefect-service.sh`，生成的服务就会用正确的 API 地址。
