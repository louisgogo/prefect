# 浏览器提示 "Can't connect to Server API at http://0.0.0.0:4200/api" 的修复

## 原因

服务里把 `PREFECT_API_URL` 设成了 `http://0.0.0.0:4200/api`。  
`0.0.0.0` 只是服务器“监听所有网卡”的地址，**浏览器在你这台电脑上会去连“本机”的 4200**，连不到服务器 10.18.8.191，所以报错。

需要把 API 地址改成**你从浏览器访问时用的地址**（服务器 IP 或域名）。

---

## 立即修复（在服务器上执行）

### 1. 改服务里的 API 地址

```bash
sudo vim /etc/systemd/system/prefect.service
```

找到这一行：

```ini
Environment=PREFECT_API_URL=http://0.0.0.0:4200/api
```

改成（把 `10.18.8.191` 换成你的服务器 IP）：

```ini
Environment=PREFECT_API_URL=http://10.18.8.191:4200/api
```

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
