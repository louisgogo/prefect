# FONE 往来数据刷新子流程

`fone_recon_flow` 按显式年月执行 FONE 内容库中的
`0501-获取ERP科目余额表-WebApi`，并将结果推送到 MySQL
`Fone2BI_IntCommCheck`。远程 deployment 名称为 `子流程-从FONE获取往来数据`。

## 长任务与 504 处理

AIHub 前置 nginx 的请求等待时间约为 60 秒，而 FONE 脚本通常需要数分钟。HTTP 504
只表示网关停止等待，不能据此判断脚本失败，也不得自动重复提交。

流程收到 504 后会：

1. 使用原始 `taskId` 调用 FONE `Script/BackendRunning`，将任务确认到后台运行。
2. 轮询 `Fone2BI_IntCommCheck` 中请求期间的行数及 ID 范围。
3. 目标期间非空、相对执行前发生变化，并连续两次保持稳定后，才判定流程成功。
4. 超过验证期限仍未刷新时失败，保留 `taskId` 和目标表状态供人工排查，不自动重试。

默认最多等待 900 秒，每 15 秒检查一次。可通过 worker 环境变量
`FONE_RECON_VERIFY_TIMEOUT_SECONDS` 和 `FONE_RECON_VERIFY_INTERVAL_SECONDS` 调整。

## 运行安全

- 必须明确填写目标 `year` 和 `month`；不要依赖默认上月处理高影响补跑。
- Bearer token 通过 `AIHUB_FONE_API_TOKEN` 提供。
- `appUserId` 由 AIHub 第三方绑定自动注入，请求体不得重复传入。
- 人工重跑前先检查目标期间数据和 FONE 运行记录，避免在 504 后启动并发刷新。
