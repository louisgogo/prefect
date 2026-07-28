# FONE 收入费用明细刷新子流程

`fone_income_expense_refresh_flow` 用于按显式会计期间顺序刷新 FONE 收入成本明细和费用明细。远程及生产部署名为 `子流程-FONE收入费用明细刷新`，仅供手工触发，不配置自动计划。

## 参数

- `year`：四位年份，必填。
- `month`：月份 `1-12`，必填。
- `permission_user`：拥有 XGD_MRPT 法人组织权限的 FONE 用户。可在触发时传入，也可通过 worker 环境变量 `FONE_DETAIL_PERMISSION_USER` 提供。

Worker 还必须通过环境变量 `AIHUB_FONE_API_TOKEN` 配置 AIHub FONE 代理 Bearer token。该配置同时供 `fone_recon_flow` 和本流程使用。token 不应作为 Prefect 参数传入，也不得写入代码或日志。

FONE API 登录账号本身不具备明细脚本所需的法人权限。若未配置 `permission_user`，流程会在读取或清理业务表之前失败。

## 执行顺序与目标表

流程直接通过 AIHub FONE 代理读取脚本定义并执行，不再调用 FONE 登录接口获取 ticket。收入脚本校验成功后，再执行费用脚本：

1. 收入成本明细：`fone_db.FONE_MRPT_AC_OffLineFormat`。
2. 费用线下底稿：`fone_db.FONE_MRPT_FY_OffLineFormat`。
3. FONE 费用单据明细：`fone_db.FONE_MRPT_FY_OffLineDetail`。

FONE 脚本定义在每次运行时从内容库读取。仓库只保存内容 ID 和编译逻辑，不保存脚本正文及其中的环境凭据。

## 校验与失败处理

每个阶段执行前读取目标表行数和 ID 范围，脚本返回成功后立即回读并检查：

- 表非空；
- 全表只有请求的年月；
- 刷新前后的行数与 ID 签名发生变化。

收入校验通过后才会开始费用刷新。脚本执行任务不配置自动重试，因为这些 FONE 脚本会删除并重建整张表；HTTP 超时代表执行状态未知，必须先检查 FONE 运行记录和目标表，再决定是否手工重跑。

这些目标表不是按月份分区保存，后续其他月份的刷新会覆盖当前月份。高影响运行必须填写明确年月，并在流程完成后及时确认 Prefect Flow/Task Runs 和数据库校验结果。
