# 业报基础数据更新

`子流程-业报基础数据更新` 统一替代旧补充数据 notebook，默认每天 06:00（Asia/Shanghai）更新供应商，也可由业报收集编辑人员在“数据同步”中更新全部或单个数据集。客户、物料、研发项目和收单指标只有在对应凭据、Python 依赖和系统驱动配置完成后才应手工触发。

## 数据集

| 参数代码 | 来源 | 目标 |
|---|---|---|
| `customer` | SQL Server `V_XGD_BD_CUSTOMER`，组织 1000 | `dim_customer_info` |
| `material` | SQL Server `V_XGD_BD_MATERIAL`，组织 1000/1700/1200 | `dim_material_master` |
| `rd_project` | SQL Server `V_XGD_BD_YFPROJ` | `dim_rd_code` |
| `supplier` | 金蝶 `BD_Supplier`，使用组织 1000 | `dim_supplier_info` |
| `acquiring_metrics` | 五张 Oracle `T_JL_*` 表 | 同名小写 PostgreSQL 表 |

客户、物料和研发项目继续保留 `C99 / 不分客户`、`PD99 / 不分产品`、`无 / 公共部门`。物料编码跨组织重复时依次优先使用 1000、1700、1200。

## Worker 配置

所有凭据只配置在 Prefect Worker 环境中：

- `BUSINESS_DATA_SQLSERVER_CONNECTION_STRING`
- `BUSINESS_DATA_ORACLE_USER`
- `BUSINESS_DATA_ORACLE_PASSWORD`
- `BUSINESS_DATA_ORACLE_DSN`
- `BUSINESS_DATA_FINANCE_DATABASE_URL`（未配置时兼容 `KINGDEE_VOUCHER_DATABASE_URL` 或项目默认财务连接）
- `XGD_TOKEN`（兼容 `AIHUB_FONE_API_TOKEN`）
- `KINGDEE_VOUCHER_BASE_URL`（可选）
- `BUSINESS_DATA_MIN_ROW_RATIO`（可选，默认 `0.5`；也可追加数据集大写后缀单独设置）

Worker 还需要 Python 包 `oracledb`、`pyodbc`，以及与连接字符串一致的 SQL Server ODBC Driver。启用部署前用 `odbcinst -q -d` 验证驱动已安装。旧 notebook 中的嵌入式凭据应轮换，验证完成后停止使用该 notebook。

## 安全与恢复

外部数据会先完整读取并通过非空、重复键、列数和异常降幅检查，再进入目标数据库事务。客户、物料、研发项目各自事务替换；五张收单表作为一个事务整体替换；供应商在一个事务内标记旧记录并 upsert 完整快照。任务失败不会先清空目标表。

`sys_business_data_sync_run` 和 `sys_business_data_sync_item` 保存运行及逐数据集状态。全局活动运行唯一索引和数据集 advisory lock 防止重叠更新。部分数据集失败时，已经提交的其他数据集保留，界面可只重试失败项。

生产部署注册、Worker 环境变量变更、依赖安装和服务重启必须按生产变更流程单独授权。
