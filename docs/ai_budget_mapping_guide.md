# AI 自治修复预算更新映射差异指南

> 用途：交给 `caiwu-data-pipeline` AI 助手，让它在预算更新流程失败后自主调用 MCP 修复 mydb map 表。
> 说明：本方案**不改造** `budget_update_flow.py`，AI 通过观察流程运行结果和读取未映射摘要文件来闭环处理。

---

## 1. 触发预算更新流程

你可以通过 Prefect UI 或 API 触发主流程-预算更新：

- Flow 名称：`budget_update_flow` / `主流程-预算更新`
- 默认参数已按当前月份自动填充（年初/年中预算规则）。
- 也可手动指定参数：`budget_year`, `fone_version`, `version`, `budget_type`, `report_date`。

流程运行后：
- 成功：会收到 Hermes `completed` 通知。
- 失败：会收到 Hermes `failed` 通知，并在 `output_dir`（默认 `/root/prefect/check/budget_unmapped`）下生成：
  - `unmapped_summary.json`：汇总各维度未映射情况。
  - `未映射的{维度}.csv`：各维度未映射明细。

---

## 2. 读取未映射差异

流程失败后，读取以下文件：

```bash
/root/prefect/check/budget_unmapped/unmapped_summary.json
/root/prefect/check/budget_unmapped/未映射的*.csv
```

`unmapped_summary.json` 结构示例：

```json
{
  "has_unmapped": true,
  "generated_at": "2026-06-16T10:00:00",
  "output_dir": "/root/prefect/check/budget_unmapped",
  "unmapped": {
    "组织编码": {
      "count": 12,
      "unique_count": 3,
      "unique_values": ["1006040501", "1006040502", "1006040503"],
      "csv_path": "/root/prefect/check/budget_unmapped/未映射的组织编码.csv",
      "key_column": "组织编码"
    }
  }
}
```

---

## 3. 查询 FONE 主数据

根据未映射维度，调用 MCP 查询对应 FONE 表：

| 未映射维度 | FONE 主数据表 | 目标 mydb map 表 | 关键字段 |
|---|---|---|---|
| 组织编码 | `XGD_MRPT_ENTITY` | `map_org` | `ID`, `Name`, `ParentID`, `PrimaryOrganization`, `SecondaryOrganization`, `TertiaryOrganization`, `FourthOrganization`, `Level` |
| 客户编码 | `XGD_MRPT_CUSTOMER` | `map_cust` | `ID`, `Name`, `ParentID` |
| 产品编码 | `XGD_MRPT_PRODUCT` | `map_prod` | `ID`, `Name`, `ParentID`, `P_Mould`, `P_Class`, `P_Model` |
| 研发项目编码 | `XGD_MRPT_PROJECT` | `map_dev` | `ID`, `Name`, `ParentID`, `Class`, `Category`, `Code`, `Dept_ID`, `Dept_Name` |
| 指标编码 | `XGD_MRPT_ACCOUNT` | `map_ind` | `ID`, `Name`, `ParentID` |
| 预算费用项目编码 | `Fone2BI_Budget_FY` | `map_exp` | `部门属性`, `资产类型`, `预算科目编码`, `预算科目名称` |

FONE 连接信息（如需手动连接）：
- 通过 `mypackage.utilities.connect_to_fone()` 获取连接。
- 或使用你已配置的 MCP FONE 查询工具。

---

## 4. 构造 mydb map 表数据

### map_org（组织）

基于 `XGD_MRPT_ENTITY` 的 `ParentID` 还原 1/2/3/4 级路径：

```python
# 伪代码
identifier_id = ID
prim_org = 一级组织名称
sec_org = 二级组织名称
third_org = 三级组织名称
fourth_org = 四级组织名称
unique_lvl = f"{prim_org}-{sec_org}-{third_org}-{fourth_org}".rstrip("-")
db_corr_rel = unique_lvl
# 名称以"本部"结尾的替换为"公共部门"
```

### map_cust（客户）

```python
identifier = ID
name = Name
prev_lvl = ParentID
cust_cat = 根据 Name 推断（如含'国际'→'国际'，含'第三方'→'第三方'）
region = 根据 Name 推断（如含'国际'→'国际'，否则'国内'）
```

### map_prod（产品）

```python
identifier = ID
name = Name
prev_lvl = ParentID
prod_map = 产品大类简称（根据 P_Class / P_Model / Name 推断）
prod_major_cat = 产品大类
model = P_Model
# category / prod_series / prod_cat 根据层级或 Name 推断
```

### map_dev（研发项目）

```python
encoding = ID / Code
proj_name = Name
bus_line = 根据 Dept_Name 或业务线规则推断
proj_status = 1（默认）
proj_cat = Category / Class
prod_name = 根据项目名称推断
prod_major_cat = 根据 prod_name 推断
```

### map_ind（预算科目/指标）

```python
identifier_id = ID
name_eng = Name
prim_subj = 根据 ID 前缀或 Name 推断（如'营业收入'、'营业成本'）
inc_major_cat = 若 Name 含'收入'则填对应收入大类
exp_major_cat = 若 Name 含'费用'则填对应费用大类
```

### map_exp（费用项目）

```python
unique_exp = f"{部门属性}-{资产类型}-{预算科目编码}"
exp_item_code = 预算科目编码
expense_item_name = 预算科目名称
dept_attr = 部门属性
asset_type = 资产类型
pl_acct_code = 根据预算科目编码前缀推断损益科目编码
pl_acct_name = 根据 pl_acct_code 推断损益科目名称
```

---

## 5. 写入待审批系统

根据你的 MCP 工具能力，将构造好的映射写入待审批/审批系统：

1. 不要直接覆盖正式 map 表。
2. 写入待审批清单，等待人工审批。
3. 返回处理摘要：`{"processed_count": N, "pending_count": N, "message": "..."}`。

---

## 6. 人工审批后重跑

待审批记录经人工确认生效后，重新触发 `budget_update_flow`：

- 如果 map 表已更新，流程应通过映射检查并继续写库。
- 如果仍有未映射值，重复步骤 2-5。

---

## 7. 关键文件路径

- 未映射摘要：`/root/prefect/check/budget_unmapped/unmapped_summary.json`
- 未映射明细：`/root/prefect/check/budget_unmapped/未映射的*.csv`
- 流程代码：`/root/prefect/modules/budget_update/flows/budget_update_flow.py`
- 任务代码：`/root/prefect/modules/budget_update/tasks/budget_update_tasks.py`

---

## 8. 注意事项

- `budget_update_flow` 保持原有严格映射检查逻辑不变，失败时会导出 CSV/JSON 并 `raise ValueError`。
- AI 侧需要具备查询 FONE 和写入待审批系统的 MCP 工具。
- 对于 `map_org`，路径还原逻辑可参考 `modules/org_sync/tasks/org_sync_tasks.py`。
- 生产环境建议先在测试环境验证 AI 推断的映射准确性，再大规模应用。
