# 薄片双人盲评模块（岩芯实验室鉴定）

零依赖（Python 标准库 + SQLite）。主持人批量编码并指派两名**不同**读片人；读片人在双方都提交前
看不到样本真实身份、也看不到对方是否已读及结论；分歧转**第三人**裁决，原读片人不得裁决；
更正留旧版、使原共识/裁决失效，重裁后才能整批发布。

## 运行

```bash
python3 server.py              # 默认 http://localhost:3037
PORT=3000 BLIND_DB=/path/x.db python3 server.py
```

浏览器打开根路径即可完成全流程；数据存于 `data/blind-review.db`（SQLite，重启保留）。

## 角色与使用

页面右上角选择角色 + 姓名进入工作台（身份通过 `?role=&name=` 传递，写操作在 body 中再次绑定防冒名）：

- **主持人**：批量编码（每行一个真实身份，自动生成连续盲码 `B0001-S001…`；可用 `身份 | 读片人A | 读片人B` 为单样另指两人）→ 查看批次 → 为分歧样指派第三人 → 整批发布。
- **读片人**：只看到自己的盲码任务；填写岩性、矿物比例（动态行，合计必须正好 100%，页面前端与服务端双重校验）、鉴定依据。双方齐交后可互相查看匿名结论。可对自己的读片发起**更正**。
- **第三人裁决人**：被主持人指派后，工作台出现待裁样本，可见两份匿名读片，提交最终鉴定。

## 业务规则落地

| 要求 | 实现 |
|---|---|
| 两名读片人不同 | 编码时校验（批次默认人与单样覆盖均校验） |
| 提交前不知身份/对方结果 | 服务端按角色裁剪视图；未齐交只返回本人 `my_reading`；未参与者/猜盲码统一 404 |
| 比例合计必须 100% | 服务端容差 0.01pct 强校验；负数/空表/同名重复均拒绝；前端实时合计 |
| 岩性不同 或 任一矿物差 > 5pct → 分歧 | `in_disagreement()`（并集比较矿物，缺失按 0） |
| 第三人不得是原读片人 | 指派与提交两处都校验 `reader_a/reader_b` |
| 更正留旧版、原共识失效、重裁后发布 | 旧读片置 `superseded` 留存、轮次 +1、旧裁决失效、状态强制回 `adjudicating`；未重裁发布 409 |
| 并发重复只生效一次 | 客户端幂等键 + `idem_keys` 重放；无键并发由部分唯一索引 `ux_active_reading` / 状态约束拦截，重复返回 409；提交与更正显式分离，更正带 `expected_version` 乐观锁 |
| 整批失败不留部分记录 | 编码、发布均在单事务 `BEGIN IMMEDIATE` 内，异常即 `ROLLBACK` |
| 重启保留 | SQLite WAL 落盘 |

状态机：`pending → reading → consensus / adjudicating → adjudicated → published`；更正使样本回到
`adjudicating` 且轮次 +1。

## HTTP 接口（均为 JSON；身份用查询参数或 `X-Role/X-Name` 头）

- `POST /api/batches`（host）批量编码
- `GET  /api/me/worklist` 各角色待办
- `GET  /api/batches/{code}` 批次明细（host 见身份；他人按盲态裁剪）
- `GET  /api/samples/{code}`、`GET /api/samples/{code}/history`
- `POST /api/samples/{code}/readings` 读片提交/更正（`action=submit|amend`，更正带 `expected_version`）
- `POST /api/samples/{code}/adjudicator/assign`（host）指派第三人
- `POST /api/samples/{code}/adjudication` 第三人裁决
- `POST /api/batches/{code}/publish`（host）整批发布

写请求建议带 `Idempotency-Key`（或 body 中 `idempotency_key`），重试安全。

## 实测

```bash
python3 e2e_test.py
```

自动启停服务、使用临时库，覆盖七组场景（当前 63 项断言全过）：
盲态/越权、比例边界、共识与分歧转第三人、更正留痕与失效重裁、并发只生效一次、
故障注入整批回滚、重启保留。故障注入通过请求体 `__failpoint`
（`encode_after_insert` / `publish_after_update`）触发，仅用于测试。
