# 薄片双人盲评模块（岩芯实验室鉴定）

零依赖（Python 标准库 + SQLite）。主持人批量编码并指派两名**不同**读片人；读片人在双方都提交前
看不到样本真实身份、也看不到对方是否已读及结论；分歧或更正后转**第三人**裁决，原读片人不得裁决；
更正留旧版、使原共识/裁决失效并重裁后才能整批发布。

## 运行

```bash
python3 server.py              # http://localhost:3037
PORT=3000 BLIND_DB=/path/x.db python3 server.py
```

浏览器打开根路径即用；数据在 `data/blind-review.db`，HMAC 密钥在 `data/blind-review.db.secret`
（均落盘，**重启后数据与已签发登录令牌仍有效**）。

## 登录（可信身份）

页面用账号 + 密码登录，换取 HMAC-SHA256 签名令牌，之后所有请求带 `Authorization: Bearer <token>`。
除 `/health` 与 `/api/auth/login` 外，无令牌、伪造/过期/角色不符一律 401。写操作还会校验 body 中的
操作者必须等于令牌本人，杜绝冒名。演示账户（可在 `server.py` 的 `USERS` 中改）：

| 账号 | 角色 | 密码 |
|---|---|---|
| 贺主持 / 卞主持 | 主持人 | host123 / host456 |
| 张工 / 李工 / 王二 | 读片人 | read123 / read456 / read789 |
| 赵裁 / 钱裁 | 第三人裁决人 | adj123 / adj456 |

## 归属与盲态

- **批次归属**：批次明细/指派/发布仅该批主持人可访问；其他主持人得到 403/404。
- **样本归属**：样本与留痕仅本批主持人或被指派的两名读片人/裁决人可访问，其他人猜盲码得到 404。
- **提交前盲态**：任一读片人**本人尚未提交**时，样本视图与 `history` 都不返回对方任何记录（含旧版），
  也不暴露样本身份与对方姓名；本人提交后才可看到双方匿名结论；发布后才揭晓身份与姓名。

## 业务规则

- 两名读片人必须不同（批次默认与单样覆盖均校验）。
- 矿物比例：必须是**有限**数值（拒绝 `NaN`/`±Infinity`/非数字/负数），合计恰好 100%（容差 0.01pct）。
- 岩性不同，或任一同名矿物相差 **>5 个百分点** → 分歧，转第三人裁决（并集比较，缺失按 0）。
- 第三人不得是该样原读片人（指派、提交两处校验），且必须具备裁决岗身份。
- **更正**：旧版置 `superseded` 留存、轮次 +1、旧裁决失效、`adjudication_required=1`。
  即使**先更正、对方后齐交**且两份结论完全一致，也必须第三人重新裁决；未重裁整批发布被拒（409）。
  更正需显式 `action=amend` 并带 `expected_version`（乐观锁，过期版本 409）。
- 状态机：`pending → reading → consensus / adjudicating → adjudicated → published`；更正回 `adjudicating`。

## 幂等：键绑定操作者 + 目标 + 内容

写请求可带 `Idempotency-Key`（或 body `idempotency_key`）。服务端为每个键记录
`(操作者, 目标, 内容指纹)`：

- 同键 + 同人 + 同目标 + 同内容 → 返回首次结果（重放，不重复生效）；
- 同键被串用到**别的操作者 / 别的目标 / 不同内容** → `409 idempotency_conflict`，绝不回放；
- 不带键的并发重复写由部分唯一索引拦截，仅一次 200，其余 409。

页面“批量编码”在内容不变时复用同一幂等键并禁用按钮，**双击/连点只建一批**；内容改动才换新键。

## 事务与故障回滚

批量编码与整批发布都在单事务 `BEGIN IMMEDIATE` 内，注入故障（body 带
`__failpoint=encode_after_insert|publish_after_update`）即整批 `ROLLBACK`，不留部分记录，
回滚后同键可干净重试。

## 接口

- `POST /api/auth/login`
- `GET  /api/me/worklist`
- `POST /api/batches`（host）；`GET /api/batches/{code}`
- `GET  /api/samples/{code}`；`GET /api/samples/{code}/history`
- `POST /api/samples/{code}/readings`（`action=submit|amend`，更正带 `expected_version`）
- `POST /api/samples/{code}/adjudicator/assign`（host）
- `POST /api/samples/{code}/adjudication`
- `POST /api/batches/{code}/publish`（host）

## 实测

```bash
python3 e2e_test.py
```

自动启停服务、用临时库，当前 **75 项断言全部通过**，分组：
可信身份、归属、提交前盲态（含历史）、非有限比例、幂等串用不回放、先更正后齐交必重裁、
双击/并发只生效一次、列表已提交状态、正常全流程、部分写失败回滚、重启保留（含旧令牌仍有效）。
