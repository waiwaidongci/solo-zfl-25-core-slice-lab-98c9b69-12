#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
岩芯实验室 · 薄片双人盲评模块（零依赖：标准库 + SQLite）

角色
  主持人 HOST   ：批量编码、指派两名不同读片人、指派第三人裁决、整批发布
  读片人        ：只能看到分配给自己且尚未发布的盲码；提交前看不到样本身份/对方结果
  第三人裁决人  ：分歧样的裁决人，不得是该样原两名读片人；提交一份裁决

关键保证
  * 比例合计必须 = 100%（容差 0.01 个百分点）
  * 岩性不同 或 任一同名矿物相差 > 5 个百分点 => 分歧，转第三人裁决
  * 更正任一读片 => 旧版留存(superseded)、原共识/裁决失效 => 回到待裁决，重裁后才能发布
  * 幂等键（编码/提交/裁决/发布）并发只生效一次；无幂等键的并发重复返回 409
  * 批量编码与整批发布为单事务，故障注入点触发后整批回滚，不留部分记录
  * SQLite 文件持久化，重启保留
"""
import json
import os
import threading
import sqlite3
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
DB_PATH = os.environ.get("BLIND_DB", os.path.join(DATA_DIR, "blind-review.db"))
PORT = int(os.environ.get("PORT", "3037"))
# 测试用故障注入开关（默认开；仅在请求显式带 X-Failpoint 时才动作，不影响正常使用）
FAILPOINTS_ENABLED = os.environ.get("FAILPOINTS", "1") != "0"

EPS = 0.01            # 比例合计容差（百分点）
MINERAL_DIFF = 5.0    # >5 个百分点判分歧
_lock = threading.Lock()  # 序列化写事务，配合 SQLite UNIQUE 约束做并发幂等


# --------------------------------------------------------------------------- 数据库
def now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def connect():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.execute("PRAGMA busy_timeout=30000;")
    return conn


SCHEMA = """
CREATE TABLE IF NOT EXISTS batches (
  id INTEGER PRIMARY KEY,
  code TEXT UNIQUE NOT NULL,
  project TEXT NOT NULL,
  host TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'open',          -- open | published
  published_at TEXT,
  created_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS samples (
  id INTEGER PRIMARY KEY,
  batch_id INTEGER NOT NULL REFERENCES batches(id),
  blind_code TEXT UNIQUE NOT NULL,
  identity TEXT NOT NULL,                       -- 盲评期间隐藏的真实身份（孔号/深度等）
  default_reader_a TEXT,
  default_reader_b TEXT,
  reader_a TEXT NOT NULL,
  reader_b TEXT NOT NULL,
  round INTEGER NOT NULL DEFAULT 1,             -- 每更正一次 +1，原轮共识失效
  status TEXT NOT NULL DEFAULT 'pending',      -- pending|reading|consensus|adjudicating|adjudicated|published
  published_at TEXT
);
CREATE TABLE IF NOT EXISTS readings (
  id INTEGER PRIMARY KEY,
  sample_id INTEGER NOT NULL REFERENCES samples(id),
  slot TEXT NOT NULL,                           -- a | b
  reader TEXT NOT NULL,
  version INTEGER NOT NULL DEFAULT 1,
  round INTEGER NOT NULL DEFAULT 1,
  lithology TEXT NOT NULL,
  minerals TEXT NOT NULL,                       -- JSON: [{mineral, percent}]
  rationale TEXT NOT NULL,
  superseded INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL
);
-- 当前轮当前槽位只允许一份有效读片（更正时旧版置 superseded，天然防并发重复）
CREATE UNIQUE INDEX IF NOT EXISTS ux_active_reading
  ON readings(sample_id, slot) WHERE superseded = 0;
CREATE TABLE IF NOT EXISTS adjudications (
  id INTEGER PRIMARY KEY,
  sample_id INTEGER NOT NULL REFERENCES samples(id),
  round INTEGER NOT NULL,
  adjudicator TEXT NOT NULL,
  lithology TEXT NOT NULL,
  minerals TEXT NOT NULL,
  rationale TEXT NOT NULL,
  superseded INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_active_adj
  ON adjudications(sample_id) WHERE superseded = 0;
CREATE TABLE IF NOT EXISTS idem_keys (
  key TEXT PRIMARY KEY,
  scope TEXT NOT NULL,                          -- encode|reading|adjudicate|publish
  response_status INTEGER NOT NULL,
  response_body TEXT NOT NULL,
  created_at TEXT NOT NULL
);
"""


def init_db():
    conn = connect()
    try:
        conn.executescript(SCHEMA)
        conn.execute("INSERT OR IGNORE INTO idem_keys(key, scope, response_status, response_body, created_at) VALUES ('__seed__','seed',200,'{}',?)", (now(),))
    finally:
        conn.close()


# --------------------------------------------------------------------------- 校验与判定
class ApiError(Exception):
    def __init__(self, status, code, message=None):
        super().__init__(message or code)
        self.status = status
        self.code = code


def failpoint(name, body):
    """仅当开启故障注入且请求显式指定 X-Failpoint 匹配时抛出，模拟事务中途崩溃。"""
    if FAILPOINTS_ENABLED and body.get("__failpoint") == name:
        raise RuntimeError("injected_failure:" + name)


def clean_str(value, field, required=True):
    if not isinstance(value, str):
        raise ApiError(400, "invalid_field", f"{field} 必须是字符串")
    value = value.strip()
    if required and not value:
        raise ApiError(400, "invalid_field", f"{field} 不能为空")
    return value


def parse_minerals(raw):
    """矿物比例：非空数组、名称非空、百分数为 >=0 的数、合计必须为 100（容差 EPS）。"""
    if not isinstance(raw, list) or not raw:
        raise ApiError(400, "minerals_required", "矿物比例至少包含一项")
    out = []
    seen = set()
    total = 0.0
    for i, item in enumerate(raw):
        if not isinstance(item, dict):
            raise ApiError(400, "invalid_mineral", f"第 {i+1} 项格式错误")
        name = clean_str(item.get("mineral"), f"矿物{i+1}")
        if name in seen:
            raise ApiError(400, "duplicate_mineral", f"矿物 {name} 重复")
        seen.add(name)
        pct = item.get("percent")
        if isinstance(pct, bool) or not isinstance(pct, (int, float)):
            raise ApiError(400, "invalid_percent", f"{name} 的比例必须是数字")
        pct = float(pct)
        if pct < -EPS:
            raise ApiError(400, "invalid_percent", f"{name} 的比例不能为负")
        total += pct
        out.append({"mineral": name, "percent": round(pct, 4)})
    if abs(total - 100.0) > EPS:
        raise ApiError(400, "minerals_sum_not_100",
                       f"矿物比例合计必须为 100%，当前为 {round(total, 4)}%")
    # 规范化负值零
    for item in out:
        if item["percent"] < 0:
            item["percent"] = 0.0
    return out


def minerals_map(minerals_json):
    return {m["mineral"]: float(m["percent"]) for m in json.loads(minerals_json)}


def in_disagreement(ra, rb):
    """岩性不同（去空白后大小写敏感）或任一矿物相差 > 5 个百分点。"""
    if ra["lithology"].strip() != rb["lithology"].strip():
        return True, "岩性不同"
    ma, mb = minerals_map(ra["minerals"]), minerals_map(rb["minerals"])
    for name in sorted(set(ma) | set(mb)):
        if abs(ma.get(name, 0.0) - mb.get(name, 0.0)) > MINERAL_DIFF + 1e-9:
            return True, f"矿物 {name} 相差超过 {MINERAL_DIFF:g} 个百分点"
    return False, None


def active_reading(conn, sample_id, slot):
    return conn.execute(
        "SELECT * FROM readings WHERE sample_id=? AND slot=? AND superseded=0",
        (sample_id, slot)).fetchone()


# --------------------------------------------------------------------------- 序列化（盲态裁剪）
def reading_public(r, include_reader=False, include_meta=False):
    d = {
        "lithology": r["lithology"],
        "minerals": json.loads(r["minerals"]),
        "rationale": r["rationale"],
    }
    if include_reader:
        d["reader"] = r["reader"]
    if include_meta:
        d["version"] = r["version"]
        d["round"] = r["round"]
        d["created_at"] = r["created_at"]
    return d


def adjudication_public(adj, include_adjudicator=False):
    if adj["lithology"] == "__PENDING__":
        # 已指派、待提交的占位：不泄露裁决内容
        d = {"pending": True, "round": adj["round"], "created_at": adj["created_at"]}
        if include_adjudicator:
            d["adjudicator"] = adj["adjudicator"]
        return d
    d = {
        "pending": False,
        "lithology": adj["lithology"],
        "minerals": json.loads(adj["minerals"]),
        "rationale": adj["rationale"],
        "round": adj["round"],
        "created_at": adj["created_at"],
    }
    if include_adjudicator:
        d["adjudicator"] = adj["adjudicator"]
    return d


def sample_view(conn, s, role, name):
    """按角色/身份裁剪的样本视图：未发布前，读片人看不到身份与对方结果。"""
    published = s["status"] == "published"
    batch = conn.execute("SELECT code,project,status FROM batches WHERE id=?", (s["batch_id"],)).fetchone()
    adj = conn.execute(
        "SELECT * FROM adjudications WHERE sample_id=? AND superseded=0", (s["id"],)).fetchone()
    ra = active_reading(conn, s["id"], "a")
    rb = active_reading(conn, s["id"], "b")

    view = {
        "blind_code": s["blind_code"],
        "batch_code": batch["code"],
        "project": batch["project"],
        "status": s["status"],
        "round": s["round"],
        "reader_a": s["reader_a"],
        "reader_b": s["reader_b"],
    }
    if published:
        # 发布后揭晓全部
        view["identity"] = s["identity"]
        view["published_at"] = s["published_at"]
        view["reading_a"] = reading_public(ra, include_reader=True, include_meta=True) if ra else None
        view["reading_b"] = reading_public(rb, include_reader=True, include_meta=True) if rb else None
        view["adjudication"] = adjudication_public(adj, include_adjudicator=True) if adj else None
        return view

    if role == "host":
        view["identity"] = s["identity"]
        view["reading_a"] = reading_public(ra, include_reader=True, include_meta=True) if ra else None
        view["reading_b"] = reading_public(rb, include_reader=True, include_meta=True) if rb else None
        view["adjudication"] = adjudication_public(adj, include_adjudicator=True) if adj else None
        return view

    # 读片人/裁决人在未发布阶段一律看不到真实身份
    my_slot = "a" if name == s["reader_a"] else "b" if name == s["reader_b"] else None
    if role == "reader" and my_slot is not None:
        # 告知本人槽位与当前版本，便于其定位/更正自己的读片（不涉及对方身份）
        view["my_slot"] = my_slot
        own = ra if my_slot == "a" else rb
        if own is not None:
            view["my_version"] = own["version"]
    if ra and rb:
        # 两份都已提交：盲态保护期结束，可互相查看（但身份仍隐藏）
        view["reading_a"] = reading_public(ra)
        view["reading_b"] = reading_public(rb)
        view["adjudication"] = adjudication_public(adj) if adj else None
    else:
        # 尚未齐交：读片人只能看自己那份，绝不暴露对方是否已写/写了什么
        own = ra if my_slot == "a" else rb if my_slot == "b" else None
        if own:
            view["my_reading"] = reading_public(own, include_meta=True)
    return view


# --------------------------------------------------------------------------- 业务操作（均在调用方事务内）
def op_encode_batch(conn, body):
    project = clean_str(body.get("project"), "project")
    host = clean_str(body.get("host"), "host")
    samples = body.get("samples")
    if not isinstance(samples, list) or not samples:
        raise ApiError(400, "samples_required", "批量编码至少需要一个样本")

    def_a = clean_str(body.get("default_reader_a"), "default_reader_a")
    def_b = clean_str(body.get("default_reader_b"), "default_reader_b")
    if def_a == def_b:
        raise ApiError(400, "readers_must_differ", "两名读片人不能是同一人")

    norm = []
    for i, item in enumerate(samples):
        if not isinstance(item, dict):
            raise ApiError(400, "invalid_sample", f"第 {i+1} 个样本格式错误")
        identity = clean_str(item.get("identity"), f"样本{i+1}身份")
        ra = clean_str(item.get("reader_a", def_a), f"样本{i+1}读片人A")
        rb = clean_str(item.get("reader_b", def_b), f"样本{i+1}读片人B")
        if ra == rb:
            raise ApiError(400, "readers_must_differ",
                           f"样本 {i+1}：两名读片人不能是同一人（{ra}）")
        norm.append((identity, ra, rb))

    # 批次编码：全局自增，批量内连续；UNIQUE(blind_code) 兜底并发
    row = conn.execute("SELECT COALESCE(MAX(id),0)+1 AS next FROM batches").fetchone()
    batch_id = row["next"]
    batch_code = "B%04d" % batch_id
    created = now()
    conn.execute("INSERT INTO batches(id,code,project,host,created_at) VALUES (?,?,?,?,?)",
                 (batch_id, batch_code, project, host, created))

    result_samples = []
    for idx, (identity, ra, rb) in enumerate(norm, start=1):
        blind = "%s-S%03d" % (batch_code, idx)
        cur = conn.execute(
            """INSERT INTO samples(batch_id,blind_code,identity,default_reader_a,default_reader_b,
                                   reader_a,reader_b,round,status)
               VALUES (?,?,?,?,?,?,?,1,'pending')""",
            (batch_id, blind, identity, def_a, def_b, ra, rb))
        failpoint("encode_after_insert", body)  # 注入点：已写部分样本后崩溃 => 必须整批回滚
        result_samples.append({"blind_code": blind, "identity": identity,
                               "reader_a": ra, "reader_b": rb})
    return 201, {"batch_code": batch_code, "project": project, "count": len(norm),
                 "samples": result_samples}


def _load_sample_for_reader(conn, blind_code, name):
    s = conn.execute("SELECT * FROM samples WHERE blind_code=?", (blind_code,)).fetchone()
    if not s:
        raise ApiError(404, "sample_not_found", "盲码不存在")
    if name not in (s["reader_a"], s["reader_b"]):
        # 不区分“不存在/未分配”，统一 404，避免盲码枚举
        raise ApiError(404, "sample_not_found", "盲码不存在或未分配给你")
    if s["status"] == "published":
        raise ApiError(409, "already_published", "该样本已发布，不能再提交或更正")
    return s


def op_submit_reading(conn, body, blind_code):
    name = clean_str(body.get("reader"), "reader")
    lithology = clean_str(body.get("lithology"), "lithology")
    rationale = clean_str(body.get("rationale"), "rationale")
    minerals = parse_minerals(body.get("minerals"))

    s = _load_sample_for_reader(conn, blind_code, name)
    slot = "a" if name == s["reader_a"] else "b"
    other_slot = "b" if slot == "a" else "a"
    existing = active_reading(conn, s["id"], slot)
    action = (body.get("action") or "submit").strip()
    expected_version = body.get("expected_version")

    if existing is not None and action != "amend":
        # 普通提交遇到已存在有效读片：重复操作直接冲突（并发重复首交也由唯一索引兜底到这里）
        raise ApiError(409, "already_submitted",
                       "你已提交过读片；如需修改请显式发起“更正”(action=amend) 并提供 expected_version")
    if action == "amend":
        if existing is None:
            raise ApiError(409, "nothing_to_amend", "你尚未提交读片，不能更正；请先提交")
        try:
            ev = int(expected_version)
        except (TypeError, ValueError):
            raise ApiError(400, "invalid_expected_version", "更正必须提供整数 expected_version")
        if ev != int(existing["version"]):
            # 乐观锁：基于过期版本更正会被拒绝，防止并发覆盖
            raise ApiError(409, "version_conflict",
                           f"更正基于的版本 {ev} 已过期，当前为 v{existing['version']}")
    elif action != "submit":
        raise ApiError(400, "invalid_action", "action 只能是 submit 或 amend")

    if existing is None:
        # ---- 首次提交：创建 v1，落当前轮 ----
        conn.execute(
            """INSERT INTO readings(sample_id,slot,reader,version,round,lithology,minerals,rationale,created_at)
               VALUES (?,?,?,1,?,?,?,?,?)""",
            (s["id"], slot, name, s["round"], lithology,
             json.dumps(minerals, ensure_ascii=False), rationale, now()))
        if s["status"] == "pending":
            conn.execute("UPDATE samples SET status='reading' WHERE id=?", (s["id"],))
        action = "submitted"
    else:
        # ---- 更正：仅归档本人旧版（留存），对方当前意见升入新一轮继续有效 ----
        new_version = existing["version"] + 1
        new_round = s["round"] + 1
        conn.execute("UPDATE readings SET superseded=1 WHERE id=?", (existing["id"],))
        # 原轮裁决（无论是否已完成）随更正失效
        conn.execute("UPDATE adjudications SET superseded=1 WHERE sample_id=? AND superseded=0",
                     (s["id"],))
        other = active_reading(conn, s["id"], other_slot)
        if other is not None:
            conn.execute("UPDATE readings SET round=? WHERE id=?", (new_round, other["id"]))
        conn.execute(
            """INSERT INTO readings(sample_id,slot,reader,version,round,lithology,minerals,rationale,created_at)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (s["id"], slot, name, new_version, new_round, lithology,
             json.dumps(minerals, ensure_ascii=False), rationale, now()))
        conn.execute("UPDATE samples SET round=? WHERE id=?", (new_round, s["id"]))
        action = "amended"

    # ---- 依据当前轮两份有效读片重新判定状态 ----
    s = conn.execute("SELECT * FROM samples WHERE id=?", (s["id"],)).fetchone()
    ra = active_reading(conn, s["id"], "a")
    rb = active_reading(conn, s["id"], "b")
    reason = None
    if not (ra and rb):
        new_status = "reading"
    elif action == "amended":
        # 更正一律使原共识失效：即便更正后两份恰好一致，也必须重新裁决后才能发布
        new_status = "adjudicating"
        _, reason = in_disagreement(ra, rb)
        reason = reason or "读片记录已更正，原共识失效，需重新裁决"
    else:
        dispute, reason = in_disagreement(ra, rb)
        new_status = "adjudicating" if dispute else "consensus"
    conn.execute("UPDATE samples SET status=? WHERE id=?", (new_status, s["id"]))

    mine = active_reading(conn, s["id"], slot)
    final_round = new_round if action == "amended" else s["round"]
    return 200, {"blind_code": blind_code, "slot": slot, "action": action,
                 "version": mine["version"], "round": final_round,
                 "sample_status": new_status,
                 "dispute_reason": reason if new_status == "adjudicating" else None}


def op_assign_adjudicator(conn, body, blind_code):
    host = clean_str(body.get("host"), "host")
    adjudicator = clean_str(body.get("adjudicator"), "adjudicator")
    s = conn.execute("SELECT * FROM samples WHERE blind_code=?", (blind_code,)).fetchone()
    if not s:
        raise ApiError(404, "sample_not_found")
    batch = conn.execute("SELECT host FROM batches WHERE id=?", (s["batch_id"],)).fetchone()
    if batch["host"] != host:
        raise ApiError(403, "forbidden", "只有本批次主持人可以指派裁决人")
    if adjudicator in (s["reader_a"], s["reader_b"]):
        raise ApiError(403, "adjudicator_conflict", "第三人裁决人不得是该样本的原读片人")
    if s["status"] == "published":
        raise ApiError(409, "already_published", "样本已发布")
    if s["status"] != "adjudicating":
        raise ApiError(409, "not_in_dispute", f"样本当前状态为 {s['status']}，无需裁决")
    existing = conn.execute(
        "SELECT * FROM adjudications WHERE sample_id=? AND superseded=0", (s["id"],)).fetchone()
    if existing is not None and existing["lithology"] != "__PENDING__":
        raise ApiError(409, "already_adjudicated", "该样本本轮已完成裁决；如需变更须由读片人更正触发重裁")
    if existing is None:
        conn.execute(
            """INSERT INTO adjudications(sample_id,round,adjudicator,lithology,minerals,rationale,created_at)
               VALUES (?,?,?,'__PENDING__','[]','',?)""",
            (s["id"], s["round"], adjudicator, now()))
    else:
        conn.execute("UPDATE adjudications SET adjudicator=?,created_at=? WHERE id=?",
                     (adjudicator, now(), existing["id"]))
    return 200, {"blind_code": blind_code, "adjudicator": adjudicator, "status": "assigned"}


def op_submit_adjudication(conn, body, blind_code):
    name = clean_str(body.get("adjudicator"), "adjudicator")
    lithology = clean_str(body.get("lithology"), "lithology")
    rationale = clean_str(body.get("rationale"), "rationale")
    minerals = parse_minerals(body.get("minerals"))

    s = conn.execute("SELECT * FROM samples WHERE blind_code=?", (blind_code,)).fetchone()
    if not s:
        raise ApiError(404, "sample_not_found")
    if s["status"] == "published":
        raise ApiError(409, "already_published", "样本已发布")
    if s["status"] != "adjudicating":
        raise ApiError(409, "not_in_dispute", f"样本当前状态为 {s['status']}，无需裁决")
    if name in (s["reader_a"], s["reader_b"]):
        raise ApiError(403, "adjudicator_conflict", "原读片人不能裁决自己的样本")

    adj = conn.execute("SELECT * FROM adjudications WHERE sample_id=? AND superseded=0",
                       (s["id"],)).fetchone()
    if adj is None:
        raise ApiError(403, "adjudicator_not_assigned", "主持人尚未指派裁决人")
    if adj["adjudicator"] != name:
        raise ApiError(403, "forbidden", "你不是该样本被指派的裁决人")
    if adj["lithology"] != "__PENDING__":
        # 活跃裁决已存在（并发重复提交）——由唯一索引/幂等键拦截，这里兜底
        raise ApiError(409, "already_adjudicated", "该样本已裁决")

    conn.execute(
        "UPDATE adjudications SET lithology=?,minerals=?,rationale=?,created_at=? WHERE id=?",
        (lithology, json.dumps(minerals, ensure_ascii=False), rationale, now(), adj["id"]))
    conn.execute("UPDATE samples SET status='adjudicated' WHERE id=?", (s["id"],))
    return 200, {"blind_code": blind_code, "adjudicator": name, "round": s["round"],
                 "sample_status": "adjudicated"}


def op_publish_batch(conn, body, batch_code):
    host = clean_str(body.get("host"), "host")
    batch = conn.execute("SELECT * FROM batches WHERE code=?", (batch_code,)).fetchone()
    if not batch:
        raise ApiError(404, "batch_not_found")
    if batch["host"] != host:
        raise ApiError(403, "forbidden", "只有本批次主持人可以发布")
    if batch["status"] == "published":
        raise ApiError(409, "already_published", "批次已发布")

    samples = conn.execute("SELECT * FROM samples WHERE batch_id=? ORDER BY id", (batch["id"],)).fetchall()
    if not samples:
        raise ApiError(400, "empty_batch", "空批次不能发布")
    blocking = []
    for s in samples:
        if s["status"] == "adjudicating":
            blocking.append({"blind_code": s["blind_code"], "reason": "第三人尚未完成裁决"})
        elif s["status"] in ("pending", "reading"):
            blocking.append({"blind_code": s["blind_code"], "reason": "两名读片人尚未都提交"})
        # consensus / adjudicated 可发布
    if blocking:
        raise ApiError(409, "batch_not_ready", "批次尚未达到可发布状态", )

    ts = now()
    for s in samples:
        failpoint("publish_after_update", body)  # 注入点：发布中途崩溃 => 整批回滚
        conn.execute("UPDATE samples SET status='published',published_at=? WHERE id=?", (ts, s["id"]))
    conn.execute("UPDATE batches SET status='published',published_at=? WHERE id=?", (ts, batch["id"]))
    return 200, {"batch_code": batch_code, "published": len(samples), "published_at": ts}


# --------------------------------------------------------------------------- 读接口
def get_batch_detail(conn, batch_code, role, name):
    batch = conn.execute("SELECT * FROM batches WHERE code=?", (batch_code,)).fetchone()
    if not batch:
        raise ApiError(404, "batch_not_found")
    samples = conn.execute("SELECT * FROM samples WHERE batch_id=? ORDER BY id", (batch["id"],)).fetchall()
    return {
        "code": batch["code"], "project": batch["project"], "host": batch["host"],
        "status": batch["status"], "created_at": batch["created_at"],
        "published_at": batch["published_at"],
        "samples": [sample_view(conn, s, role, name) for s in samples],
    }


# --------------------------------------------------------------------------- HTTP
class Handler(BaseHTTPRequestHandler):
    server_version = "BlindReview/1.0"

    def log_message(self, fmt, *args):
        pass  # 静默；测试脚本自行输出

    def _send(self, status, obj):
        data = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _err(self, status, code, message=None):
        self._send(status, {"error": code, "message": message or code})

    def _hdr_utf8(self, key):
        """http.server 按 latin-1 解析请求头，中文姓名需还原为 UTF-8。"""
        value = self.headers.get(key)
        if not value:
            return ""
        try:
            return value.encode("latin-1").decode("utf-8").strip()
        except (UnicodeEncodeError, UnicodeDecodeError):
            return value.strip()

    def _auth(self):
        qs = parse_qs(urlparse(self.path).query)
        first = lambda key: (qs.get(key) or [""])[0].strip()
        # 角色：头优先（ASCII 安全），退回查询参数；姓名：查询参数（浏览器 UTF-8 安全）或头（curl）
        role = (self.headers.get("X-Role") or first("role")).strip()
        name = first("name") or self._hdr_utf8("X-Name")
        name = name.strip()
        if role not in ("host", "reader", "adjudicator"):
            raise ApiError(401, "unauthorized", "缺少有效角色（role=host|reader|adjudicator）")
        if not name:
            raise ApiError(401, "unauthorized", "缺少姓名（name）")
        return role, name

    def _read_body(self):
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            length = 0
        raw = self.rfile.read(length) if length > 0 else b""
        if not raw:
            return {}
        try:
            data = json.loads(raw.decode("utf-8"))
            if not isinstance(data, dict):
                raise ApiError(400, "invalid_body", "请求体必须是 JSON 对象")
            return data
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise ApiError(400, "invalid_body", "请求体不是合法 JSON")

    def do_GET(self):
        try:
            url = urlparse(self.path)
            p = url.path.strip("/")
            parts = p.split("/") if p else []
            conn = connect()
            try:
                if p == "health":
                    return self._send(200, {"ok": True, "db": DB_PATH, "time": now()})

                role, name = self._auth()

                if p == "api/me/worklist":
                    return self._send(200, self._worklist(conn, role, name))
                if len(parts) == 3 and parts[0] == "api" and parts[1] == "batches":
                    # /api/batches/{code}
                    return self._send(200, get_batch_detail(conn, parts[2], role, name))
                if len(parts) == 4 and parts[0] == "api" and parts[1] == "samples" and parts[3] == "history":
                    # /api/samples/{code}/history
                    return self._send(200, self._history(conn, parts[2], role, name))
                if len(parts) == 3 and parts[0] == "api" and parts[1] == "samples":
                    s = conn.execute("SELECT * FROM samples WHERE blind_code=?", (parts[2],)).fetchone()
                    if not s:
                        raise ApiError(404, "sample_not_found")
                    self._guard_sample_access(conn, s, role, name)
                    return self._send(200, sample_view(conn, s, role, name))
                raise ApiError(404, "not_found")
            finally:
                conn.close()
        except ApiError as e:
            self._err(e.status, e.code, str(e))
        except Exception as e:  # noqa: BLE001
            self._err(500, "internal_error", str(e))

    def do_POST(self):
        try:
            url = urlparse(self.path)
            p = urlparse(self.path).path.strip("/")
            parts = p.split("/") if p else []
            role, name = self._auth()
            body = self._read_body()
            idem = (self.headers.get("Idempotency-Key") or body.get("idempotency_key") or "").strip()

            # 路由 -> (scope, 操作函数, 是否仅限 host, body 中操作者字段)
            route = None
            if p == "api/batches" :
                route = ("encode", lambda c, b: op_encode_batch(c, b), True, "host")
            elif len(parts) == 4 and parts[0] == "api" and parts[1] == "samples" and parts[3] == "readings":
                route = ("reading", lambda c, b: op_submit_reading(c, b, parts[2]), False, "reader")
            elif len(parts) == 5 and parts[0] == "api" and parts[1] == "samples" and parts[3] == "adjudicator":
                if parts[4] == "assign":
                    route = ("assign_adj", lambda c, b: op_assign_adjudicator(c, b, parts[2]), True, "host")
            elif len(parts) == 4 and parts[0] == "api" and parts[1] == "samples" and parts[3] == "adjudication":
                route = ("adjudicate", lambda c, b: op_submit_adjudication(c, b, parts[2]), False, "adjudicator")
            elif len(parts) == 4 and parts[0] == "api" and parts[1] == "batches" and parts[3] == "publish":
                route = ("publish", lambda c, b: op_publish_batch(c, b, parts[2]), True, "host")
            if route is None:
                raise ApiError(404, "not_found")

            scope, op, host_only, actor_field = route
            if host_only and role != "host":
                raise ApiError(403, "forbidden", "该操作仅限主持人")
            # 身份绑定：body 中操作者必须与登录身份一致，防止冒名
            actor = (body.get(actor_field) or "").strip() if isinstance(body.get(actor_field), str) else ""
            if actor != name:
                raise ApiError(403, "identity_mismatch",
                               f"操作人 {actor_field}='{actor}' 与登录身份 '{name}' 不一致")

            self._run_write(scope, idem, op, body)
        except ApiError as e:
            self._err(e.status, e.code, str(e))
        except Exception as e:  # noqa: BLE001
            # 注入故障统一返回 500（事务已回滚）
            self._err(500, "internal_error", str(e))

    # ------------------------------------------------------------------ 写入与幂等
    def _run_write(self, scope, idem, op, body):
        if not idem:
            # 无幂等键：全局写锁 + 事务；并发重复写由唯一索引兜底（IntegrityError -> 409）
            with _lock:
                conn = connect()
                try:
                    conn.execute("BEGIN IMMEDIATE")
                    try:
                        status, resp = op(conn, body)
                        conn.execute("COMMIT")
                    except Exception:
                        conn.execute("ROLLBACK")
                        raise
                    self._send(status, resp)
                except sqlite3.IntegrityError as e:
                    self._err(409, "conflict", "并发重复操作，仅生效一次（" + str(e) + "）")
                finally:
                    conn.close()
            return

        full_key = "%s:%s" % (scope, idem)
        with _lock:
            conn = connect()
            try:
                cached = conn.execute("SELECT response_status,response_body FROM idem_keys WHERE key=?",
                                      (full_key,)).fetchone()
                if cached:
                    return self._send(cached["response_status"], json.loads(cached["response_body"]))
                conn.execute("BEGIN IMMEDIATE")
                try:
                    status, resp = op(conn, body)
                    conn.execute(
                        "INSERT INTO idem_keys(key,scope,response_status,response_body,created_at) VALUES (?,?,?,?,?)",
                        (full_key, scope, status, json.dumps(resp, ensure_ascii=False), now()))
                    conn.execute("COMMIT")
                except Exception:
                    conn.execute("ROLLBACK")
                    raise
                self._send(status, resp)
            except sqlite3.IntegrityError:
                # 并发下另一线程已插入同键 -> 重放其结果
                conn.close()
                conn = connect()
                cached = conn.execute("SELECT response_status,response_body FROM idem_keys WHERE key=?",
                                      (full_key,)).fetchone()
                if cached:
                    return self._send(cached["response_status"], json.loads(cached["response_body"]))
                self._err(409, "conflict", "并发重复操作，仅生效一次")
            finally:
                conn.close()

    # ------------------------------------------------------------------ 访问控制
    def _guard_sample_access(self, conn, s, role, name):
        if role == "host":
            return
        allowed = {s["reader_a"], s["reader_b"]}
        adj = conn.execute("SELECT adjudicator FROM adjudications WHERE sample_id=? AND superseded=0",
                           (s["id"],)).fetchone()
        if adj:
            allowed.add(adj["adjudicator"])
        if name not in allowed:
            # 统一 404，防止盲码枚举
            raise ApiError(404, "sample_not_found", "盲码不存在或与你无关")

    def _worklist(self, conn, role, name):
        if role == "host":
            batches = conn.execute("SELECT code FROM batches WHERE host=? ORDER BY id", (name,)).fetchall()
            return {"role": "host", "batches": [b["code"] for b in batches]}
        rows = conn.execute(
            """SELECT s.* FROM samples s WHERE (s.reader_a=? OR s.reader_b=?)
                 AND s.status!='published' ORDER BY s.id""", (name, name)).fetchall()
        if role == "reader":
            items = []
            for s in rows:
                v = sample_view(conn, s, "reader", name)
                items.append(v)
            return {"role": "reader", "samples": items}
        # adjudicator：被指派且仍待裁决
        adj_rows = conn.execute(
            """SELECT s.* FROM samples s
               JOIN adjudications a ON a.sample_id=s.id AND a.superseded=0
               WHERE a.adjudicator=? AND s.status='adjudicating' AND a.lithology='__PENDING__'
               ORDER BY s.id""", (name,)).fetchall()
        # 裁决人需要看到两份读片才能裁决（身份仍隐藏）
        items = []
        for s in adj_rows:
            v = sample_view(conn, s, "adjudicator", name)
            items.append(v)
        return {"role": "adjudicator", "samples": items}

    def _history(self, conn, blind_code, role, name):
        s = conn.execute("SELECT * FROM samples WHERE blind_code=?", (blind_code,)).fetchone()
        if not s:
            raise ApiError(404, "sample_not_found")
        self._guard_sample_access(conn, s, role, name)
        readings = conn.execute("SELECT * FROM readings WHERE sample_id=? ORDER BY id", (s["id"],)).fetchall()
        adjs = conn.execute("SELECT * FROM adjudications WHERE sample_id=? ORDER BY id", (s["id"],)).fetchall()
        # 历史中含读者姓名：仅主持人；读片人在双方齐交后可见内容，历史版本始终不暴露对方姓名给非主持人
        include_people = role == "host"
        return {
            "blind_code": blind_code,
            "status": s["status"],
            "round": s["round"],
            "identity": s["identity"] if role == "host" or s["status"] == "published" else None,
            "readings": [
                {**reading_public(r, include_reader=include_people, include_meta=True),
                 "slot": r["slot"], "superseded": bool(r["superseded"])}
                for r in readings
            ],
            "adjudications": [
                {**adjudication_public(a, include_adjudicator=include_people),
                 "superseded": bool(a["superseded"]),
                 "pending": a["lithology"] == "__PENDING__"}
                for a in adjs
            ],
        }


def serve_index(handler):
    index_path = os.path.join(BASE_DIR, "index.html")
    with open(index_path, "rb") as f:
        data = f.read()
    handler.send_response(200)
    handler.send_header("Content-Type", "text/html; charset=utf-8")
    handler.send_header("Content-Length", str(len(data)))
    handler.end_headers()
    handler.wfile.write(data)


# 根路径返回页面
_orig_get = Handler.do_GET
def do_get_with_index(self):
    if urlparse(self.path).path in ("/", "/index.html"):
        try:
            return serve_index(self)
        except FileNotFoundError:
            return self._err(404, "not_found", "页面缺失")
    return _orig_get(self)
Handler.do_GET = do_get_with_index


def main():
    init_db()
    httpd = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    print("盲评模块已启动: http://localhost:%d  (数据库 %s)" % (PORT, DB_PATH), flush=True)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        httpd.shutdown()


if __name__ == "__main__":
    main()
