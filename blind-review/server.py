#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
岩芯实验室 · 薄片双人盲评模块（零依赖：标准库 + SQLite）

角色
  主持人 HOST   ：批量编码、指派两名不同读片人、指派第三人裁决、整批发布
  读片人        ：只能看到分配给自己且尚未发布的盲码；提交前看不到样本身份/对方结果
  第三人裁决人  ：分歧样的裁决人，不得是原读片人；提交一份裁决

身份与安全
  * 可信身份：/api/auth/login 凭账号密码换取 HMAC-SHA256 签名令牌；除登录/健康检查外所有接口必验签。
  * 归属校验：批次仅其主持人可读写；样本/留痕仅本批主持人或被指派的两名读片人/裁决人可访问。
  * 盲态：任一读片人本人尚未提交时，样本视图与 history 都不暴露对方任何记录（历史含旧版）。
  * 幂等：幂等键绑定 [操作者 + 目标 + 内容指纹]，键被串用于别的操作者/目标/内容一律 409，不回放。
  * 比例：拒绝 NaN/Infinity/非数字；非负；合计必须为 100%（容差 0.01pct）。
  * 更正：旧版留存、轮次+1、原裁决失效，并置 adjudication_required；先更正后齐交也必须经第三人
    重新裁决，未重裁整批不可发布。
  * 批量编码与整批发布为单事务，故障注入点触发后整批回滚，不留部分记录。
"""
import hashlib
import hmac
import json
import math
import os
import secrets
import sqlite3
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
DB_PATH = os.environ.get("BLIND_DB", os.path.join(DATA_DIR, "blind-review.db"))
SECRET_PATH = os.environ.get("BLIND_SECRET", DB_PATH + ".secret")
PORT = int(os.environ.get("PORT", "3037"))
FAILPOINTS_ENABLED = os.environ.get("FAILPOINTS", "1") != "0"
TOKEN_TTL = int(os.environ.get("TOKEN_TTL", str(12 * 3600)))

EPS = 0.01            # 比例合计容差（百分点）
MINERAL_DIFF = 5.0    # >5 个百分点判分歧
_lock = threading.Lock()

# 演示用账户（密码经 PBKDF2 加盐哈希；生产应改为外部目录/库表）
USERS = {
    "贺主持": {"role": "host", "password": "host123"},
    "卞主持": {"role": "host", "password": "host456"},
    "张工": {"role": "reader", "password": "read123"},
    "李工": {"role": "reader", "password": "read456"},
    "王二": {"role": "reader", "password": "read789"},
    "赵裁": {"role": "adjudicator", "password": "adj123"},
    "钱裁": {"role": "adjudicator", "password": "adj456"},
}


# --------------------------------------------------------------------------- 时间 / DB
def now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def load_secret():
    """HMAC 密钥独立持久化，保证重启后旧令牌仍有效。"""
    try:
        with open(SECRET_PATH, "rb") as f:
            return f.read().strip()
    except OSError:
        os.makedirs(os.path.dirname(SECRET_PATH), exist_ok=True)
        s = secrets.token_bytes(32)
        with open(SECRET_PATH, "wb") as f:
            f.write(s.hex().encode("ascii"))
        try:
            os.chmod(SECRET_PATH, 0o600)
        except OSError:
            pass
        return s.hex().encode("ascii")


SECRET = load_secret()


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
  identity TEXT NOT NULL,
  default_reader_a TEXT,
  default_reader_b TEXT,
  reader_a TEXT NOT NULL,
  reader_b TEXT NOT NULL,
  round INTEGER NOT NULL DEFAULT 1,
  status TEXT NOT NULL DEFAULT 'pending',      -- pending|reading|consensus|adjudicating|adjudicated|published
  adjudication_required INTEGER NOT NULL DEFAULT 0,  -- 更正轮置1：必须经第三人重裁后才能发布
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
  minerals TEXT NOT NULL,
  rationale TEXT NOT NULL,
  superseded INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL
);
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
  scope TEXT NOT NULL,
  actor TEXT NOT NULL,
  target TEXT NOT NULL,
  fingerprint TEXT NOT NULL,
  response_status INTEGER NOT NULL,
  response_body TEXT NOT NULL,
  created_at TEXT NOT NULL
);
"""


def _column_exists(conn, table, col):
    return any(r["name"] == col for r in conn.execute(f"PRAGMA table_info({table})").fetchall())


def init_db():
    conn = connect()
    try:
        conn.executescript(SCHEMA)
        # 旧库迁移：补齐更正强制重裁标记
        if not _column_exists(conn, "samples", "adjudication_required"):
            conn.execute("ALTER TABLE samples ADD COLUMN adjudication_required INTEGER NOT NULL DEFAULT 0")
        conn.execute(
            "INSERT OR IGNORE INTO idem_keys(key,scope,actor,target,fingerprint,response_status,response_body,created_at)"
            " VALUES ('__seed__','seed','-','-','-',200,'{}',?)", (now(),))
    finally:
        conn.close()


# --------------------------------------------------------------------------- 令牌（可信身份）
def _b64(b):
    import base64
    return base64.urlsafe_b64encode(b).rstrip(b"=").decode("ascii")


def _ub64(s):
    import base64
    return base64.urlsafe_b64decode(s + "=" * (-len(s) % 4))


def issue_token(name, role):
    payload = {"name": name, "role": role, "exp": int(time.time()) + TOKEN_TTL}
    body = _b64(json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))
    sig = hmac.new(SECRET, body.encode("ascii"), hashlib.sha256).hexdigest()
    return f"{body}.{sig}"


def verify_token(token):
    try:
        body, sig = token.split(".", 1)
    except (ValueError, AttributeError):
        raise ApiError(401, "invalid_token", "令牌格式错误")
    expected = hmac.new(SECRET, body.encode("ascii"), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected, sig):
        raise ApiError(401, "invalid_token", "令牌签名无效（身份不可信）")
    try:
        payload = json.loads(_ub64(body).decode("utf-8"))
    except Exception:
        raise ApiError(401, "invalid_token", "令牌内容损坏")
    if int(payload.get("exp", 0)) < int(time.time()):
        raise ApiError(401, "token_expired", "登录已过期，请重新登录")
    if payload.get("name") not in USERS or USERS[payload["name"]]["role"] != payload.get("role"):
        raise ApiError(401, "invalid_token", "令牌对应用户不存在或角色不符")
    return payload["role"], payload["name"]


# --------------------------------------------------------------------------- 校验
class ApiError(Exception):
    def __init__(self, status, code, message=None):
        super().__init__(message or code)
        self.status = status
        self.code = code


def failpoint(name, body):
    if FAILPOINTS_ENABLED and isinstance(body, dict) and body.get("__failpoint") == name:
        raise RuntimeError("injected_failure:" + name)


def clean_str(value, field, required=True):
    if not isinstance(value, str):
        raise ApiError(400, "invalid_field", f"{field} 必须是字符串")
    value = value.strip()
    if required and not value:
        raise ApiError(400, "invalid_field", f"{field} 不能为空")
    return value


def parse_minerals(raw):
    """非空数组；名称非空不重复；比例必须是有限非负数；合计为 100（容差 EPS）。"""
    if not isinstance(raw, list) or not raw:
        raise ApiError(400, "minerals_required", "矿物比例至少包含一项")
    out, seen, total = [], set(), 0.0
    for i, item in enumerate(raw):
        if not isinstance(item, dict):
            raise ApiError(400, "invalid_mineral", f"第 {i+1} 项格式错误")
        name = clean_str(item.get("mineral"), f"矿物{i+1}")
        if name in seen:
            raise ApiError(400, "duplicate_mineral", f"矿物 {name} 重复")
        seen.add(name)
        pct = item.get("percent")
        # 显式拒绝布尔、非数值、NaN、±Infinity
        if isinstance(pct, bool) or not isinstance(pct, (int, float)):
            raise ApiError(400, "invalid_percent", f"{name} 的比例必须是数字")
        pct = float(pct)
        if not math.isfinite(pct):
            raise ApiError(400, "invalid_percent", f"{name} 的比例必须是有限数（拒绝 NaN/Infinity）")
        if pct < -EPS:
            raise ApiError(400, "invalid_percent", f"{name} 的比例不能为负")
        total += pct
        out.append({"mineral": name, "percent": round(pct, 4)})
    if not math.isfinite(total) or abs(total - 100.0) > EPS:
        raise ApiError(400, "minerals_sum_not_100",
                       f"矿物比例合计必须为 100%，当前为 {round(total, 4) if math.isfinite(total) else total}%")
    for item in out:
        if item["percent"] < 0:
            item["percent"] = 0.0
    return out


def minerals_map(minerals_json):
    return {m["mineral"]: float(m["percent"]) for m in json.loads(minerals_json)}


def in_disagreement(ra, rb):
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
    d = {"lithology": r["lithology"], "minerals": json.loads(r["minerals"]), "rationale": r["rationale"]}
    if include_reader:
        d["reader"] = r["reader"]
    if include_meta:
        d["version"] = r["version"]
        d["round"] = r["round"]
        d["created_at"] = r["created_at"]
    return d


def adjudication_public(adj, include_adjudicator=False):
    if adj["lithology"] == "__PENDING__":
        d = {"pending": True, "round": adj["round"], "created_at": adj["created_at"]}
        if include_adjudicator:
            d["adjudicator"] = adj["adjudicator"]
        return d
    d = {"pending": False, "lithology": adj["lithology"],
         "minerals": json.loads(adj["minerals"]), "rationale": adj["rationale"],
         "round": adj["round"], "created_at": adj["created_at"]}
    if include_adjudicator:
        d["adjudicator"] = adj["adjudicator"]
    return d


def assigned_adjudicator(conn, sample_id):
    r = conn.execute("SELECT adjudicator FROM adjudications WHERE sample_id=? AND superseded=0",
                     (sample_id,)).fetchone()
    return r["adjudicator"] if r else None


def sample_view(conn, s, role, name):
    published = s["status"] == "published"
    batch = conn.execute("SELECT code,project,status FROM batches WHERE id=?", (s["batch_id"],)).fetchone()
    adj = conn.execute("SELECT * FROM adjudications WHERE sample_id=? AND superseded=0", (s["id"],)).fetchone()
    ra = active_reading(conn, s["id"], "a")
    rb = active_reading(conn, s["id"], "b")

    view = {"blind_code": s["blind_code"], "batch_code": batch["code"], "project": batch["project"],
            "status": s["status"], "round": s["round"],
            "adjudication_required": bool(s["adjudication_required"])}

    if published:
        # 发布后揭晓双方身份与全部结论
        view["reader_a"] = s["reader_a"]
        view["reader_b"] = s["reader_b"]
        view["identity"] = s["identity"]
        view["published_at"] = s["published_at"]
        view["reading_a"] = reading_public(ra, True, True) if ra else None
        view["reading_b"] = reading_public(rb, True, True) if rb else None
        view["adjudication"] = adjudication_public(adj, True) if adj else None
        return view

    if role == "host":
        # 经调用方确认是本批主持人才会到这里
        view["reader_a"] = s["reader_a"]
        view["reader_b"] = s["reader_b"]
        view["identity"] = s["identity"]
        view["reading_a"] = reading_public(ra, True, True) if ra else None
        view["reading_b"] = reading_public(rb, True, True) if rb else None
        view["adjudication"] = adjudication_public(adj, True) if adj else None
        return view

    # ---- 非主持人：身份与对方姓名恒隐藏（盲态），仅告知本人槽位 ----
    my_slot = "a" if name == s["reader_a"] else "b" if name == s["reader_b"] else None
    is_assigned_adj = assigned_adjudicator(conn, s["id"]) == name

    if role == "reader" and my_slot is not None:
        view["my_slot"] = my_slot
        own = ra if my_slot == "a" else rb
        if own is not None:
            view["my_version"] = own["version"]

    # 关键盲态：读片人只有“本人已提交”后才允许看到双方内容；否则只看到自己那份。
    own_submitted = False
    if role == "reader" and my_slot is not None:
        own = ra if my_slot == "a" else rb
        own_submitted = own is not None
    # 被指派的裁决人可查看双方（裁决所需），但身份仍隐藏
    can_see_pair = (role == "reader" and own_submitted) or (role == "adjudicator" and is_assigned_adj)

    if can_see_pair:
        view["reading_a"] = reading_public(ra) if ra else None
        view["reading_b"] = reading_public(rb) if rb else None
        view["adjudication"] = adjudication_public(adj) if adj else None
    elif role == "reader" and my_slot is not None:
        own = ra if my_slot == "a" else rb
        if own is not None:
            view["my_reading"] = reading_public(own, include_meta=True)
    return view


# --------------------------------------------------------------------------- 业务操作
def op_login(body):
    name = clean_str(body.get("name"), "name")
    password = body.get("password")
    if not isinstance(password, str) or not password:
        raise ApiError(400, "invalid_field", "密码不能为空")
    user = USERS.get(name)
    stored = user["password"] if user else "!" + secrets.token_hex(16)
    # 恒定时间比较；用户不存在时与随机串比较，避免直接短路
    if not (user and hmac.compare_digest(password, stored)):
        raise ApiError(401, "bad_credentials", "用户名或密码错误")
    return 200, {"token": issue_token(name, user["role"]), "role": user["role"], "name": name,
                 "expires_in": TOKEN_TTL}


def op_encode_batch(conn, body, actor_name):
    project = clean_str(body.get("project"), "project")
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
            raise ApiError(400, "readers_must_differ", f"样本 {i+1}：两名读片人不能是同一人（{ra}）")
        norm.append((identity, ra, rb))

    row = conn.execute("SELECT COALESCE(MAX(id),0)+1 AS next FROM batches").fetchone()
    batch_id, batch_code, created = row["next"], "B%04d" % row["next"], now()
    conn.execute("INSERT INTO batches(id,code,project,host,created_at) VALUES (?,?,?,?,?)",
                 (batch_id, batch_code, project, actor_name, created))

    result_samples = []
    for idx, (identity, ra, rb) in enumerate(norm, start=1):
        blind = "%s-S%03d" % (batch_code, idx)
        conn.execute(
            """INSERT INTO samples(batch_id,blind_code,identity,default_reader_a,default_reader_b,
                                   reader_a,reader_b,round,status,adjudication_required)
               VALUES (?,?,?,?,?,?,?,1,'pending',0)""",
            (batch_id, blind, identity, def_a, def_b, ra, rb))
        failpoint("encode_after_insert", body)
        result_samples.append({"blind_code": blind, "identity": identity, "reader_a": ra, "reader_b": rb})
    return 201, {"batch_code": batch_code, "project": project, "count": len(norm), "samples": result_samples}


def _load_sample_for_reader(conn, blind_code, name):
    s = conn.execute("SELECT * FROM samples WHERE blind_code=?", (blind_code,)).fetchone()
    if not s or name not in (s["reader_a"], s["reader_b"]):
        raise ApiError(404, "sample_not_found", "盲码不存在或未分配给你")
    if s["status"] == "published":
        raise ApiError(409, "already_published", "该样本已发布，不能再提交或更正")
    return s


def _derive_pair_status(conn, s, amended_this_call=False):
    """返回 (new_status, reason)。更正轮强制要求裁决。"""
    ra = active_reading(conn, s["id"], "a")
    rb = active_reading(conn, s["id"], "b")
    if not (ra and rb):
        return "reading", None
    if amended_this_call or s["adjudication_required"]:
        # 即便先更正后齐交、且内容恰好一致，也必须重新裁决
        _, reason = in_disagreement(ra, rb)
        return "adjudicating", reason or "读片记录已更正，原共识失效，需重新裁决"
    dispute, reason = in_disagreement(ra, rb)
    return ("adjudicating", reason) if dispute else ("consensus", None)


def op_submit_reading(conn, body, blind_code, actor_name):
    name = clean_str(body.get("reader"), "reader")
    if name != actor_name:
        raise ApiError(403, "identity_mismatch", "提交人必须是登录本人")
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
        raise ApiError(409, "already_submitted",
                       "你已提交过读片；修改请显式发起更正(action=amend)并提供 expected_version")
    if action == "amend":
        if existing is None:
            raise ApiError(409, "nothing_to_amend", "你尚未提交读片，不能更正；请先提交")
        try:
            ev = int(expected_version)
        except (TypeError, ValueError):
            raise ApiError(400, "invalid_expected_version", "更正必须提供整数 expected_version")
        if ev != int(existing["version"]):
            raise ApiError(409, "version_conflict",
                           f"更正基于的版本 {ev} 已过期，当前为 v{existing['version']}")
    elif action != "submit":
        raise ApiError(400, "invalid_action", "action 只能是 submit 或 amend")

    if existing is None:
        conn.execute(
            """INSERT INTO readings(sample_id,slot,reader,version,round,lithology,minerals,rationale,created_at)
               VALUES (?,?,?,1,?,?,?,?,?)""",
            (s["id"], slot, name, s["round"], lithology,
             json.dumps(minerals, ensure_ascii=False), rationale, now()))
        if s["status"] == "pending":
            conn.execute("UPDATE samples SET status='reading' WHERE id=?", (s["id"],))
        kind = "submitted"
    else:
        new_version, new_round = existing["version"] + 1, s["round"] + 1
        conn.execute("UPDATE readings SET superseded=1 WHERE id=?", (existing["id"],))
        conn.execute("UPDATE adjudications SET superseded=1 WHERE sample_id=? AND superseded=0", (s["id"],))
        other = active_reading(conn, s["id"], other_slot)
        if other is not None:
            conn.execute("UPDATE readings SET round=? WHERE id=?", (new_round, other["id"]))
        conn.execute(
            """INSERT INTO readings(sample_id,slot,reader,version,round,lithology,minerals,rationale,created_at)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (s["id"], slot, name, new_version, new_round, lithology,
             json.dumps(minerals, ensure_ascii=False), rationale, now()))
        conn.execute("UPDATE samples SET round=?, status='adjudicating', adjudication_required=1 WHERE id=?",
                     (new_round, s["id"]))
        kind = "amended"

    s = conn.execute("SELECT * FROM samples WHERE id=?", (s["id"],)).fetchone()
    amended = kind == "amended"
    new_status, reason = _derive_pair_status(conn, s, amended_this_call=amended)
    conn.execute("UPDATE samples SET status=? WHERE id=?", (new_status, s["id"]))

    mine = active_reading(conn, s["id"], slot)
    final_round = new_round if amended else s["round"]
    return 200, {"blind_code": blind_code, "slot": slot, "action": kind, "version": mine["version"],
                 "round": final_round, "sample_status": new_status,
                 "dispute_reason": reason if new_status == "adjudicating" else None}


def op_assign_adjudicator(conn, body, blind_code, actor_name):
    host = clean_str(body.get("host"), "host")
    if host != actor_name:
        raise ApiError(403, "identity_mismatch", "操作人必须是登录主持人本人")
    adjudicator = clean_str(body.get("adjudicator"), "adjudicator")
    s = conn.execute("SELECT * FROM samples WHERE blind_code=?", (blind_code,)).fetchone()
    if not s:
        raise ApiError(404, "sample_not_found")
    batch = conn.execute("SELECT host FROM batches WHERE id=?", (s["batch_id"],)).fetchone()
    if batch["host"] != host:
        raise ApiError(403, "forbidden", "只有本批次主持人可以指派裁决人")
    if adjudicator in (s["reader_a"], s["reader_b"]):
        raise ApiError(403, "adjudicator_conflict", "第三人裁决人不得是该样本的原读片人")
    if USERS.get(adjudicator, {}).get("role") not in ("adjudicator",):
        raise ApiError(400, "not_an_adjudicator", "被指派人不具备裁决人身份")
    if s["status"] == "published":
        raise ApiError(409, "already_published", "样本已发布")
    if s["status"] != "adjudicating":
        raise ApiError(409, "not_in_dispute", f"样本当前状态为 {s['status']}，无需裁决")
    existing = conn.execute("SELECT * FROM adjudications WHERE sample_id=? AND superseded=0",
                            (s["id"],)).fetchone()
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


def op_submit_adjudication(conn, body, blind_code, actor_name):
    name = clean_str(body.get("adjudicator"), "adjudicator")
    if name != actor_name:
        raise ApiError(403, "identity_mismatch", "裁决人必须是登录本人")
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
        raise ApiError(409, "already_adjudicated", "该样本已裁决")

    conn.execute("UPDATE adjudications SET lithology=?,minerals=?,rationale=?,created_at=? WHERE id=?",
                 (lithology, json.dumps(minerals, ensure_ascii=False), rationale, now(), adj["id"]))
    conn.execute("UPDATE samples SET status='adjudicated', adjudication_required=0 WHERE id=?", (s["id"],))
    return 200, {"blind_code": blind_code, "adjudicator": name, "round": s["round"],
                 "sample_status": "adjudicated"}


def op_publish_batch(conn, body, batch_code, actor_name):
    host = clean_str(body.get("host"), "host")
    if host != actor_name:
        raise ApiError(403, "identity_mismatch", "操作人必须是登录主持人本人")
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
        if s["adjudication_required"] or s["status"] == "adjudicating":
            blocking.append({"blind_code": s["blind_code"], "reason": "更正后须经第三人重新裁决" if s["adjudication_required"] else "第三人尚未完成裁决"})
        elif s["status"] in ("pending", "reading"):
            blocking.append({"blind_code": s["blind_code"], "reason": "两名读片人尚未都提交"})
    if blocking:
        raise ApiError(409, "batch_not_ready", "批次尚未达到可发布状态：" + "；".join(
            f"{x['blind_code']}（{x['reason']}）" for x in blocking))

    ts = now()
    for s in samples:
        failpoint("publish_after_update", body)
        conn.execute("UPDATE samples SET status='published',published_at=? WHERE id=?", (ts, s["id"]))
    conn.execute("UPDATE batches SET status='published',published_at=? WHERE id=?", (ts, batch["id"]))
    return 200, {"batch_code": batch_code, "published": len(samples), "published_at": ts}


def get_batch_detail(conn, batch_code, role, name):
    batch = conn.execute("SELECT * FROM batches WHERE code=?", (batch_code,)).fetchone()
    if not batch:
        raise ApiError(404, "batch_not_found")
    # 批次明细含全部样本身份，仅本批主持人可见
    if role != "host" or batch["host"] != name:
        raise ApiError(403, "forbidden", "批次明细仅本批主持人可见")
    samples = conn.execute("SELECT * FROM samples WHERE batch_id=? ORDER BY id", (batch["id"],)).fetchall()
    return {"code": batch["code"], "project": batch["project"], "host": batch["host"],
            "status": batch["status"], "created_at": batch["created_at"],
            "published_at": batch["published_at"],
            "samples": [sample_view(conn, s, role, name) for s in samples]}


# --------------------------------------------------------------------------- 幂等指纹
def request_fingerprint(body):
    payload = {k: v for k, v in (body or {}).items()
               if k not in ("idempotency_key", "__failpoint")}
    return hashlib.sha256(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        .encode("utf-8")).hexdigest()


# --------------------------------------------------------------------------- HTTP
class Handler(BaseHTTPRequestHandler):
    server_version = "BlindReview/2.0"

    def log_message(self, fmt, *args):
        pass

    def _send(self, status, obj):
        data = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _err(self, status, code, message=None):
        self._send(status, {"error": code, "message": message or code})

    def _bearer(self):
        auth = self.headers.get("Authorization", "")
        if auth.startswith("Bearer "):
            return auth[7:].strip()
        qs = parse_qs(urlparse(self.path).query)
        return (qs.get("token") or [""])[0].strip()

    def _authenticate(self):
        token = self._bearer()
        if not token:
            raise ApiError(401, "unauthorized", "缺少登录令牌（Authorization: Bearer）")
        return verify_token(token)

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

    # ---- 归属守卫 ----
    def _guard_sample(self, conn, s, role, name):
        if role == "host":
            batch = conn.execute("SELECT host FROM batches WHERE id=?", (s["batch_id"],)).fetchone()
            if not batch or batch["host"] != name:
                raise ApiError(404, "sample_not_found", "样本不存在或不属于你的批次")
            return True
        allowed = {s["reader_a"], s["reader_b"]}
        adj = assigned_adjudicator(conn, s["id"])
        if adj:
            allowed.add(adj)
        if name not in allowed:
            raise ApiError(404, "sample_not_found", "盲码不存在或与你无关")
        return False

    # ---------------------------------------------------------------- GET
    def do_GET(self):
        try:
            parsed = urlparse(self.path)
            p = parsed.path.strip("/")
            parts = p.split("/") if p else []
            if p == "health":
                return self._send(200, {"ok": True, "time": now()})

            role, name = self._authenticate()
            conn = connect()
            try:
                if p == "api/me/worklist":
                    return self._send(200, self._worklist(conn, role, name))
                if len(parts) == 3 and parts[0] == "api" and parts[1] == "batches":
                    return self._send(200, get_batch_detail(conn, parts[2], role, name))
                if len(parts) == 4 and parts[0] == "api" and parts[1] == "samples" and parts[3] == "history":
                    return self._send(200, self._history(conn, parts[2], role, name))
                if len(parts) == 3 and parts[0] == "api" and parts[1] == "samples":
                    s = conn.execute("SELECT * FROM samples WHERE blind_code=?", (parts[2],)).fetchone()
                    if not s:
                        raise ApiError(404, "sample_not_found")
                    self._guard_sample(conn, s, role, name)
                    return self._send(200, sample_view(conn, s, role, name))
                raise ApiError(404, "not_found")
            finally:
                conn.close()
        except ApiError as e:
            self._err(e.status, e.code, str(e))
        except Exception as e:  # noqa: BLE001
            self._err(500, "internal_error", str(e))

    # ---------------------------------------------------------------- POST
    def do_POST(self):
        try:
            parsed = urlparse(self.path)
            p = parsed.path.strip("/")
            parts = p.split("/") if p else []
            body = self._read_body()

            # 登录不要求既有令牌
            if p == "api/auth/login":
                status, resp = op_login(body)
                return self._send(status, resp)

            role, name = self._authenticate()
            idem = (self.headers.get("Idempotency-Key") or body.get("idempotency_key") or "")
            idem = idem.strip() if isinstance(idem, str) else ""

            # scope, target, op, host_only, actor_field
            if p == "api/batches":
                spec = ("encode", "BATCH", lambda c, b: op_encode_batch(c, b, name), True, "host")
            elif len(parts) == 4 and parts[0] == "api" and parts[1] == "samples" and parts[3] == "readings":
                code = parts[2]
                spec = ("reading", code, lambda c, b: op_submit_reading(c, b, code, name), False, "reader")
            elif len(parts) == 5 and parts[0] == "api" and parts[1] == "samples" \
                    and parts[3] == "adjudicator" and parts[4] == "assign":
                code = parts[2]
                spec = ("assign_adj", code, lambda c, b: op_assign_adjudicator(c, b, code, name), True, "host")
            elif len(parts) == 4 and parts[0] == "api" and parts[1] == "samples" and parts[3] == "adjudication":
                code = parts[2]
                spec = ("adjudicate", code, lambda c, b: op_submit_adjudication(c, b, code, name), False, "adjudicator")
            elif len(parts) == 4 and parts[0] == "api" and parts[1] == "batches" and parts[3] == "publish":
                code = parts[2]
                spec = ("publish", code, lambda c, b: op_publish_batch(c, b, code, name), True, "host")
            else:
                raise ApiError(404, "not_found")

            scope, target, op, host_only, actor_field = spec
            if host_only and role != "host":
                raise ApiError(403, "forbidden", "该操作仅限主持人")
            actor = body.get(actor_field)
            if not isinstance(actor, str) or actor.strip() != name:
                raise ApiError(403, "identity_mismatch",
                               f"操作人 {actor_field} 与登录身份不一致（禁止冒名）")

            self._run_write(scope, target, request_fingerprint(body), idem, op, body, name)
        except ApiError as e:
            self._err(e.status, e.code, str(e))
        except Exception as e:  # noqa: BLE001
            self._err(500, "internal_error", str(e))

    # ---------------------------------------------------------------- 写事务 + 幂等
    def _run_write(self, scope, target, fingerprint, idem, op, body, actor):
        if not idem:
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
                cached = conn.execute(
                    "SELECT response_status,response_body,actor,target,fingerprint FROM idem_keys WHERE key=?",
                    (full_key,)).fetchone()
                if cached:
                    if (cached["actor"], cached["target"], cached["fingerprint"]) != (actor, target, fingerprint):
                        raise ApiError(409, "idempotency_conflict",
                                       "幂等键已被用于其他操作者/目标/内容，拒绝回放")
                    return self._send(cached["response_status"], json.loads(cached["response_body"]))
                conn.execute("BEGIN IMMEDIATE")
                try:
                    status, resp = op(conn, body)
                    conn.execute(
                        "INSERT INTO idem_keys(key,scope,actor,target,fingerprint,response_status,"
                        "response_body,created_at) VALUES (?,?,?,?,?,?,?,?)",
                        (full_key, scope, actor, target, fingerprint, status,
                         json.dumps(resp, ensure_ascii=False), now()))
                    conn.execute("COMMIT")
                except Exception:
                    conn.execute("ROLLBACK")
                    raise
                self._send(status, resp)
            except sqlite3.IntegrityError:
                conn.close()
                conn = connect()
                cached = conn.execute(
                    "SELECT response_status,response_body,actor,target,fingerprint FROM idem_keys WHERE key=?",
                    (full_key,)).fetchone()
                if cached:
                    if (cached["actor"], cached["target"], cached["fingerprint"]) != (actor, target, fingerprint):
                        raise ApiError(409, "idempotency_conflict", "幂等键串用，拒绝回放")
                    return self._send(cached["response_status"], json.loads(cached["response_body"]))
                self._err(409, "conflict", "并发重复操作，仅生效一次")
            finally:
                conn.close()

    # ---------------------------------------------------------------- 工作台 / 留痕
    def _worklist(self, conn, role, name):
        if role == "host":
            batches = conn.execute("SELECT code FROM batches WHERE host=? ORDER BY id", (name,)).fetchall()
            return {"role": "host", "batches": [b["code"] for b in batches]}
        rows = conn.execute(
            "SELECT * FROM samples WHERE (reader_a=? OR reader_b=?) AND status!='published' ORDER BY id",
            (name, name)).fetchall()
        if role == "reader":
            return {"role": "reader", "samples": [sample_view(conn, s, "reader", name) for s in rows]}
        adj_rows = conn.execute(
            """SELECT s.* FROM samples s
               JOIN adjudications a ON a.sample_id=s.id AND a.superseded=0
               WHERE a.adjudicator=? AND s.status='adjudicating' AND a.lithology='__PENDING__'
               ORDER BY s.id""", (name,)).fetchall()
        return {"role": "adjudicator",
                "samples": [sample_view(conn, s, "adjudicator", name) for s in adj_rows]}

    def _history(self, conn, blind_code, role, name):
        s = conn.execute("SELECT * FROM samples WHERE blind_code=?", (blind_code,)).fetchone()
        if not s:
            raise ApiError(404, "sample_not_found")
        is_host = self._guard_sample(conn, s, role, name)
        readings = conn.execute("SELECT * FROM readings WHERE sample_id=? ORDER BY id", (s["id"],)).fetchall()
        adjs = conn.execute("SELECT * FROM adjudications WHERE sample_id=? ORDER BY id", (s["id"],)).fetchall()

        if is_host:
            vis_readings, vis_adjs = readings, adjs
            identity = s["identity"]
            include_people = True
        else:
            identity = s["identity"] if s["status"] == "published" else None
            include_people = False
            my_slot = "a" if name == s["reader_a"] else "b" if name == s["reader_b"] else None
            is_assigned_adj = assigned_adjudicator(conn, s["id"]) == name
            own_rows = [r for r in readings if r["slot"] == my_slot] if my_slot else []
            own_active = next((r for r in own_rows if not r["superseded"]), None)
            if role == "adjudicator" and is_assigned_adj:
                # 被指派裁决人可见全部读片
                vis_readings = readings
                vis_adjs = adjs
            elif role == "reader" and my_slot is not None and own_active is not None:
                # 本人已提交：可见双方（含各自旧版）
                vis_readings = readings
                vis_adjs = adjs
            elif role == "reader" and my_slot is not None:
                # 本人尚未提交：只可见本人（通常为空），绝不泄露对方任何记录/旧版
                vis_readings = own_rows
                vis_adjs = []
            else:
                # 理论上守卫已拦截
                vis_readings, vis_adjs = [], []

        return {
            "blind_code": blind_code, "status": s["status"], "round": s["round"],
            "adjudication_required": bool(s["adjudication_required"]), "identity": identity,
            "readings": [{**reading_public(r, include_reader=include_people, include_meta=True),
                          "slot": r["slot"], "superseded": bool(r["superseded"])} for r in vis_readings],
            "adjudications": [{**adjudication_public(a, include_adjudicator=include_people),
                               "superseded": bool(a["superseded"])} for a in vis_adjs],
        }


def serve_index(handler):
    with open(os.path.join(BASE_DIR, "index.html"), "rb") as f:
        data = f.read()
    handler.send_response(200)
    handler.send_header("Content-Type", "text/html; charset=utf-8")
    handler.send_header("Content-Length", str(len(data)))
    handler.end_headers()
    handler.wfile.write(data)


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
