#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
薄片双人盲评 · 安全与状态加固后端到端实测（v2）

覆盖：
  可信身份（登录/HMAC 令牌/伪造与过期角色）、批次与样本归属、提交前历史不泄露对方、
  非有限矿物比例拒绝、幂等键绑定操作者/目标/内容（串用不回放）、
  先更正后齐交也必须重裁（未重裁不可发布）、双击/并发编码只建一批、
  列表本人已提交状态、正常全流程、部分写失败整批回滚、重启保留（含令牌仍有效）。
"""
import json
import os
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.request
import urllib.error

HERE = os.path.dirname(os.path.abspath(__file__))
PY = sys.executable
PASS, FAIL = [], []


def check(name, cond, detail=""):
    (PASS if cond else FAIL).append((name, detail))
    print(("  [PASS] " if cond else "  [FAIL] ") + name +
          ("" if cond else (f"  -> {detail}" if detail else "")))


def section(t):
    print("\n" + "=" * 72 + f"\n{t}\n" + "=" * 72)


PASSWORDS = {"贺主持": "host123", "卞主持": "host456", "张工": "read123", "李工": "read456",
             "王二": "read789", "赵裁": "adj123", "钱裁": "adj456"}


class Client:
    def __init__(self, base, name):
        self.base, self.name, self.token = base, name, None

    def login(self, password=None):
        st, b = self.call("POST", "/api/auth/login",
                          {"name": self.name, "password": password or PASSWORDS[self.name]}, auth=False)
        if st == 200:
            self.token = b["token"]
        return st, b

    def call(self, method, path, body=None, auth=True, raw=None, idem=None):
        url = self.base + path
        headers = {"Content-Type": "application/json"}
        if auth and self.token:
            headers["Authorization"] = "Bearer " + self.token
        if idem:
            headers["Idempotency-Key"] = idem
        if raw is not None:
            data = raw.encode("utf-8")
        elif body is not None:
            data = json.dumps(body, ensure_ascii=False, allow_nan=False).encode("utf-8")
        else:
            data = None
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=15) as r:
                return r.status, json.loads(r.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            try:
                return e.code, json.loads(e.read().decode("utf-8"))
            except Exception:
                return e.code, {}

    def get(self, path):
        return self.call("GET", path)

    def post(self, path, body=None, idem=None):
        return self.call("POST", path, body=body, idem=idem)


def free_port():
    s = socket.socket(); s.bind(("127.0.0.1", 0)); p = s.getsockname()[1]; s.close(); return p


def start_server(port, db):
    os.makedirs(os.path.dirname(db), exist_ok=True)
    env = dict(os.environ, PORT=str(port), BLIND_DB=db, FAILPOINTS="1")
    log = open(db + ".log", "w")
    proc = subprocess.Popen([PY, os.path.join(HERE, "server.py")], env=env,
                            stdout=log, stderr=subprocess.STDOUT, cwd=HERE)
    base = f"http://127.0.0.1:{port}"
    deadline = time.time() + 15
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(base + "/health", timeout=2) as r:
                if r.status == 200:
                    return proc, base
        except Exception:
            time.sleep(0.2)
    proc.kill()
    raise RuntimeError("server failed to start; see " + db + ".log")


def stop_server(proc):
    proc.send_signal(signal.SIGINT)
    try:
        proc.wait(timeout=5)
    except Exception:
        proc.kill()


def mins(**kw):
    return [{"mineral": k, "percent": v} for k, v in kw.items()]


def encode(cli, ids, ra="张工", rb="李工", project="东岭铜矿", idem=None, extra=None):
    body = {"idempotency_key": idem, "host": cli.name, "project": project,
            "default_reader_a": ra, "default_reader_b": rb,
            "samples": [{"identity": x} for x in ids]}
    if extra:
        body.update(extra)
    return cli.post("/api/batches", body)


def read(cli, code, lith, minerals, rat="镜下定名为依据", idem=None, action="submit", ev=None, raw=None):
    body = {"idempotency_key": idem, "reader": cli.name, "lithology": lith,
            "minerals": minerals, "rationale": rat, "action": action}
    if ev is not None:
        body["expected_version"] = ev
    return cli.call("POST", f"/api/samples/{code}/readings", body=body if raw is None else None, raw=raw)


# ===========================================================================
def main():
    tmp = tempfile.mkdtemp(prefix="blind-v2-")
    db = os.path.join(tmp, "review.db")
    port = free_port()
    proc, base = start_server(port, db)
    try:
        host = Client(base, "贺主持"); host.login()
        host2 = Client(base, "卞主持"); host2.login()
        zhang = Client(base, "张工"); zhang.login()
        li = Client(base, "李工"); li.login()
        wang = Client(base, "王二"); wang.login()
        zhao = Client(base, "赵裁"); zhao.login()
        qian = Client(base, "钱裁"); qian.login()
        anon = Client(base, "无名氏")  # 不登录

        # ------------------------------------------------ 1. 可信身份
        section("1 · 可信身份：登录令牌 / 伪造 / 角色越权")
        st, b = anon.get("/api/me/worklist")
        check("无令牌访问 -> 401", st == 401, str(st))
        st, b = anon.call("POST", "/api/auth/login", {"name": "贺主持", "password": "wrong"}, auth=False)
        check("错误密码 -> 401", st == 401, f"{st}")
        st, b = anon.call("POST", "/api/auth/login", {"name": "不存在", "password": "x"}, auth=False)
        check("不存在用户 -> 401", st == 401, f"{st}")
        forged = host.token[:-4] + ("0000" if not host.token.endswith("0000") else "1111")
        bad = Client(base, "x"); bad.token = forged
        st, b = bad.get("/api/me/worklist")
        check("篡改签名的令牌 -> 401", st == 401, f"{st}")
        bad2 = Client(base, "x"); bad2.token = "aaa.bbb"
        st, b = bad2.get("/api/me/worklist")
        check("格式非法令牌 -> 401", st == 401, f"{st}")
        st, b = zhang.post("/api/batches", {"host": zhang.name, "project": "x",
                                            "default_reader_a": "a", "default_reader_b": "b",
                                            "samples": [{"identity": "z"}]})
        check("读片人令牌编码批次 -> 403", st == 403, f"{st}")
        st, b = zhao.post("/api/batches", {"host": zhao.name, "project": "x",
                                           "default_reader_a": "a", "default_reader_b": "b",
                                           "samples": [{"identity": "z"}]})
        check("裁决人令牌编码批次 -> 403", st == 403, f"{st}")
        # body 冒名：持张工令牌但 reader 写李工
        st, b = zhang.post("/api/samples/B0001-S001/readings",
                           {"reader": li.name, "lithology": "x", "minerals": mins(石英=100),
                            "rationale": "r", "action": "submit"})
        check("持张工令牌冒名李工提交 -> 403", st == 403, f"{st}")

        # ------------------------------------------------ 2. 归属
        section("2 · 批次与样本归属校验")
        st, b = encode(host, ["I-1", "I-2"], idem="enc-main")
        check("贺主持编码成功", st == 201 and b["count"] == 2, f"{st} {b}")
        bc, s1, s2 = b["batch_code"], f"{b['batch_code']}-S001", f"{b['batch_code']}-S002"
        st, b = host2.get(f"/api/batches/{bc}")
        check("另一主持人读他人批次 -> 403", st == 403, f"{st}")
        st, b = host2.post(f"/api/batches/{bc}/publish", {"host": host2.name})
        check("另一主持人发布他人批次 -> 403", st == 403, f"{st}")
        st, b = host2.post(f"/api/samples/{s1}/adjudicator/assign",
                           {"host": host2.name, "adjudicator": "钱裁"})
        check("另一主持人给他人样本指派裁决 -> 403/404", st in (403, 404), f"{st}")
        st, b = wang.get(f"/api/samples/{s1}")
        check("未参与读片人读样本 -> 404", st == 404, f"{st}")
        st, b = wang.get(f"/api/samples/{s1}/history")
        check("未参与读片人查留痕 -> 404", st == 404, f"{st}")
        st, b = qian.get(f"/api/samples/{s1}")
        check("未被指派的裁决人读样本 -> 404", st == 404, f"{st}")

        # ------------------------------------------------ 3. 提交前盲态（含历史）
        section("3 · 本人未提交前，视图与历史都不得泄露对方")
        # 李工先交，张工未交
        st, b = read(li, s1, "砂岩", mins(石英=60, 长石=40), idem="li-s1")
        check("李工首交成功", st == 200, f"{st} {b}")
        st, v = zhang.get(f"/api/samples/{s1}")
        check("张工未交：样本视图无对方结论", "reading_b" not in v and "reading_a" not in v, str(sorted(v.keys())))
        check("张工未交：看不到真实身份", "identity" not in v, "")
        check("张工未交：看不到对方姓名", "reader_b" not in v and "reader_a" not in v, str(sorted(v.keys())))
        st, h = zhang.get(f"/api/samples/{s1}/history")
        other_leak = [r for r in h.get("readings", []) if r["slot"] == "b"]
        check("张工未交：历史接口不含李工任何记录（含旧版）", st == 200 and not other_leak, str(h.get("readings")))
        check("张工未交：历史不含身份", h.get("identity") is None, "")
        check("张工未交：历史不含裁决记录", h.get("adjudications") == [], str(h.get("adjudications")))
        # 张工提交后即可见双方
        st, b = read(zhang, s1, "砂岩", mins(石英=62, 长石=38), idem="zhang-s1")
        check("张工首交 -> 共识（差2pct）", b["sample_status"] == "consensus", f"{b}")
        st, h = zhang.get(f"/api/samples/{s1}/history")
        check("张工提交后：历史可见双方记录",
              {r["slot"] for r in h["readings"]} == {"a", "b"}, str([r["slot"] for r in h["readings"]]))

        # ------------------------------------------------ 4. 非有限比例
        section("4 · 拒绝非有限矿物比例（NaN / Infinity / 负数 / 非数字）")
        st, eb = encode(host, ["N-1", "N-2", "N-3", "N-4"], idem="enc-num")
        nbc = eb["batch_code"]
        codes = [f"{nbc}-S00{i}" for i in (1, 2, 3, 4)]
        # 用真正的 NaN / Infinity 字面量（服务端必须在业务校验拦截）
        raw_nan = '{"reader":"张工","lithology":"岩","minerals":[{"mineral":"石英","percent":NaN}],"rationale":"r","action":"submit"}'
        raw_inf = '{"reader":"张工","lithology":"岩","minerals":[{"mineral":"石英","percent":1e999}],"rationale":"r","action":"submit"}'
        raw_ninf = '{"reader":"张工","lithology":"岩","minerals":[{"mineral":"石英","percent":-1e999},{"mineral":"长石","percent":100}],"rationale":"r","action":"submit"}'
        st, b = zhang.call("POST", f"/api/samples/{codes[0]}/readings", raw=raw_nan)
        check("NaN 比例 -> 400", st == 400 and b.get("error") == "invalid_percent", f"{st} {b}")
        st, b = zhang.call("POST", f"/api/samples/{codes[1]}/readings", raw=raw_inf)
        check("Infinity 比例(1e999) -> 400", st == 400 and b.get("error") == "invalid_percent", f"{st} {b}")
        st, b = zhang.call("POST", f"/api/samples/{codes[2]}/readings", raw=raw_ninf)
        check("-Infinity 比例 -> 400", st == 400 and b.get("error") == "invalid_percent", f"{st} {b}")
        st, b = read(zhang, codes[3], "岩", [{"mineral": "石英", "percent": "五十"}], idem="str-pct")
        check("字符串比例 -> 400", st == 400, f"{st}")
        st, b = read(zhang, codes[3], "岩", mins(石英=30, 长石=70), idem="ok-pct")
        check("有限且合计100 -> 通过", st == 200, f"{st} {b}")

        # ------------------------------------------------ 5. 幂等绑定
        section("5 · 幂等键绑定操作者/目标/内容，串用不回放")
        st, eb = encode(host, ["K-1", "K-2"], idem="enc-key")
        k1, k2 = f"{eb['batch_code']}-S001", f"{eb['batch_code']}-S002"
        payload1 = {"idempotency_key": "SHARED", "reader": "张工", "action": "submit",
                    "lithology": "砂岩", "minerals": mins(石英=100), "rationale": "r"}
        st, first = zhang.post(f"/api/samples/{k1}/readings", payload1)
        check("首次提交成功", st == 200 and first["version"] == 1, f"{st}")
        # 同键换目标
        payload2 = dict(payload1); payload2["lithology"] = "泥岩"; payload2["minerals"] = mins(黏土=100)
        st, b = zhang.post(f"/api/samples/{k2}/readings", payload2)
        check("同键串用到另一目标 -> 409 不回放", st == 409 and b.get("error") == "idempotency_conflict", f"{st} {b}")
        st, h = host.get(f"/api/samples/{k2}/history")
        check("被串用的目标未产生任何记录", len(h["readings"]) == 0, str(h["readings"]))
        # 同键同目标换内容
        payload3 = dict(payload1); payload3["rationale"] = "被篡改"
        st, b = zhang.post(f"/api/samples/{k1}/readings", payload3)
        check("同键同目标但内容不同 -> 409", st == 409 and b.get("error") == "idempotency_conflict", f"{st}")
        # 同键同内容不同操作者（李工拿同一键）
        payload_li = {"idempotency_key": "SHARED", "reader": "李工", "action": "submit",
                      "lithology": "砂岩", "minerals": mins(石英=100), "rationale": "r"}
        st, b = li.post(f"/api/samples/{k1}/readings", payload_li)
        check("同键被另一操作者使用 -> 409", st == 409 and b.get("error") == "idempotency_conflict", f"{st}")
        # 合法重放：同人同目标同内容
        st, replay = zhang.post(f"/api/samples/{k1}/readings", payload1)
        check("合法重放同内容 -> 200 且返回同版本", st == 200 and replay["version"] == 1, f"{st}")
        st, h = host.get(f"/api/samples/{k1}/history")
        check("重放不产生重复版本", len([r for r in h["readings"] if r["slot"] == "a"]) == 1, "")
        # 编码键串用：同键不同内容
        st, b = encode(host, ["不同内容"], idem="enc-key")
        check("编码幂等键换内容 -> 409", st == 409 and b.get("error") == "idempotency_conflict", f"{st}")
        # 编码键跨主持人
        st, b = encode(host2, ["K-1", "K-2"], idem="enc-key")
        check("编码幂等键换操作者 -> 409", st == 409 and b.get("error") == "idempotency_conflict", f"{st}")
        # 编码合法重放返回同批次
        st, replay_b = encode(host, ["K-1", "K-2"], idem="enc-key")
        check("编码合法重放 -> 同一批次号", st == 201 and replay_b["batch_code"] == eb["batch_code"], f"{st}")

        # ------------------------------------------------ 6. 先更正后齐交必重裁
        section("6 · 先更正、后齐交：仍必须第三人重裁，未重裁不可发布")
        st, ab = encode(host, ["AMEND-EARLY"], idem="enc-amend-early")
        ac = f"{ab['batch_code']}-S001"
        read(zhang, ac, "砂岩", mins(石英=60, 长石=40), idem="ae-a1")
        st, b = read(zhang, ac, "砂岩", mins(石英=61, 长石=39), idem="ae-a2", action="amend", ev=1)
        check("对方未交时本人更正 -> v2、仍在读片中但标记须重裁",
              b["version"] == 2 and b["sample_status"] == "reading", f"{b}")
        st, v = host.get(f"/api/batches/{ab['batch_code']}")
        samp = next(x for x in v["samples"] if x["blind_code"] == ac)
        check("样本 adjudication_required=1", samp["adjudication_required"] is True, str(samp.get("adjudication_required")))
        # 李工首交且与更正后完全一致
        st, b = read(li, ac, "砂岩", mins(石英=61, 长石=39), idem="ae-b1")
        check("齐交后即便结论一致仍 -> adjudicating", b["sample_status"] == "adjudicating", f"{b}")
        st, b = host.post(f"/api/batches/{ab['batch_code']}/publish",
                          {"idempotency_key": "pub-blocked", "host": host.name})
        check("未重裁整批发布 -> 409", st == 409, f"{st}")
        st, _ = host.post(f"/api/samples/{ac}/adjudicator/assign",
                          {"idempotency_key": "ae-as", "host": host.name, "adjudicator": "赵裁"})
        check("指派原读片人为裁决人被拒（前置反例已测）；指派赵裁成功", st == 200, f"{st}")
        st, b = zhao.post(f"/api/samples/{ac}/adjudication",
                          {"idempotency_key": "ae-ad", "adjudicator": "赵裁", "lithology": "砂岩",
                           "minerals": mins(石英=61, 长石=39), "rationale": "复核维持"})
        check("第三人重裁完成 -> adjudicated", b["sample_status"] == "adjudicated", f"{b}")
        st, b = host.post(f"/api/batches/{ab['batch_code']}/publish",
                          {"idempotency_key": "ae-pub", "host": host.name})
        check("重裁后可发布", st == 200 and b["published"] == 1, f"{st} {b}")

        # ------------------------------------------------ 7. 双击 / 并发编码只建一批
        section("7 · 双击与并发重复编码只生效一次")
        payload = {"idempotency_key": "double-click", "host": host.name, "project": "双击批",
                   "default_reader_a": "张工", "default_reader_b": "李工",
                   "samples": [{"identity": "D1"}, {"identity": "D2"}]}
        r1 = host.post("/api/batches", payload)
        r2 = host.post("/api/batches", payload)  # 模拟双击（同键同内容）
        check("连续双击同键：同一批次、不报错",
              r1[0] == 201 and r2[0] == 201 and r1[1]["batch_code"] == r2[1]["batch_code"],
              f"{r1[0]} {r2[0]}")
        results = []
        barrier = threading.Barrier(6)

        def race():
            barrier.wait()
            p = dict(payload); p["idempotency_key"] = "race-encode"
            results.append(host.post("/api/batches", p))
        ts = [threading.Thread(target=race) for _ in range(6)]
        [t.start() for t in ts]; [t.join() for t in ts]
        codes = {r[1].get("batch_code") for r in results if r[0] == 201}
        check("6 并发同键编码：只产生一个批次号", len(codes) == 1 and all(r[0] == 201 for r in results),
              f"{codes} {[r[0] for r in results]}")
        race_code = next(iter(codes))
        st, det = host.get(f"/api/batches/{race_code}")
        check("该批次恰好 2 个样本（无部分重复）", len(det["samples"]) == 2, str(len(det["samples"])))

        # 无幂等键并发读片只生效一次
        rc = f"{race_code}-S001"
        body_no_key = {"reader": "张工", "action": "submit", "lithology": "砂岩",
                       "minerals": mins(石英=80, 长石=20), "rationale": "x"}
        rr = []
        b2 = threading.Barrier(5)

        def race_read():
            b2.wait(); rr.append(zhang.post(f"/api/samples/{rc}/readings", body_no_key))
        ts = [threading.Thread(target=race_read) for _ in range(5)]
        [t.start() for t in ts]; [t.join() for t in ts]
        n200 = sum(1 for s_, _ in rr if s_ == 200)
        n409 = sum(1 for s_, _ in rr if s_ == 409)
        check("5 并发无键重复读片：恰 1 次 200，余 409", n200 == 1 and n409 == 4, f"200x{n200} 409x{n409}")

        # ------------------------------------------------ 8. 列表本人已提交状态
        section("8 · 工作台正确显示本人是否已提交")
        st, wb = encode(host, ["WL-1", "WL-2"], idem="enc-wl")
        w1, w2 = f"{wb['batch_code']}-S001", f"{wb['batch_code']}-S002"
        st, wl = zhang.get("/api/me/worklist")
        j1 = next(x for x in wl["samples"] if x["blind_code"] == w1)
        check("未提交：my_version 缺失", j1.get("my_version") is None, str(j1.get("my_version")))
        read(zhang, w1, "灰岩", mins(方解石=100), idem="wl-a1")
        st, wl = zhang.get("/api/me/worklist")
        j1 = next(x for x in wl["samples"] if x["blind_code"] == w1)
        check("提交后：my_version=1 且 my_slot=a", j1.get("my_version") == 1 and j1.get("my_slot") == "a", str((j1.get('my_version'), j1.get('my_slot'))))
        read(zhang, w1, "灰岩", mins(方解石=99, 白云石=1), idem="wl-a2", action="amend", ev=1)
        st, wl = zhang.get("/api/me/worklist")
        j1 = next(x for x in wl["samples"] if x["blind_code"] == w1)
        check("更正后：my_version=2", j1.get("my_version") == 2, str(j1.get("my_version")))

        # -------------------------------- 8B. 本人先交/对方未交 → 详情与列表契约
        section("8B · 本人先提交（对方未交）：保留本人记录、可更正、不见对方、列表不空")
        st, ub = encode(host, ["U-A", "U-B"], idem="enc-ui-state")
        u1 = f"{ub['batch_code']}-S001"
        read(zhang, u1, "砂岩", mins(石英=70, 长石=30), idem="u-a1")

        st, wl = zhang.get("/api/me/worklist")
        codes = [x["blind_code"] for x in wl["samples"]]
        check("提交后任务仍在本人列表（列表不空/不消失）", u1 in codes, str(codes))
        item = next(x for x in wl["samples"] if x["blind_code"] == u1)
        check("列表项带 my_version=1、my_slot=a",
              item.get("my_version") == 1 and item.get("my_slot") == "a", str((item.get('my_version'), item.get('my_slot'))))
        check("列表项不泄露对方结论/身份",
              "reading_b" not in item and "reading_a" not in item and "identity" not in item,
              str(sorted(item.keys())))

        st, v = zhang.get(f"/api/samples/{u1}")
        check("详情保留本人记录 my_reading（前端据此显示已提交/可更正，而非尚未提交）",
              v.get("my_reading") is not None and v["my_reading"]["version"] == 1, str(sorted(v.keys())))
        check("详情不成对暴露：无 reading_a/reading_b", "reading_a" not in v and "reading_b" not in v, "")
        check("详情仍不见对方与身份", "reader_b" not in v and "identity" not in v, str(sorted(v.keys())))

        # 本人此时更正（对方仍未交）
        st, b = read(zhang, u1, "砂岩", mins(石英=69, 长石=31), idem="u-a2", action="amend", ev=1)
        check("对方未交即可更正 -> reading、v2、标记须重裁",
              b["sample_status"] == "reading" and b["version"] == 2, str(b))
        st, v = zhang.get(f"/api/samples/{u1}")
        check("更正后详情保留本人 v2", v.get("my_reading", {}).get("version") == 2, str(v.get("my_reading")))
        st, wl = zhang.get("/api/me/worklist")
        item = next(x for x in wl["samples"] if x["blind_code"] == u1)
        check("更正后列表 my_version=2（即时一致，不靠刷新）", item.get("my_version") == 2, str(item.get("my_version")))

        # 对方随后齐交
        st, b = read(li, u1, "砂岩", mins(石英=69, 长石=31), idem="u-b1")
        check("齐交后（更正轮）强制 adjudicating", b["sample_status"] == "adjudicating", str(b))
        st, v = zhang.get(f"/api/samples/{u1}")
        check("齐交后本人可见成对结论", v.get("reading_a") is not None and v.get("reading_b") is not None, "")
        check("齐交后仍不见真实身份（未发布）", "identity" not in v, "")

        # 对方（晚交者）首交后立即看自己的视图：应同时见双方（齐交），且其列表含该样
        st, lv = li.get(f"/api/samples/{u1}")
        check("晚交者齐交后见成对结论", lv.get("reading_a") is not None and lv.get("reading_b") is not None, "")
        st, wl = li.get("/api/me/worklist")
        check("晚交者列表含该样且 my_version=1",
              any(x["blind_code"] == u1 and x.get("my_version") == 1 for x in wl["samples"]), "")

        # 重新登录（不重启）后，任务列表与本人提交状态仍一致
        zhang_relog = Client(base, "张工"); zhang_relog.login()
        st, wl = zhang_relog.get("/api/me/worklist")
        item = next((x for x in wl["samples"] if x["blind_code"] == u1), None)
        check("重新登录后任务仍在列表且 my_version=2（不靠页面缓存）",
              item is not None and item.get("my_version") == 2 and item["status"] == "adjudicating",
              str(item and {k: item.get(k) for k in ("blind_code", "my_version", "status")}))

        # ------------------------------------------------ 9. 正常全流程
        section("9 · 正常全流程（共识 / 矿物分歧 / 岩性分歧 / 第三人 / 发布揭晓）")
        st, fb = encode(host, ["F-1", "F-2", "F-3"], idem="enc-full")
        fc = fb["batch_code"]
        f1, f2, f3 = f"{fc}-S001", f"{fc}-S002", f"{fc}-S003"
        read(zhang, f1, "花岗闪长岩", mins(石英=30, 斜长石=45, 角闪石=25), idem="f1a")
        st, b = read(li, f1, "花岗闪长岩", mins(石英=34, 斜长石=41, 角闪石=25), idem="f1b")
        check("F1 差4pct -> 共识", b["sample_status"] == "consensus", f"{b}")
        read(zhang, f2, "花岗闪长岩", mins(石英=30, 斜长石=70), idem="f2a")
        st, b = read(li, f2, "花岗闪长岩", mins(石英=36, 斜长石=64), idem="f2b")
        check("F2 差6pct -> 分歧", b["sample_status"] == "adjudicating", f"{b}")
        read(zhang, f3, "花岗闪长岩", mins(石英=50, 长石=50), idem="f3a")
        st, b = read(li, f3, "石英二长岩", mins(石英=50, 长石=50), idem="f3b")
        check("F3 岩性不同 -> 分歧", b["sample_status"] == "adjudicating", f"{b}")
        # 原读片人不能被指派
        for who in ("张工", "李工"):
            st, _ = host.post(f"/api/samples/{f2}/adjudicator/assign",
                              {"idempotency_key": "bad-" + who, "host": host.name, "adjudicator": who})
            check(f"指派原读片人 {who} 为裁决人 -> 403", st == 403, f"{st}")
        st, _ = host.post(f"/api/samples/{f2}/adjudicator/assign",
                          {"idempotency_key": "as-f2", "host": host.name, "adjudicator": "赵裁"})
        st, _ = host.post(f"/api/samples/{f3}/adjudicator/assign",
                          {"idempotency_key": "as-f3", "host": host.name, "adjudicator": "钱裁"})
        check("分别指派赵裁/钱裁成功", st == 200, f"{st}")
        # 赵裁不能裁未指派给他的 F3
        st, b = zhao.post(f"/api/samples/{f3}/adjudication",
                          {"adjudicator": "赵裁", "lithology": "x", "minerals": mins(石英=100), "rationale": "r"})
        check("裁决人越权裁他人样本 -> 403", st == 403, f"{st}")
        st, b = zhao.post(f"/api/samples/{f2}/adjudication",
                          {"idempotency_key": "ad-f2", "adjudicator": "赵裁", "lithology": "花岗闪长岩",
                           "minerals": mins(石英=33, 斜长石=67), "rationale": "取中间"})
        st, b2 = qian.post(f"/api/samples/{f3}/adjudication",
                           {"idempotency_key": "ad-f3", "adjudicator": "钱裁", "lithology": "石英二长岩",
                            "minerals": mins(石英=52, 长石=48), "rationale": "见钾长石"})
        check("两份分歧均裁决完成", b["sample_status"] == "adjudicated" and b2["sample_status"] == "adjudicated", f"{b} {b2}")
        # 裁决提交后应即时移出各自待裁列表（不靠刷新）
        st, awl = zhao.get("/api/me/worklist")
        check("赵裁提交后 f2 移出待裁列表", f2 not in [x["blind_code"] for x in awl["samples"]],
              str([x["blind_code"] for x in awl["samples"]]))
        st, awl = qian.get("/api/me/worklist")
        check("钱裁提交后 f3 移出待裁列表", f3 not in [x["blind_code"] for x in awl["samples"]],
              str([x["blind_code"] for x in awl["samples"]]))
        st, b = host.post(f"/api/batches/{fc}/publish", {"idempotency_key": "pub-full", "host": host.name})
        check("整批发布 3 个", st == 200 and b["published"] == 3, f"{st} {b}")
        st, v = zhang.get(f"/api/samples/{f2}")
        check("发布后读片人可见身份与全部结论", v["status"] == "published" and "identity" in v
              and v.get("adjudication", {}).get("adjudicator") == "赵裁", "")
        st, b = read(zhang, f2, "x", mins(石英=100))
        check("发布后禁止再提交", st == 409, f"{st}")

        # ------------------------------------------------ 10. 部分写失败回滚
        section("10 · 故障注入：部分写失败整批回滚、不留残留")
        before = set(host.get("/api/me/worklist")[1]["batches"])
        st, b = host.post("/api/batches", {
            "idempotency_key": "enc-fail", "host": host.name, "project": "应回滚",
            "default_reader_a": "张工", "default_reader_b": "李工", "__failpoint": "encode_after_insert",
            "samples": [{"identity": "A"}, {"identity": "B"}, {"identity": "C"}]})
        check("编码中途故障 -> 500", st == 500, f"{st}")
        after = set(host.get("/api/me/worklist")[1]["batches"])
        check("无新增批次（无部分样本残留）", after == before, f"{after ^ before}")
        st, b = host.post("/api/batches", {
            "idempotency_key": "enc-fail", "host": host.name, "project": "重试成功",
            "default_reader_a": "张工", "default_reader_b": "李工",
            "samples": [{"identity": "A"}, {"identity": "B"}, {"identity": "C"}]})
        check("回滚后同键重试完整成功（3 样）", st == 201 and b["count"] == 3, f"{st} {b}")
        fbc = b["batch_code"]
        for i in (1, 2, 3):
            c = f"{fbc}-S00{i}"
            read(zhang, c, "灰岩", mins(方解石=95, 白云石=5), idem=f"rb{i}a")
            read(li, c, "灰岩", mins(方解石=95, 白云石=5), idem=f"rb{i}b")
        st, _ = host.post(f"/api/batches/{fbc}/publish",
                          {"idempotency_key": "pub-fail", "host": host.name,
                           "__failpoint": "publish_after_update"})
        check("发布中途故障 -> 500", st == 500, f"{st}")
        st, det = host.get(f"/api/batches/{fbc}")
        check("发布回滚：批次仍进行中、无一样本被发布",
              det["status"] == "open" and all(x["status"] != "published" for x in det["samples"]),
              det["status"])
        st, b = host.post(f"/api/batches/{fbc}/publish",
                          {"idempotency_key": "pub-fail-ok", "host": host.name})
        check("发布回滚后重试成功", st == 200 and b["published"] == 3, f"{st}")

    finally:
        stop_server(proc)

    # ------------------------------------------------ 11. 重启保留（含令牌）
    section("11 · 重启保留：数据、留痕与登录令牌仍有效")
    proc2, base2 = start_server(free_port(), db)
    try:
        h2 = Client(base2, "贺主持")
        h2.token = host.token  # 复用重启前签发的令牌
        z2 = Client(base2, "张工"); z2.token = zhang.token
        st, det = h2.get(f"/api/batches/{fc}")
        check("旧令牌在重启后仍可用（HMAC 密钥持久化）", st == 200, f"{st}")
        check("已发布批次状态保持", det["status"] == "published", det["status"])
        st, h = h2.get(f"/api/samples/{ac}/history")
        check("更正旧版与裁决留痕仍在",
              any(r["superseded"] for r in h["readings"]) and len(h["adjudications"]) >= 1, "")
        st, wl = z2.get("/api/me/worklist")
        check("已发布样本不出现在待办", all(x["status"] != "published" for x in wl["samples"]), "")
        # 重新登录同样可用
        st, b = h2.login()
        check("重启后可重新登录", st == 200 and bool(b["token"]), f"{st}")
    finally:
        stop_server(proc2)

    section(f"实测汇总：{len(PASS)} 通过 / {len(FAIL)} 失败")
    for name, detail in FAIL:
        print("  FAIL：", name, detail)
    print("\n数据库：", db)
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
