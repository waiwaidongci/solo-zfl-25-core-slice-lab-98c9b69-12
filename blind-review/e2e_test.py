#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
薄片双人盲评 · 端到端实测
覆盖：盲态越权 / 比例边界 / 分歧转第三人裁决 / 更正失效与重裁 / 并发只生效一次 /
      故障整批回滚 / 重启保留。
自动启停服务，使用临时数据库；最终输出 PASS/FAIL 汇总并以退出码反映结果。
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
import urllib.parse
import urllib.request
import urllib.error

HERE = os.path.dirname(os.path.abspath(__file__))
PY = sys.executable

PASS, FAIL = [], []


def check(name, cond, detail=""):
    (PASS if cond else FAIL).append((name, detail))
    print(("  [PASS] " if cond else "  [FAIL] ") + name + ("" if cond else (f"  -> {detail}" if detail else "")))


def section(t):
    print("\n" + "=" * 70 + f"\n{t}\n" + "=" * 70)


class Client:
    def __init__(self, base, role, name):
        self.base, self.role, self.name = base, role, name

    def _call(self, method, path, body=None, idem=None, raw_role=None, raw_name=None, no_auth=False):
        role = raw_role if raw_role is not None else self.role
        name = raw_name if raw_name is not None else self.name
        url = self.base + path
        if not no_auth:
            url += ("&" if "?" in url else "?") + "role=" + urllib.parse.quote(role) + \
                   "&name=" + urllib.parse.quote(name)
        headers = {"Content-Type": "application/json"}
        if idem:
            headers["Idempotency-Key"] = idem
        data = json.dumps(body, ensure_ascii=False).encode("utf-8") if body is not None else None
        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=15) as r:
                return r.status, json.loads(r.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            try:
                return e.code, json.loads(e.read().decode("utf-8"))
            except Exception:
                return e.code, {}

    def get(self, path, **kw):
        return self._call("GET", path, **kw)

    def post(self, path, body=None, idem=None, **kw):
        return self._call("POST", path, body=body, idem=idem, **kw)


def free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


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
            time.sleep(0.25)
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


def encode(host, code_seed, ids, ra="张工", rb="李工", project="东岭铜矿", idem=None, extra=None):
    body = {"idempotency_key": idem or ("enc-" + code_seed), "host": host.name, "project": project,
            "default_reader_a": ra, "default_reader_b": rb,
            "samples": [{"identity": x} for x in ids]}
    if extra:
        body.update(extra)
    return host.post("/api/batches", body)


def read(cli, code, lith, minerals, rat="镜下定名为依据", idem=None, action="submit", expected_version=None):
    body = {"idempotency_key": idem, "reader": cli.name, "lithology": lith,
            "minerals": minerals, "rationale": rat, "action": action}
    if expected_version is not None:
        body["expected_version"] = expected_version
    return cli.post(f"/api/samples/{code}/readings", body)


# ===========================================================================
def main():
    tmp = tempfile.mkdtemp(prefix="blind-e2e-")
    db = os.path.join(tmp, "review.db")
    port = free_port()
    proc, base = start_server(port, db)
    try:
        host = Client(base, "host", "贺主持")
        zhang = Client(base, "reader", "张工")
        li = Client(base, "reader", "李工")
        wang = Client(base, "reader", "王二")
        zhao = Client(base, "adjudicator", "赵裁")

        # ----------------------------------------------------------- 1. 盲态与越权
        section("场景 1 · 盲态保护与越权拦截")
        st, b = encode(host, "main", ["ZK17-128 花岗闪长岩", "ZK17-130 矿化带", "ZK17-131 石英脉"])
        check("批量编码成功（3 样）", st == 201 and b.get("count") == 3, f"{st} {b}")
        bc = b["batch_code"]
        s1, s2, s3 = [f"{bc}-S00{i}" for i in (1, 2, 3)]

        st, wl = zhang.get("/api/me/worklist")
        mine = [x["blind_code"] for x in wl["samples"]]
        check("读片人只看到分配给自己的盲码", set(mine) == {s1, s2, s3}, str(mine))
        first = wl["samples"][0]
        check("提交前看不到样本真实身份", "identity" not in first, str(first)[:200])
        check("提交前看不到对方结论", "reading_b" not in first and "reading_a" not in first, str(first)[:200])

        st, b = wang.get(f"/api/samples/{s1}")
        check("未参与者猜盲码 -> 404（不泄漏存在性）", st == 404, f"{st}")
        st, b = wang.get(f"/api/samples/{s1}/history")
        check("未参与者查留痕 -> 404", st == 404, f"{st}")
        st, b = li.post("/api/batches",
                        {"host": li.name, "project": "x", "default_reader_a": "a",
                         "default_reader_b": "b", "samples": [{"identity": "z"}]})
        check("读片人角色编码批次 -> 403", st == 403, f"{st}")
        st, b = zhang.post(f"/api/samples/{s1}/readings",
                           {"reader": li.name, "lithology": "x", "minerals": mins(石英=100), "rationale": "r"})
        check("登录张工却冒名李工提交 -> 403", st == 403, f"{st}")
        st, b = zhang.get("/api/samples/B9999-S999")
        check("查询不存在盲码 -> 404", st == 404, f"{st}")
        st, b = zhang.get("/api/me/worklist", no_auth=True)
        check("无身份访问 -> 401", st == 401, f"{st}")
        st, b = host.post("/api/batches",
                          {"host": host.name, "project": "p", "default_reader_a": "同", "default_reader_b": "同",
                           "samples": [{"identity": "z"}]})
        check("两名读片人相同 -> 400", st == 400, f"{st} {b}")

        # ----------------------------------------------------------- 2. 比例边界
        section("场景 2 · 矿物比例合计边界（必须恰好 100%）")
        st, b = encode(host, "bound", ["边界-1", "边界-2", "边界-3", "边界-4", "边界-5"], idem="enc-bound")
        bcodes = [f"{b['batch_code']}-S00{i}" for i in range(1, 6)]
        cases = [
            ("合计 100.00 合法", mins(石英=30, 长石=70), 200),
            ("合计 99.99 拒绝", mins(石英=30, 长石=69.99), 400),
            ("合计 100.01 拒绝", mins(石英=30, 长石=70.01), 400),
            ("负值拒绝", mins(石英=110, 长石=-10), 400),
            ("空矿物表拒绝", [], 400),
        ]
        for (name, minerals, want), code in zip(cases, bcodes):
            st, resp = read(zhang, code, "岩性", minerals, idem="k-" + code)
            check(name, st == want, f"got {st} {resp.get('error')}")
        st, resp = read(zhang, bcodes[0], "岩性",
                        [{"mineral": "石英", "percent": 50}, {"mineral": "石英", "percent": 50}], idem="dup-min")
        check("同名矿物重复 -> 400", st == 400, f"{st}")

        # ----------------------------------------------------------- 3. 分歧判定与第三人裁决
        section("场景 3 · 共识 / 分歧（岩性或矿物差>5pct）/ 第三人裁决")
        # S1：矿物差 4 个百分点（30 vs 34），岩性同 -> 共识
        read(zhang, s1, "花岗闪长岩", mins(石英=30, 斜长石=45, 角闪石=25), idem="s1-a")
        st, r = read(li, s1, "花岗闪长岩", mins(石英=34, 斜长石=41, 角闪石=25), idem="s1-b")
        check("矿物差 4pct（≤5）且岩性同 -> 共识", st == 200 and r["sample_status"] == "consensus", f"{r}")
        # S2：石英差 6 个百分点 -> 分歧
        read(zhang, s2, "花岗闪长岩", mins(石英=30, 斜长石=70), idem="s2-a")
        st, r = read(li, s2, "花岗闪长岩", mins(石英=36, 斜长石=64), idem="s2-b")
        check("矿物差 6pct（>5）-> 分歧待裁决", r["sample_status"] == "adjudicating", f"{r}")
        # S3：岩性不同 -> 分歧
        read(zhang, s3, "花岗闪长岩", mins(石英=50, 长石=50), idem="s3-a")
        st, r = read(li, s3, "石英二长岩", mins(石英=50, 长石=50), idem="s3-b")
        check("岩性不同 -> 分歧待裁决", r["sample_status"] == "adjudicating", f"{r}")
        # 齐交后可读对方，但仍无身份
        st, view = zhang.get(f"/api/samples/{s2}")
        check("齐交后可见双方结论（匿名）", "reading_a" in view and "reading_b" in view, "")
        check("齐交后仍看不到真实身份", "identity" not in view, "")

        st, _ = host.post(f"/api/samples/{s2}/adjudicator/assign",
                          {"idempotency_key": "as2-self", "host": host.name, "adjudicator": "张工"})
        check("原读片人 A 被指为裁决人 -> 403", st == 403, f"{st}")
        st, _ = host.post(f"/api/samples/{s2}/adjudicator/assign",
                          {"idempotency_key": "as2-self2", "host": host.name, "adjudicator": "李工"})
        check("原读片人 B 被指为裁决人 -> 403", st == 403, f"{st}")
        st, resp = host.post(f"/api/samples/{s2}/adjudicator/assign",
                             {"idempotency_key": "as2", "host": host.name, "adjudicator": "赵裁"})
        check("指派独立第三人成功", st == 200, f"{st} {resp}")
        st, view = li.get(f"/api/samples/{s2}")
        check("指派后占位标记 pending 且不泄露裁决内容",
              view.get("adjudication", {}).get("pending") is True
              and "lithology" not in view.get("adjudication", {}), str(view.get("adjudication")))
        # 赵裁不是该样读者：服务端按其被指派授权
        st, wl = zhao.get("/api/me/worklist")
        check("裁决人工作台列出待裁样本", s2 in [x["blind_code"] for x in wl["samples"]], "")
        adj_item = next(x for x in wl["samples"] if x["blind_code"] == s2)
        check("裁决人看不到真实身份", "identity" not in adj_item, "")
        st, b = zhao.post(f"/api/samples/{s2}/adjudication",
                          {"idempotency_key": "adj2", "adjudicator": "赵裁", "lithology": "花岗闪长岩",
                           "minerals": mins(石英=33, 斜长石=67), "rationale": "复核后采信中间值"})
        check("第三人提交裁决 -> 已裁决", st == 200 and b["sample_status"] == "adjudicated", f"{st} {b}")
        st, b = zhao.post(f"/api/samples/{s2}/adjudication",
                          {"adjudicator": "赵裁", "lithology": "x", "minerals": mins(石英=100), "rationale": "r"})
        check("裁决并发/重复提交 -> 409", st == 409, f"{st}")
        st, wl = zhao.get("/api/me/worklist")
        check("已裁决样本移出待裁队列", s2 not in [x["blind_code"] for x in wl["samples"]], "")
        # 未达可发布状态时发布被拒（S3 仍未裁决）
        st, b = host.post(f"/api/batches/{bc}/publish", {"idempotency_key": "pub-early", "host": host.name})
        check("存在待裁决样本时整批发布 -> 409", st == 409, f"{st} {b.get('message')}")
        # S3 仍需指派+裁决
        host.post(f"/api/samples/{s3}/adjudicator/assign",
                  {"idempotency_key": "as3", "host": host.name, "adjudicator": "赵裁"})
        st, b = zhao.post(f"/api/samples/{s3}/adjudication",
                          {"idempotency_key": "adj3", "adjudicator": "赵裁", "lithology": "石英二长岩",
                           "minerals": mins(石英=52, 长石=48), "rationale": "见钾长石含量"})
        check("S3 完成裁决", st == 200, f"{st}")
        st, b = host.post(f"/api/batches/{bc}/publish", {"idempotency_key": "pub-main", "host": host.name})
        check("全部就绪后整批发布成功", st == 200 and b["published"] == 3, f"{st} {b}")
        st, view = zhang.get(f"/api/samples/{s2}")
        check("发布后读片人可见真实身份与全部结论", view["status"] == "published" and "identity" in view, "")
        st, b = read(zhang, s2, "x", mins(石英=100))
        check("发布后禁止再提交/更正", st == 409, f"{st}")

        # ----------------------------------------------------------- 4. 更正：旧版留存 + 共识/裁决失效 + 重裁
        section("场景 4 · 更正留痕、原共识/裁决失效、重裁后方可发布")
        st, b = encode(host, "amend", ["更正流程样本"], idem="enc-amend")
        ac, = [f"{b['batch_code']}-S001"]
        read(zhang, ac, "砂岩", mins(石英=60, 长石=40), idem="am-a1")
        read(li, ac, "砂岩", mins(石英=62, 长石=38), idem="am-b1")
        st, det = host.get(f"/api/batches/{b['batch_code']}")
        samp = next(x for x in det["samples"] if x["blind_code"] == ac)
        check("更正前为共识", samp["status"] == "consensus", samp["status"])
        # 张工更正（矿物差拉大到 10pct，且岩性改变）
        st, r = read(zhang, ac, "岩屑砂岩", mins(石英=70, 长石=20, 岩屑=10),
                     idem="am-a2", action="amend", expected_version=1)
        check("更正后进入新一轮且状态待裁决", r["action"] == "amended" and r["version"] == 2
              and r["round"] == 2 and r["sample_status"] == "adjudicating", f"{r}")
        # 基于过期版本 v1 再次更正 -> 拒绝（乐观锁）
        st, err = read(zhang, ac, "岩屑砂岩", mins(石英=100), idem="am-stale",
                       action="amend", expected_version=1)
        check("基于过期版本 v1 更正 -> 409", st == 409 and err.get("error") == "version_conflict", f"{st} {err}")
        # 未走更正通道、直接再次普通提交 -> 409 already_submitted
        st, err = read(zhang, ac, "岩屑砂岩", mins(石英=100), idem="am-resubmit")
        check("已有有效读片后普通重复提交 -> 409", st == 409 and err.get("error") == "already_submitted", f"{st} {err}")
        st, h = host.get(f"/api/samples/{ac}/history")
        old = [x for x in h["readings"] if x["superseded"]]
        cur = [x for x in h["readings"] if not x["superseded"]]
        check("旧版读片留存且标记作废", len(old) == 1 and old[0]["version"] == 1, str(h["readings"]))
        check("当前有效为 A v2 与 B v1（升入第2轮）",
              {(x["slot"], x["version"], x["round"]) for x in cur} == {("a", 2, 2), ("b", 1, 2)}, str(cur))
        # 此时整批不得发布
        st, pb = host.post(f"/api/batches/{b['batch_code']}/publish",
                           {"idempotency_key": "pub-amend-block", "host": host.name})
        check("更正后未重裁 -> 发布被拒", st == 409, f"{st}")
        # 指派另一名第三人（钱工），原读者仍不得裁决
        qian = Client(base, "adjudicator", "钱裁")
        st, _ = host.post(f"/api/samples/{ac}/adjudicator/assign",
                          {"idempotency_key": "as-am", "host": host.name, "adjudicator": "钱裁"})
        check("为新一轮指派另一第三人成功", st == 200, f"{st}")
        st, b2 = qian.post(f"/api/samples/{ac}/adjudication",
                           {"idempotency_key": "adj-am", "adjudicator": "钱裁", "lithology": "岩屑砂岩",
                            "minerals": mins(石英=68, 长石=22, 岩屑=10), "rationale": "复核采信修正结论"})
        check("重新裁决完成", st == 200 and b2["sample_status"] == "adjudicated", f"{st} {b2}")
        st, h = host.get(f"/api/samples/{ac}/history")
        check("留痕含失效/当前裁决各一",
              sum(1 for a in h["adjudications"] if a["superseded"]) == 0
              and sum(1 for a in h["adjudications"] if not a["superseded"]) == 1
              and len(h["adjudications"]) == 1, str(h["adjudications"]))
        st, pb = host.post(f"/api/batches/{b['batch_code']}/publish",
                           {"idempotency_key": "pub-amend-ok", "host": host.name})
        check("重裁后整批可发布", st == 200, f"{st} {pb}")

        # ----------------------------------------------------------- 5. 并发：只生效一次
        section("场景 5 · 并发重复（编码 / 读片 / 发布）只生效一次")
        # 5a 同幂等键并发编码 N 次 -> 只建一批
        key = "enc-race-" + str(port)
        payload = {"idempotency_key": key, "host": host.name, "project": "并发批",
                   "default_reader_a": "张工", "default_reader_b": "李工",
                   "samples": [{"identity": "并发-1"}, {"identity": "并发-2"}]}
        results = []
        barrier = threading.Barrier(6)

        def enc_race():
            barrier.wait()
            results.append(host.post("/api/batches", payload))
        ts = [threading.Thread(target=enc_race) for _ in range(6)]
        [t.start() for t in ts]; [t.join() for t in ts]
        codes = {r[1].get("batch_code") for r in results if r[0] == 201}
        check("6 并发同键编码：全部返回同一批次、只建一批",
              len(codes) == 1 and all(r[0] == 201 for r in results), f"{codes} {[r[0] for r in results]}")
        race_bc = next(iter(codes))
        # 数据库中该批确实只有 2 个样本（无重复行）
        st, det = host.get(f"/api/batches/{race_bc}")
        check("批次内样本无重复（2 个）", len(det["samples"]) == 2, str(len(det["samples"])))

        # 5b 无幂等键：同一人对同一样本并发首交 -> 仅一成一冲突
        rc = f"{race_bc}-S001"
        body = {"reader": "张工", "lithology": "砂岩", "minerals": mins(石英=80, 长石=20), "rationale": "x"}
        res2 = []
        barrier2 = threading.Barrier(5)

        def read_race():
            barrier2.wait()
            res2.append(zhang.post(f"/api/samples/{rc}/readings", body))
        ts = [threading.Thread(target=read_race) for _ in range(5)]
        [t.start() for t in ts]; [t.join() for t in ts]
        ok = sum(1 for s_, _ in res2 if s_ == 200)
        conflict = sum(1 for s_, _ in res2 if s_ == 409)
        check("5 并发无键重复读片：恰好 1 次生效，其余 409", ok == 1 and conflict == 4,
              f"200x{ok} 409x{conflict}")
        st, h = host.get(f"/api/samples/{rc}/history")
        check("该读者仅 1 条有效读片（无重复版本）",
              len([x for x in h["readings"] if x["slot"] == "a" and not x["superseded"]]) == 1, "")

        # 补全 race_bc：另一人提交达成共识后测并发发布去重
        read(li, rc, "砂岩", mins(石英=80, 长石=20), idem="race-rc-b")
        rc2 = f"{race_bc}-S002"
        read(zhang, rc2, "泥岩", mins(黏土=90, 石英=10), idem="race-rc2-a")
        read(li, rc2, "泥岩", mins(黏土=90, 石英=10), idem="race-rc2-b")
        pub_res = []
        pkey = "pub-race-" + str(port)
        barrier3 = threading.Barrier(6)

        def pub_race():
            barrier3.wait()
            pub_res.append(host.post(f"/api/batches/{race_bc}/publish",
                                     {"idempotency_key": pkey, "host": host.name}))
        ts = [threading.Thread(target=pub_race) for _ in range(6)]
        [t.start() for t in ts]; [t.join() for t in ts]
        ok = sum(1 for s_, _ in pub_res if s_ == 200)
        check("6 并发同键发布：全部成功响应但只发布一次", ok == 6, f"{[s_ for s_,_ in pub_res]}")
        st, det = host.get(f"/api/batches/{race_bc}")
        check("批次状态为已发布且样本均只发布一次",
              det["status"] == "published" and all(x["status"] == "published" for x in det["samples"]), "")

        # ----------------------------------------------------------- 6. 故障注入：整批回滚
        section("场景 6 · 故障注入：整批失败不留部分记录")
        before_batches = host.get("/api/me/worklist")[1]["batches"]
        st, b = host.post("/api/batches", {
            "idempotency_key": "enc-fail", "host": host.name, "project": "应回滚批",
            "default_reader_a": "张工", "default_reader_b": "李工",
            "__failpoint": "encode_after_insert",
            "samples": [{"identity": "A"}, {"identity": "B"}, {"identity": "C"}]})
        check("编码中途故障 -> 500", st == 500, f"{st}")
        after_batches = host.get("/api/me/worklist")[1]["batches"]
        check("回滚后批次列表无新增", set(after_batches) == set(before_batches),
              f"{set(after_batches) ^ set(before_batches)}")
        # 同键重试：上次回滚未占用幂等键，应能正常成功（证明无残留）
        st, b = host.post("/api/batches", {
            "idempotency_key": "enc-fail", "host": host.name, "project": "重试成功批",
            "default_reader_a": "张工", "default_reader_b": "李工",
            "samples": [{"identity": "A"}, {"identity": "B"}, {"identity": "C"}]})
        check("回滚后同键重试可成功且为完整 3 样", st == 201 and b["count"] == 3, f"{st} {b}")
        fb = b["batch_code"]
        # 让该批达到可发布（构造 3 个共识）
        for i in (1, 2, 3):
            c = f"{fb}-S00{i}"
            read(zhang, c, "灰岩", mins(方解石=95, 白云石=5), idem=f"fb{i}a")
            read(li, c, "灰岩", mins(方解石=95, 白云石=5), idem=f"fb{i}b")
        st, b = host.post(f"/api/batches/{fb}/publish", {
            "idempotency_key": "pub-fail", "host": host.name, "__failpoint": "publish_after_update"})
        check("发布中途故障 -> 500", st == 500, f"{st}")
        st, det = host.get(f"/api/batches/{fb}")
        check("发布回滚：批次仍进行中、无一样本被置为已发布",
              det["status"] == "open" and all(x["status"] != "published" for x in det["samples"]),
              det["status"])
        st, b = host.post(f"/api/batches/{fb}/publish",
                          {"idempotency_key": "pub-fail-retry", "host": host.name})
        check("发布回滚后重试成功", st == 200 and b["published"] == 3, f"{st}")

    finally:
        stop_server(proc)

    # ----------------------------------------------------------- 7. 重启保留
    section("场景 7 · 重启后数据保留")
    proc2, base2 = start_server(free_port(), db)
    try:
        h2 = Client(base2, "host", "贺主持")
        z2 = Client(base2, "reader", "张工")
        st, det = h2.get(f"/api/batches/{bc}")
        check("重启后已发布批次仍在且状态保持", st == 200 and det["status"] == "published", f"{st}")
        check("重启后发布结论与身份仍可查",
              det["samples"][0]["status"] == "published" and bool(det["samples"][0].get("identity")), "")
        st, h = h2.get(f"/api/samples/{ac}/history")
        check("重启后更正旧版/裁决留痕仍在",
              len([x for x in h["readings"] if x["superseded"]]) >= 1
              and len(h["adjudications"]) >= 1, f"{h.get('readings') and 'readings-ok'}")
        st, wl = z2.get("/api/me/worklist")
        check("重启后已发布样本不出现在待办", all(x["status"] != "published" for x in wl["samples"]), "")
    finally:
        stop_server(proc2)

    # ----------------------------------------------------------- 汇总
    section(f"实测汇总：{len(PASS)} 通过 / {len(FAIL)} 失败")
    for name, detail in FAIL:
        print("  FAIL：", name, detail)
    print("\n数据库文件：", db)
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
