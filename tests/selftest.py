#!/usr/bin/env python3
"""端到端自测：验证延迟代理的关键语义。

前置（见 README）：
  * 示例接收端监听 127.0.0.1:19000
  * 代理网页/API 在 127.0.0.1:18000，代理 TCP 监听 127.0.0.1:17000

运行：python3 tests/selftest.py
可用环境变量覆盖：WEB_URL / PROXY_HOST / PROXY_PORT / TARGET
"""
from __future__ import annotations

import asyncio
import hashlib
import os
import socket
import struct
import sys
import time

import httpx

WEB = os.environ.get("WEB_URL", "http://127.0.0.1:18000")
PROXY_HOST = os.environ.get("PROXY_HOST", "127.0.0.1")
PROXY_PORT = int(os.environ.get("PROXY_PORT", "17000"))
TARGET_HOST, TARGET_PORT = os.environ.get("TARGET", "127.0.0.1:19000").rsplit(":", 1)

failures: list[str] = []


def check(name: str, cond: bool, detail: str = ""):
    print(f"[{'ok ' if cond else 'FAIL'}] {name}" + (f"  {detail}" if detail else ""))
    if not cond:
        failures.append(name)


async def set_cfg(client: httpx.AsyncClient, *, up: float, down: float,
                  marker: str = ""):
    r = await client.post("/api/config", json={
        "listen_host": "0.0.0.0", "listen_port": PROXY_PORT,
        "target_host": TARGET_HOST, "target_port": int(TARGET_PORT),
        "up_delay_ms": up, "down_delay_ms": down, "block_marker": marker})
    r.raise_for_status()
    d = r.json()
    assert d["listening"], d
    return d


async def upload(client: httpx.AsyncClient, payload: bytes, name="blob.bin"):
    r = await client.post("/api/test-upload", files={"file": (name, payload)})
    r.raise_for_status()
    return r.json()


async def find_conn(client: httpx.AsyncClient, up_bytes: int, retries: int = 30):
    """按上行字节数精确定位一条连接（先历史后活动）。"""
    for _ in range(retries):
        d = (await client.get("/api/connections")).json()
        for c in d["history"] + d["active"]:
            if c["up"]["bytes"] == up_bytes:
                return c
        await asyncio.sleep(0.2)
    return None


class ChatterTarget:
    """目标：每收到一段数据就立刻回一条 PARTIAL-RECEIPT（记录回复次数）。

    用于模拟“目标已经回复了收到的前缀”，验证试传/连接仍按拦截处理。
    """

    def __init__(self):
        self.replies: list[int] = []
        self.received: list[bytearray] = []
        self.server: asyncio.Server | None = None
        self.port = 0

    async def start(self):
        self.server = await asyncio.start_server(self._handle, "127.0.0.1", 0)
        self.port = self.server.sockets[0].getsockname()[1]

    async def _handle(self, reader, writer):
        replies = 0
        buf = bytearray()
        self.replies.append(replies)
        self.received.append(buf)
        idx = len(self.replies) - 1
        try:
            while True:
                chunk = await reader.read(65536)
                if not chunk:
                    break
                buf.extend(chunk)
                writer.write(b"PARTIAL-RECEIPT got=%d\n" % len(chunk))
                await writer.drain()
                replies += 1
                self.replies[idx] = replies
        except (ConnectionError, OSError):
            pass
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    async def stop(self):
        if self.server is not None:
            self.server.close()
            await self.server.wait_closed()


class CaptureTarget:
    """记录每条连接实际收到的全部上行字节，并可在对端 EOF 后回一条回执。

    用于核对“完整标记从未送达”，以及“未命中数据原样转发且半关闭后能收到回执”。
    """

    def __init__(self, receipt: bool = False):
        self.receipt = receipt
        self.received: list[bytearray] = []
        self.server: asyncio.Server | None = None
        self.port = 0

    async def start(self):
        self.server = await asyncio.start_server(self._handle, "127.0.0.1", 0)
        self.port = self.server.sockets[0].getsockname()[1]

    async def _handle(self, reader, writer):
        buf = bytearray()
        self.received.append(buf)
        try:
            while True:
                chunk = await reader.read(65536)
                if not chunk:
                    break
                buf.extend(chunk)
            if self.receipt:
                h = hashlib.sha256(bytes(buf)).hexdigest()
                writer.write(f"CAPTURE-RECEIPT bytes={len(buf)} sha256={h}\n".encode())
                await writer.drain()
        except (ConnectionError, OSError):
            pass
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    async def stop(self):
        if self.server is not None:
            self.server.close()
            await self.server.wait_closed()

    def all_bytes(self) -> bytes:
        return b"".join(bytes(b) for b in self.received)


async def _configure_target(client: httpx.AsyncClient, port: int, marker: str,
                            up: float = 0, down: float = 0):
    r = await client.post("/api/config", json={
        "listen_host": "0.0.0.0", "listen_port": PROXY_PORT,
        "target_host": "127.0.0.1", "target_port": port,
        "up_delay_ms": up, "down_delay_ms": down, "block_marker": marker})
    r.raise_for_status()
    assert r.json()["listening"], r.text


async def raw_upload(payload_chunks, *, eof: bool = True,
                     inter_chunk_delay: float = 0.05, hold_s: float = 3.0):
    """直接用 socket 经代理上传（不先读欢迎行），返回 (下行数据, 客户端地址)。

    * 逐块发送，模拟 TCP 分块；
    * ``eof=True`` 时发送完立即半关闭；否则一直保持连接；
    * 随后继续读下行直到代理关闭连接或 hold_s 超时。
    """
    reader, writer = await asyncio.open_connection(PROXY_HOST, PROXY_PORT)
    local = writer.get_extra_info("sockname")[1]
    got = bytearray()

    async def drain_down():
        try:
            while True:
                data = await reader.read(65536)
                if not data:
                    break
                got.extend(data)
        except (ConnectionError, OSError):
            pass

    down = asyncio.create_task(drain_down())
    try:
        for chunk in payload_chunks:
            writer.write(chunk)
            await writer.drain()
            await asyncio.sleep(inter_chunk_delay)
        if eof:
            try:
                writer.write_eof()
            except (ConnectionError, OSError):
                pass
    except (ConnectionError, OSError):
        pass
    try:
        await asyncio.wait_for(down, hold_s)
    except asyncio.TimeoutError:
        down.cancel()
    try:
        writer.close()
        await writer.wait_closed()
    except Exception:
        pass
    return bytes(got), f"127.0.0.1:{local}"


async def marker_tests(client: httpx.AsyncClient):
    """禁发标记：跨块识别、完整标记不外发、拦截记录、未命中原样转发、并发不受影响。"""
    marker = "SECRET-MARKER"

    # 6a. 配置校验
    bad = await client.post("/api/config", json={
        "listen_host": "0.0.0.0", "listen_port": PROXY_PORT,
        "target_host": TARGET_HOST, "target_port": int(TARGET_PORT),
        "up_delay_ms": 0, "down_delay_ms": 0, "block_marker": "A"})
    check("marker: 1-byte marker rejected", bad.status_code == 422, str(bad.status_code))
    bad = await client.post("/api/config", json={
        "listen_host": "0.0.0.0", "listen_port": PROXY_PORT,
        "target_host": TARGET_HOST, "target_port": int(TARGET_PORT),
        "up_delay_ms": 0, "down_delay_ms": 0, "block_marker": "bad\tmarker"})
    check("marker: non-printable marker rejected", bad.status_code == 422, str(bad.status_code))

    await set_cfg(client, up=0, down=0, marker=marker)

    # 6b. 命中：标记被切成多个 TCP 分块（也在 16 KiB read 块边界附近再切一次）
    cap = CaptureTarget(receipt=True)
    await cap.start()
    try:
        await _configure_target(client, cap.port, marker)
        payload = b"x" * (16 * 1024 - 5) + marker.encode() + b"tail-after-marker" * 100
        got, client_key = await raw_upload([
            payload[:16 * 1024 - 6],     # 标记第 1 字节断在 read 块末尾
            payload[16 * 1024 - 6:16 * 1024 + 4],
            payload[16 * 1024 + 4:],
        ], hold_s=2.0)
        conn = await find_conn_by_client(client, client_key)
        check("marker: split upload blocked (connection recorded blocked)",
              conn is not None and conn["blocked"] is True,
              str(conn and conn["state"]))
        check("marker: connection recorded as blocked",
              conn is not None and conn["state"] == "blocked" and conn["blocked"] is True,
              str(conn and {k: conn[k] for k in ("state", "blocked", "block_reason")}))
        check("marker: blocked connection buffers released",
              conn is not None and conn["up"]["queued"] == 0 and conn["down"]["queued"] == 0,
              str(conn and (conn["up"], conn["down"])))
        received = cap.all_bytes()
        check("marker: target never received the complete marker",
              marker.encode() not in received,
              f"target got {len(received)} bytes")

        # 6c. 标记恰好跨越 64 KiB 缓冲边界（64 KiB 前缀后再命中）
        cap.received.clear()
        big = b"y" * 65530 + marker.encode() + b"z" * 1000
        chunks = [big[i:i + 16384] for i in range(0, len(big), 16384)]
        got2, key2 = await raw_upload(chunks, hold_s=2.0)
        conn2 = await find_conn_by_client(client, key2)
        check("marker: marker across 64 KiB buffer boundary blocked",
              conn2 is not None and conn2["blocked"] is True, str(conn2 and conn2["state"]))
        check("marker: boundary hit recorded blocked",
              conn2 is not None and conn2["blocked"] is True, str(conn2 and conn2["state"]))
        check("marker: target never received marker across buffer boundary",
              marker.encode() not in cap.all_bytes(),
              f"target got {len(cap.all_bytes())} bytes")

        # 6d. 未命中：原样转发 + 正常半关闭后仍收到回执
        cap.received.clear()
        clean = b"head-" + os.urandom(64 * 1024 - 3).replace(marker.encode(), b"")
        clean_payload = clean + b"SECRET-MARKE"  # 只是前缀，不构成完整标记
        reader, writer = await asyncio.open_connection(PROXY_HOST, PROXY_PORT)
        for i in range(0, len(clean_payload), 20000):
            writer.write(clean_payload[i:i + 20000])
            await writer.drain()
        writer.write_eof()
        try:
            tail = await asyncio.wait_for(reader.read(), 30)
        except (ConnectionError, OSError):
            tail = b""
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass
        check("marker: clean upload forwarded byte-for-byte after half-close",
              cap.all_bytes() == clean_payload, f"{len(cap.all_bytes())}/{len(clean_payload)}")
        check("marker: clean upload got receipt after half-close",
              f"bytes={len(clean_payload)}".encode() in tail
              and hashlib.sha256(clean_payload).hexdigest().encode() in tail,
              f"tail={tail[-120:]!r}")
    finally:
        await cap.stop()

    # 6e. 回到示例接收端：试传含标记文件 -> 记录 blocked，试传不报成功
    await set_cfg(client, up=0, down=0, marker=marker)
    blocked_payload = b"a" * 40000 + marker.encode() + b"b" * 40000
    res = await upload(client, blocked_payload, "blocked.bin")
    check("marker: upload with marker reported blocked, not ok",
          res.get("blocked") is True and res.get("ok") is False, str(res)[:300])

    # 6f. 目标已回复前缀时，经试传上传仍不能报成功
    chat = ChatterTarget()
    await chat.start()
    try:
        await _configure_target(client, chat.port, marker, up=100, down=100)
        res_blocked = await upload(client, b"w" * 100000 + marker.encode(), "blocked2.bin")
        check("marker: target replied to prefix during test-upload",
              bool(chat.replies) and max(chat.replies) >= 1, str(chat.replies))
        check("marker: test-upload blocked even though target replied prefix",
              res_blocked.get("blocked") is True and res_blocked.get("ok") is False,
              str(res_blocked)[:300])

        # 直接连接同样验证：下行确实收到目标的前缀回复，连接仍被拦截
        got3, key3 = await raw_upload(
            [b"c" * 100000, marker.encode() + b"d" * 500],
            inter_chunk_delay=0.6, hold_s=3.0)
        conn3 = await find_conn_by_client(client, key3)
        check("marker: raw connection received target prefix before block",
              got3.count(b"PARTIAL-RECEIPT") >= 1, got3[-120:])
        check("marker: raw connection blocked despite target prefix",
              conn3 is not None and conn3["blocked"] is True
              and conn3["state"] == "blocked", str(conn3 and conn3["state"]))
        check("marker: target saw no complete marker via raw connection",
              marker.encode() not in b"".join(bytes(b) for b in chat.received),
              f"target got {sum(len(b) for b in chat.received)} bytes")
    finally:
        await chat.stop()

    # 6g. 其他连接不受影响：一条被拦截时另一条正常完成
    await set_cfg(client, up=100, down=100, marker=marker)
    bad_payload = b"p" * 1024 + marker.encode()
    good_payload = os.urandom(200 * 1024)
    bad_res, good_res = await asyncio.gather(
        upload(client, bad_payload, "bad.bin"),
        upload(client, good_payload, "good.bin"))
    check("marker: concurrent good upload unaffected",
          good_res.get("ok") is True
          and hashlib.sha256(good_payload).hexdigest() in good_res.get("receipt", ""),
          str(good_res)[:200])
    check("marker: concurrent bad upload blocked",
          bad_res.get("blocked") is True and bad_res.get("ok") is False, str(bad_res)[:200])

    # 6h. 每条连接沿用建立时规则：连接建立后清空标记，该连接仍按旧规则拦截
    reader, writer = await asyncio.open_connection(PROXY_HOST, PROXY_PORT)
    await reader.readline()
    await asyncio.sleep(0.2)
    await set_cfg(client, up=0, down=0, marker="")  # 新连接不再检查
    writer.write(b"late-" + marker.encode() + b"-end")
    await writer.drain()
    terminated = False
    got_late = b""
    try:
        got_late = await asyncio.wait_for(reader.read(), 10)
        terminated = True  # 连接被代理关闭 -> 读到 EOF
    except (ConnectionError, OSError):
        terminated = True
    writer.close()
    try:
        await writer.wait_closed()
    except Exception:
        pass
    check("marker: connection keeps establish-time marker rule",
          terminated and marker.encode() not in got_late,
          f"got={got_late[:80]!r}")

    # 6i. 清空标记后，含原标记的新连接正常转发
    res = await upload(client, b"after-clear-" + marker.encode(), "after.bin")
    check("marker: cleared marker no longer blocks new connections",
          res.get("blocked") is False and res.get("ok") is True
          and f"bytes={len(b'after-clear-' + marker.encode())}" in res.get("receipt", ""),
          str(res)[:200])

    await set_cfg(client, up=300, down=300, marker="")  # 恢复默认


async def find_conn_by_client(client: httpx.AsyncClient, client_key: str):
    for _ in range(40):
        d = (await client.get("/api/connections")).json()
        matches = [c for c in d["history"] + d["active"] if c["client"] == client_key]
        if matches:
            return max(matches, key=lambda c: c["id"])
        await asyncio.sleep(0.1)
    return None


async def main():
    async with httpx.AsyncClient(base_url=WEB, timeout=httpx.Timeout(300.0, connect=10.0)) as client:
        # 0. 服务起来且在监听
        d = (await client.get("/api/config")).json()
        check("web api up, proxy listening", d.get("listening") is True, str(d))

        await set_cfg(client, up=300, down=300)

        # 1. 上传 1 MiB：回执须带正确字节数与 SHA-256（隐含验证：EOF 在全部
        #    待发字节之后才转发，否则接收端统计不到完整大小）
        payload = os.urandom(1024 * 1024)
        sha = hashlib.sha256(payload).hexdigest()
        t0 = time.monotonic()
        res = await upload(client, payload)
        dt = time.monotonic() - t0
        check("upload ok", res.get("ok") is True, str(res)[:300])
        check("welcome line received", bool(res.get("welcome")), str(res.get("welcome")))
        receipt = res.get("receipt", "")
        check("receipt byte count matches", f"bytes={len(payload)}" in receipt, receipt)
        check("receipt sha256 matches", sha in receipt, receipt)
        # 流水化：1 MiB = 64 个 16KiB 块，若按块累加等待需 >= 64*0.3s ≈ 19s
        check("delay pipelined (not per-chunk)", dt < 8.0, f"{dt:.2f}s for 1 MiB @300ms")

        # 2. 统计：双向字节数、64 KiB 缓冲上限
        conn = await find_conn(client, len(payload))
        check("connection stats recorded", conn is not None)
        if conn:
            check("up bytes counted", conn["up"]["bytes"] == len(payload), str(conn["up"]))
            check("down bytes counted", conn["down"]["bytes"] > 0, str(conn["down"]))
            check("up buffer peak <= 64 KiB", conn["up"]["peak"] <= 65536, str(conn["up"]["peak"]))
            check("up buffer peak reaches 64 KiB", conn["up"]["peak"] == 65536,
                  str(conn["up"]["peak"]))
            check("receipt captured in stats", sha in conn.get("receipt", ""),
                  conn.get("receipt", "")[-80:])

        # 3. 每条连接沿用建立时的配置：连接中途改配置不影响它
        await set_cfg(client, up=1500, down=1500)
        reader, writer = await asyncio.open_connection(PROXY_HOST, PROXY_PORT)
        await asyncio.wait_for(reader.readline(), 30)  # 欢迎行
        data = b"snapshot-test-" + b"x" * 200
        writer.write(data)
        await writer.drain()
        await asyncio.sleep(0.3)  # 让数据进入延迟线
        await set_cfg(client, up=0, down=0)  # 连接中途改配置
        writer.write_eof()
        receipt_b = await asyncio.wait_for(reader.read(), 60)
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass
        check("snapshot conn receipt ok", f"bytes={len(data)}".encode() in receipt_b,
              receipt_b.decode(errors="replace"))
        conn2 = await find_conn(client, len(data))
        check("connection kept establish-time config",
              conn2 is not None and conn2["up_delay_ms"] == 1500
              and conn2["down_delay_ms"] == 1500,
              f"up={conn2 and conn2['up_delay_ms']} down={conn2 and conn2['down_delay_ms']}")

        # 4. 并发上传互不影响
        await set_cfg(client, up=200, down=200)
        payloads = [os.urandom(256 * 1024) for _ in range(3)]
        results = await asyncio.gather(*(upload(client, p, f"c{i}.bin")
                                         for i, p in enumerate(payloads)))
        ok = all(r.get("ok") and hashlib.sha256(p).hexdigest() in r.get("receipt", "")
                 for r, p in zip(results, payloads))
        check("3 concurrent uploads, receipts correct", ok,
              str([r.get("receipt") for r in results]))

        # 5. 异常断连（RST）后释放队列与连接
        before = {c["id"] for c in (await client.get("/api/connections")).json()["history"]}

        def rst():
            s = socket.create_connection((PROXY_HOST, PROXY_PORT), timeout=5)
            s.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
            s.sendall(b"reset-me")
            s.close()  # 发 RST（在途数据可能被丢弃，故不按字节数识别该连接）
        await asyncio.to_thread(rst)
        rst_conn = None
        for _ in range(30):
            d = (await client.get("/api/connections")).json()
            for c in d["history"]:
                if c["id"] not in before:
                    rst_conn = c
                    break
            if rst_conn is not None:
                break
            await asyncio.sleep(0.2)
        check("reset connection moved to history", rst_conn is not None)
        if rst_conn is not None:
            active_ids = {c["id"] for c in (await client.get("/api/connections")).json()["active"]}
            check("reset connection released from active set", rst_conn["id"] not in active_ids,
                  f"state={rst_conn['state']} error={rst_conn['error']}")

        # 6. 禁发标记
        await marker_tests(client)

        await set_cfg(client, up=300, down=300)  # 恢复默认

    print()
    if failures:
        print(f"FAILED: {len(failures)} check(s): {failures}")
        sys.exit(1)
    print("ALL CHECKS PASSED")


if __name__ == "__main__":
    asyncio.run(main())
