"""TCP 延迟代理引擎（纯 asyncio，无第三方依赖）。

实现的语义：
* 延迟从代理读到字节时起算：每个数据块获得各自的投递时刻
  (read_time + delay)，按 FIFO 顺序写出，连续数据流水通过延迟线，
  不会按读取块数累加等待。
* 每个方向的应用缓冲（已读未写）上限 64 KiB；缓冲满时只暂停该方向的
  读取，反向及本机其他连接照常传输。
* 连接建立时快照配置，整条连接沿用，之后改配置不影响它。
* 收到单向 EOF 后，先把该方向队列中待发字节全部写出，再转发半关闭
  (write_eof)；反向方向继续传输直到结束。
* 断连（正常结束或对端 RST）时释放队列并关闭两侧连接。
* 可选的上行禁发标记：在读到字节时做流式匹配，跨 TCP 分块、read 块/
  64 KiB 缓冲边界仍能识别；命中后连接标记为 blocked 并终止，完整标记
  不会交给目标。
"""
from __future__ import annotations

import asyncio
import dataclasses
import time
from collections import deque
from dataclasses import dataclass, field

BUFFER_LIMIT = 64 * 1024   # 每方向应用缓冲上限
READ_CHUNK = 16 * 1024     # 单次 read() 最大字节数
TAIL_KEEP = 4096           # 下行数据尾部保留长度（回执预览）
HISTORY_MAX = 100          # 历史连接保留条数
CONNECT_TIMEOUT = 15.0     # 连接目标的超时


def validate_marker(marker: str) -> str:
    """校验禁发标记：2～32 个可打印 ASCII 字符（0x20～0x7E）。"""
    if not isinstance(marker, str):
        raise ValueError("marker must be a string")
    if not 2 <= len(marker) <= 32:
        raise ValueError("marker length must be 2..32 bytes")
    if any(ord(ch) < 0x20 or ord(ch) > 0x7E for ch in marker):
        raise ValueError("marker must contain only printable ASCII (0x20-0x7E)")
    return marker


@dataclass
class ProxyConfig:
    listen_host: str = "0.0.0.0"
    listen_port: int = 7000
    target_host: str = "127.0.0.1"
    target_port: int = 9000
    up_delay_ms: float = 300.0    # 客户端 -> 目标
    down_delay_ms: float = 300.0  # 目标 -> 客户端
    block_marker: str = ""        # 上行禁发标记（空 = 不检查）


@dataclass
class DirectionStats:
    bytes: int = 0    # 已读累计字节
    peak: int = 0     # 应用缓冲峰值
    queued: int = 0   # 当前缓冲字节


@dataclass
class ConnInfo:
    id: int
    client: str
    cfg: ProxyConfig  # 建立时的配置快照
    started_at: float = field(default_factory=time.time)
    ended_at: float | None = None
    state: str = "active"  # active | closed | error | blocked
    error: str | None = None
    up: DirectionStats = field(default_factory=DirectionStats)    # 客户端 -> 目标
    down: DirectionStats = field(default_factory=DirectionStats)  # 目标 -> 客户端
    downlink_tail: bytes = b""  # 下行数据尾部（回执预览）
    blocked: bool = False       # 是否因命中禁发标记被拦截
    block_reason: str | None = None


class MarkerMatcher:
    """流式匹配禁发标记，跨读取块保持状态。

    ``scan(data)`` 返回 *(可安全转发的数据, 是否命中)*：

    * 未命中时，只交出“不可能是标记前缀”的字节，把尾部（可能是标记开头
      的 ≤ len(marker)-1 字节）暂存到下一次扫描，因此标记被 TCP 分块、
      read 块或 64 KiB 缓冲边界切开时仍能被完整识别，且完整标记不会写出；
    * 命中时交出标记起点之前的字节（标记本身及之后的数据一律丢弃），
      由调用方终止该连接。

    ``flush()`` 在对端 EOF 时把暂存字节原样交回，保证未命中数据的字节数
    与顺序不变。
    """

    def __init__(self, marker: str):
        self._pat = marker.encode("ascii")
        self._hold = b""

    @property
    def pattern(self) -> bytes:
        return self._pat

    def scan(self, chunk: bytes) -> tuple[bytes, bool]:
        data = self._hold + chunk
        self._hold = b""
        idx = data.find(self._pat)
        if idx >= 0:
            return data[:idx], True
        keep = self._suffix_prefix_len(data)
        if keep:
            self._hold = data[-keep:]
            return data[:-keep], False
        return data, False

    def flush(self) -> bytes:
        """EOF：暂存字节已被证明不是完整标记，原样交出。"""
        data, self._hold = self._hold, b""
        return data

    def _suffix_prefix_len(self, data: bytes) -> int:
        for k in range(min(len(self._pat) - 1, len(data)), 0, -1):
            if data.endswith(self._pat[:k]):
                return k
        return 0


class DirectionPump:
    """单向延迟管道：从 reader 读，延迟后写入 writer。"""

    def __init__(self, name, reader, writer, delay_s, stats, on_data=None,
                 marker_matcher=None, on_marker_hit=None):
        self.name = name
        self.reader = reader
        self.writer = writer
        self.delay = delay_s
        self.stats = stats
        self.on_data = on_data  # 写出后的回调（用于抓回执）
        self.on_marker_hit = on_marker_hit  # 命中禁发标记的回调（用于终止连接）
        # 有匹配器时，不会写出任何可能构成标记前缀的尾部字节
        self._matcher = marker_matcher
        # 暂存的标记前缀（≤ len(marker)-1 字节）不占应用缓冲，为它预留空间，
        # 保证“已读未写”字节数仍不超过 BUFFER_LIMIT。
        self._limit = BUFFER_LIMIT - (len(marker_matcher.pattern) - 1
                                      if marker_matcher is not None else 0)
        self._aborted = False
        self._queue: deque[tuple[float, bytes | None]] = deque()  # None = EOF 哨兵
        self._queued = 0
        self._space = asyncio.Event()   # 缓冲未满
        self._space.set()
        self._items = asyncio.Event()   # 队列非空
        self._wake = asyncio.Event()    # 用于打断延迟等待（终止时立即返回）

    def abort(self):
        """立即终止该向：清空队列、解除读取暂停、打断延迟等待。"""
        self._aborted = True
        self._queue.clear()
        self._queued = 0
        self.stats.queued = 0
        self._space.set()
        self._items.set()
        self._wake.set()

    async def run(self):
        rx = asyncio.create_task(self._read_loop(), name=f"pump-{self.name}-rx")
        tx = asyncio.create_task(self._write_loop(), name=f"pump-{self.name}-tx")
        try:
            await asyncio.gather(rx, tx)
        finally:
            for t in (rx, tx):
                t.cancel()
            await asyncio.gather(rx, tx, return_exceptions=True)
            # 断连/结束：释放队列
            self._queue.clear()
            self._queued = 0
            self.stats.queued = 0

    def _scan(self, chunk: bytes) -> tuple[bytes | None, bool]:
        """匹配禁发标记。返回 (可安全写出的数据, 是否命中)。

        未命中时只交出“不可能是标记前缀”的部分，尾部残留留到下一块再判，
        因此无论标记怎么被分块切开，完整标记都不会被写出。
        """
        body, hit = self._matcher.scan(chunk)
        return (body or None), hit

    async def _read_loop(self):
        loop = asyncio.get_running_loop()
        while True:
            if self._aborted:
                return
            await self._space.wait()  # 缓冲满则暂停该向读取（反向不受影响）
            max_read = min(READ_CHUNK, self._limit - self._queued)
            chunk = await self.reader.read(max_read)
            if self._aborted:
                return
            ready_at = loop.time() + self.delay  # 延迟自读到字节时起算
            if chunk:
                self.stats.bytes += len(chunk)
                if self._matcher is None:
                    self._enqueue(ready_at, chunk)
                else:
                    body, hit = self._scan(chunk)
                    if body:
                        self._enqueue(ready_at, body)
                    if hit:
                        # 命中：清空待发队列（连同之前的未发前缀），通知连接终止
                        self._aborted = True
                        self._queue.clear()
                        self._queued = 0
                        self.stats.queued = 0
                        self._space.set()
                        if self.on_marker_hit is not None:
                            self.on_marker_hit()
                        return
            else:
                # EOF：先冲刷暂存的非标记尾部，再入 EOF 哨兵；
                # 半关闭只在该向待发字节全部写出后转发
                if self._matcher is not None:
                    tail = self._matcher.flush()
                    if tail:
                        self._enqueue(ready_at, tail)
                self._queue.append((ready_at, None))
                self._items.set()
                return

    def _enqueue(self, ready_at, chunk: bytes):
        self._queue.append((ready_at, chunk))
        self._queued += len(chunk)
        self.stats.queued = self._queued
        if self._queued > self.stats.peak:
            self.stats.peak = self._queued
        if self._queued >= self._limit:
            self._space.clear()
        self._items.set()

    async def _write_loop(self):
        loop = asyncio.get_running_loop()
        while True:
            while not self._queue:
                self._items.clear()
                await self._items.wait()
            if self._aborted:
                return
            ready_at, chunk = self._queue[0]
            wait = ready_at - loop.time()
            if wait > 0:
                # 只等到队首块到期；后续块各自带投递时刻，流水发送。
                # 终止时 _wake 会立即打断等待。
                self._wake.clear()
                try:
                    await asyncio.wait_for(self._wake.wait(), wait)
                except asyncio.TimeoutError:
                    pass
            if self._aborted:
                return
            self._queue.popleft()
            if chunk is None:
                # 该向待发字节已全部写出 -> 转发半关闭
                try:
                    if self.writer.can_write_eof():
                        self.writer.write_eof()
                        await self.writer.drain()
                    else:
                        self.writer.close()
                except (ConnectionError, OSError, RuntimeError):
                    pass
                return
            try:
                self.writer.write(chunk)
                await self.writer.drain()
            except (ConnectionError, OSError, RuntimeError):
                return
            self._queued -= len(chunk)
            self.stats.queued = self._queued
            if self.on_data is not None:
                self.on_data(chunk)
            if self._queued < self._limit:
                self._space.set()


class Connection:
    """一条代理连接：客户端 <-> 两个 DirectionPump <-> 目标。"""

    def __init__(self, client_reader, client_writer, cfg: ProxyConfig, conn_id: int):
        self.client_reader = client_reader
        self.client_writer = client_writer
        self.target_reader = None
        self.target_writer = None
        peer = client_writer.get_extra_info("peername")
        client = f"{peer[0]}:{peer[1]}" if peer else "?"
        self.info = ConnInfo(id=conn_id, client=client, cfg=dataclasses.replace(cfg))

    def _capture_downlink(self, data: bytes):
        tail = self.info.downlink_tail + data
        self.info.downlink_tail = tail[-TAIL_KEEP:]

    async def run(self):
        info = self.info
        cfg = info.cfg
        try:
            self.target_reader, self.target_writer = await asyncio.wait_for(
                asyncio.open_connection(cfg.target_host, cfg.target_port),
                timeout=CONNECT_TIMEOUT,
            )
        except Exception as exc:
            info.state = "error"
            info.error = f"connect target {cfg.target_host}:{cfg.target_port} failed: {exc}"
            self._close(self.client_writer)
            await self._wait_closed(self.client_writer)
            info.ended_at = time.time()
            return

        up_matcher = None
        if cfg.block_marker:
            up_matcher = MarkerMatcher(cfg.block_marker)
        up = DirectionPump("up", self.client_reader, self.target_writer,
                           cfg.up_delay_ms / 1000.0, info.up,
                           marker_matcher=up_matcher,
                           on_marker_hit=self._on_marker_hit)
        down = DirectionPump("down", self.target_reader, self.client_writer,
                             cfg.down_delay_ms / 1000.0, info.down,
                             on_data=self._capture_downlink)
        self._up_pump = up
        self._down_pump = down
        tasks = [asyncio.create_task(up.run()), asyncio.create_task(down.run())]
        try:
            await asyncio.gather(*tasks)
        except Exception as exc:
            if not info.blocked:
                info.state = "error"
                info.error = f"{type(exc).__name__}: {exc}"
        finally:
            # 终止该连接的双向泵并释放队列（命中标记或任一侧结束时）
            up.abort()
            down.abort()
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            # 断连：关闭两侧连接（队列已在 pump 内释放）
            self._close(self.client_writer)
            self._close(self.target_writer)
            await self._wait_closed(self.client_writer)
            await self._wait_closed(self.target_writer)
            if info.blocked:
                info.state = "blocked"
            elif info.state == "active":
                info.state = "closed"
            info.ended_at = time.time()

    def _on_marker_hit(self):
        """上行命中禁发标记：终止整条连接，不再转发剩余字节。"""
        info = self.info
        if info.blocked:
            return
        info.blocked = True
        info.state = "blocked"
        info.block_reason = f"blocked: upload contains forbidden marker {info.cfg.block_marker!r}"
        info.error = info.block_reason
        # 立刻关闭两侧连接：目标收不到完整标记，客户端也收不到后续回执
        self._close(self.target_writer)
        self._close(self.client_writer)
        self._down_pump.abort()
        self._up_pump.abort()

    @staticmethod
    def _close(writer):
        if writer is not None:
            try:
                writer.close()
            except Exception:
                pass

    @staticmethod
    async def _wait_closed(writer):
        if writer is not None:
            try:
                await asyncio.wait_for(writer.wait_closed(), timeout=5)
            except Exception:
                pass


class DelayProxy:
    """监听 TCP 端口，为每条连接建立到目标的延迟管道。"""

    def __init__(self, cfg: ProxyConfig):
        self.cfg = cfg
        self.server: asyncio.Server | None = None
        self.listen_error: str | None = None
        self.active: dict[int, Connection] = {}
        self.history: deque[ConnInfo] = deque(maxlen=HISTORY_MAX)
        self._next_id = 1
        self._tasks: set[asyncio.Task] = set()
        self._lock = asyncio.Lock()

    @property
    def listening(self) -> bool:
        return self.server is not None and self.server.is_serving()

    async def apply_config(self, new_cfg: ProxyConfig):
        """应用新配置；仅影响之后建立的连接。监听地址/端口变化时重启监听器。"""
        if new_cfg.block_marker:
            validate_marker(new_cfg.block_marker)
        async with self._lock:
            restart = (
                self.server is None
                or new_cfg.listen_host != self.cfg.listen_host
                or new_cfg.listen_port != self.cfg.listen_port
            )
            self.cfg = dataclasses.replace(new_cfg)
            if not restart:
                return
            if self.server is not None:
                self.server.close()
                await self.server.wait_closed()
                self.server = None
            self.listen_error = None
            try:
                self.server = await asyncio.start_server(
                    self._on_client, new_cfg.listen_host, new_cfg.listen_port)
            except Exception as exc:
                self.listen_error = str(exc)

    def _on_client(self, reader, writer):
        cfg = dataclasses.replace(self.cfg)  # 建立时快照配置
        conn = Connection(reader, writer, cfg, self._next_id)
        self._next_id += 1
        cid = conn.info.id
        self.active[cid] = conn
        task = asyncio.create_task(conn.run(), name=f"conn-{cid}")
        self._tasks.add(task)
        task.add_done_callback(lambda t: (self._tasks.discard(t), self._finish(cid)))

    def _finish(self, cid: int):
        conn = self.active.pop(cid, None)
        if conn is not None:
            self.history.appendleft(conn.info)

    async def shutdown(self):
        if self.server is not None:
            self.server.close()
            await self.server.wait_closed()
            self.server = None
        tasks = list(self._tasks)
        for t in tasks:
            t.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    def snapshot(self) -> dict:
        def conn_dict(info: ConnInfo) -> dict:
            return {
                "id": info.id,
                "client": info.client,
                "state": info.state,
                "error": info.error,
                "target": f"{info.cfg.target_host}:{info.cfg.target_port}",
                "up_delay_ms": info.cfg.up_delay_ms,
                "down_delay_ms": info.cfg.down_delay_ms,
                "block_marker": info.cfg.block_marker,
                "blocked": info.blocked,
                "block_reason": info.block_reason,
                "started_at": info.started_at,
                "ended_at": info.ended_at,
                "up": dataclasses.asdict(info.up),
                "down": dataclasses.asdict(info.down),
                "receipt": info.downlink_tail.decode("utf-8", "replace"),
            }

        return {
            "listening": self.listening,
            "listen_error": self.listen_error,
            "config": dataclasses.asdict(self.cfg),
            "active": [conn_dict(c.info) for c in self.active.values()],
            "history": [conn_dict(i) for i in self.history],
        }
