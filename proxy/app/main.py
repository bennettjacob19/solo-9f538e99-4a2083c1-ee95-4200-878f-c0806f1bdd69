"""FastAPI 应用：代理配置、连接统计、上传试传、网页。"""
from __future__ import annotations

import asyncio
import os
import time
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, UploadFile
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field, field_validator

from .engine import DelayProxy, ProxyConfig, validate_marker

STATIC_DIR = Path(__file__).resolve().parent / "static"
UPLOAD_CHUNK = 256 * 1024


def _env_config() -> ProxyConfig:
    marker = os.getenv("BLOCK_MARKER", "")
    if marker:
        validate_marker(marker)  # 启动即校验，避免带非法标记运行
    return ProxyConfig(
        listen_host=os.getenv("LISTEN_HOST", "0.0.0.0"),
        listen_port=int(os.getenv("LISTEN_PORT", "7000")),
        target_host=os.getenv("TARGET_HOST", "127.0.0.1"),
        target_port=int(os.getenv("TARGET_PORT", "9000")),
        up_delay_ms=float(os.getenv("UP_DELAY_MS", "300")),
        down_delay_ms=float(os.getenv("DOWN_DELAY_MS", "300")),
        block_marker=marker,
    )


proxy = DelayProxy(_env_config())


@asynccontextmanager
async def lifespan(_app: FastAPI):
    await proxy.apply_config(proxy.cfg)  # 启动监听器
    yield
    await proxy.shutdown()


app = FastAPI(title="TCP Delay Proxy", lifespan=lifespan)


class ConfigIn(BaseModel):
    listen_host: str = "0.0.0.0"
    listen_port: int = Field(ge=1, le=65535)
    target_host: str = Field(min_length=1)
    target_port: int = Field(ge=1, le=65535)
    up_delay_ms: float = Field(ge=0, le=3_600_000)
    down_delay_ms: float = Field(ge=0, le=3_600_000)
    block_marker: str = ""  # 2～32 个可打印 ASCII；空串 = 不检查

    @field_validator("block_marker")
    @classmethod
    def _check_marker(cls, value: str) -> str:
        return validate_marker(value) if value else ""


@app.get("/", include_in_schema=False)
async def index():
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/api/config")
async def get_config():
    snap = proxy.snapshot()
    return {k: snap[k] for k in ("config", "listening", "listen_error")}


@app.post("/api/config")
async def set_config(cfg: ConfigIn):
    await proxy.apply_config(ProxyConfig(**cfg.model_dump()))
    snap = proxy.snapshot()
    return {"ok": snap["listening"],
            **{k: snap[k] for k in ("config", "listening", "listen_error")}}


@app.get("/api/connections")
async def connections():
    return proxy.snapshot()


@app.post("/api/history/clear")
async def clear_history():
    proxy.history.clear()
    return {"ok": True}


async def _read_to_eof(reader: asyncio.StreamReader) -> bytes:
    parts = []
    while True:
        data = await reader.read(65536)
        if not data:
            return b"".join(parts)
        parts.append(data)


async def _find_conn_by_client(client: str) -> dict | None:
    """在活动/历史连接里找到本次试传对应的连接（连接可能已进入历史表）。"""
    for _ in range(40):
        snap = proxy.snapshot()
        matches = [c for c in snap["history"] + snap["active"] if c["client"] == client]
        if matches:
            return max(matches, key=lambda c: c["id"])
        await asyncio.sleep(0.1)
    return None


@app.post("/api/test-upload")
async def test_upload(file: UploadFile):
    """把上传的文件经本代理转发到目标：先读欢迎行，发完文件后半关闭，
    再读回执直到 EOF。整条路径都经过延迟代理，等同真实设备链路。"""
    snap = proxy.snapshot()
    if not snap["listening"]:
        raise HTTPException(503, f"proxy listener not running: {snap['listen_error']}")
    cfg = proxy.cfg
    delay_s = (cfg.up_delay_ms + cfg.down_delay_ms) / 1000.0
    result: dict = {"file_name": file.filename, "bytes_sent": 0, "ok": False,
                    "blocked": False}
    writer = None
    t0 = time.monotonic()
    try:
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection("127.0.0.1", cfg.listen_port), timeout=15)
        local_port = writer.get_extra_info("sockname")[1]
        client_key = f"127.0.0.1:{local_port}"
        # 目标（示例接收端）先发的欢迎行；自定义目标可能不发，探测失败不阻塞上传
        try:
            welcome = await asyncio.wait_for(reader.readline(),
                                             timeout=max(5.0, delay_s + 5))
            result["welcome"] = welcome.decode("utf-8", "replace").strip()
        except Exception:
            result["welcome"] = None
        try:
            while True:
                chunk = await file.read(UPLOAD_CHUNK)
                if not chunk:
                    break
                writer.write(chunk)
                await writer.drain()
                result["bytes_sent"] += len(chunk)
            # 半关闭：目标读到 EOF 后才返回回执
            if writer.can_write_eof():
                writer.write_eof()
            else:
                writer.close()
            receipt = await asyncio.wait_for(_read_to_eof(reader),
                                             timeout=max(120.0, 2 * delay_s + 60))
        except (ConnectionError, OSError) as exc:
            # 代理命中禁发标记后会终止连接，下面先确认是“被拦截”而非普通断连
            receipt = b""
            error = exc
        else:
            error = None
        result["receipt"] = receipt.decode("utf-8", "replace").strip()
        conn = await _find_conn_by_client(client_key)
        if conn is not None:
            result["connection_id"] = conn["id"]
        if conn is not None and conn["blocked"]:
            # 命中禁发标记：即使目标已回过前缀/回执，试传也不算成功
            result["blocked"] = True
            result["ok"] = False
            result["error"] = conn.get("block_reason") or "blocked by proxy"
        elif error is not None:
            raise error
        else:
            result["ok"] = True
    except Exception as exc:
        result["error"] = f"{type(exc).__name__}: {exc}"
    finally:
        result["duration_ms"] = round((time.monotonic() - t0) * 1000, 1)
        if writer is not None:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass
        await file.close()
    return result
