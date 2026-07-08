"""
fastapi_bridge.py — Turn your local Claude Code CLI into an OpenAI-compatible HTTP API.

A single-file FastAPI backend that lets any frontend speaking the OpenAI
Chat Completions protocol (Internal Beyond, ChatGPT-Next-Web, or anything
with a custom OpenAI endpoint setting) chat through your locally installed
`claude` CLI (Claude Code). For personal local use with your own Claude
Code CLI session.

单文件最小可运行版本：任何 OpenAI 兼容前端 -> 本 FastAPI -> 本地 claude CLI。
个人本地使用，通过本地 Claude Code CLI。

Disclaimer: this is an unofficial personal bridge, not affiliated with or
endorsed by Anthropic or Claude. It is intended for personal/local use.
Do not expose it to the public internet. Users are responsible for
complying with Anthropic's terms of service.

Exposes:
  POST /chat/completions      OpenAI chat-completions compatible, SSE streaming
  POST /v1/chat/completions   same endpoint (for frontends that hardcode /v1)
  GET  /models, /v1/models    minimal model list (some frontends probe this)
  GET  /health                sanity check (also shows resolved claude binary)

Run:
  pip install fastapi uvicorn
  python fastapi_bridge.py          # listens on 127.0.0.1:8000
                                    # (BRIDGE_HOST / BRIDGE_PORT to override)

Requires the `claude` CLI (Claude Code) installed and logged in.

Three hard-won fixes are baked in (see README.md for details):
  1. Prompt is piped via stdin, never argv  (Windows cmd.exe newline truncation)
  2. Subprocess pipe limit raised to 16 MB  (asyncio 64 KB line limit)
  3. 20 s keepalive chunks while streaming  (frontend stream-inactivity timeouts)
"""

import asyncio
import hashlib
import json
import os
import shutil
import sys
import time
import uuid
from typing import Any, Optional

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Configuration 配置
# ---------------------------------------------------------------------------

# Origins allowed to call this backend. "null" covers pages opened via file://.
# Override with BRIDGE_ALLOWED_ORIGINS (comma-separated), or "*" to allow all
# (only do that on a trusted machine).
# 允许访问的来源。"null" 对应直接双击 file:// 打开的页面。
# 可用 BRIDGE_ALLOWED_ORIGINS 环境变量覆盖（逗号分隔），"*" 表示全部放行。
_origins_env = os.environ.get("BRIDGE_ALLOWED_ORIGINS", "").strip()
if _origins_env == "*":
    ALLOWED_ORIGINS = ["*"]
elif _origins_env:
    ALLOWED_ORIGINS = [o.strip() for o in _origins_env.split(",") if o.strip()]
else:
    ALLOWED_ORIGINS = [
        "null",
        "http://localhost:5174",
        "http://127.0.0.1:5174",
        "http://localhost:3000",
        "http://127.0.0.1:3000",
    ]

# Working directory for the claude subprocess (decides which CLAUDE.md /
# project context it loads). Defaults to your home directory.
# claude 子进程的工作目录（决定它加载哪个 CLAUDE.md）。默认用户主目录。
CLAUDE_CWD = os.environ.get("CLAUDE_CWD", os.path.expanduser("~"))

# Bind address / port. Defaults to loopback only; set BRIDGE_HOST to override
# (e.g. BRIDGE_HOST=0.0.0.0 for a trusted LAN — never the public internet).
# 监听地址/端口。默认只绑本机回环；需要局域网访问时用 BRIDGE_HOST 环境变量覆盖。
HOST = os.environ.get("BRIDGE_HOST", "127.0.0.1")
PORT = int(os.environ.get("BRIDGE_PORT", "8000"))

# stream-json lines can be huge (hook output, full thinking blocks); asyncio's
# default 64 KB StreamReader limit raises LimitOverrunError mid-stream.
# 关键修复 2：asyncio 默认单行 64KB 上限会炸，提到 16MB。
PIPE_LIMIT = 16 * 1024 * 1024

# Emit an empty delta if the CLI is silent this long, so frontends with a
# stream-inactivity timeout (e.g. Internal Beyond kills quiet streams after
# 45 s) don't abort during cold starts.
# 关键修复 3：CLI 静默超过 20 秒就发一个空 chunk，喂住前端的流式心跳超时。
KEEPALIVE_SECONDS = 20.0

# Model name echoed back when the client doesn't send one.
# 客户端没传 model 时回显的默认模型名。
DEFAULT_MODEL_NAME = "claude-cli"

# ---------------------------------------------------------------------------

app = FastAPI(title="Claude CLI Bridge", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=ALLOWED_ORIGINS != ["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Resolve the claude CLI once at import time so failures surface immediately.
CLAUDE_BIN = shutil.which("claude")


class OpenAIChatMessage(BaseModel):
    role: str
    content: Optional[Any] = None


class OpenAIChatCompletionsRequest(BaseModel):
    model: Optional[str] = None
    messages: list[OpenAIChatMessage]
    stream: Optional[bool] = True
    temperature: Optional[float] = None
    top_p: Optional[float] = None
    max_tokens: Optional[int] = None
    max_completion_tokens: Optional[int] = None
    thinking: Optional[dict] = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _extract_text(content: Any) -> str:
    """Pull plain text out of an OpenAI-style `content` field (string or
    list of content parts)."""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if isinstance(item, dict):
                text_value = item.get("text")
                if isinstance(text_value, str):
                    parts.append(text_value)
                elif isinstance(item.get("content"), str):
                    parts.append(item["content"])
        return "\n".join(parts)
    if content is None:
        return ""
    return str(content)


def _build_transcript(messages: list[dict]) -> str:
    """Flatten a full OpenAI-style message list into one plain-text prompt,
    used the first time we see a conversation (no cached claude session)."""
    role_labels = {"system": "System", "user": "User", "assistant": "Assistant", "tool": "Tool"}
    lines: list[str] = []
    for msg in messages:
        role = msg.get("role", "user")
        text = _extract_text(msg.get("content"))
        if not text.strip():
            continue
        label = role_labels.get(role, str(role).title())
        lines.append(f"[{label}]\n{text.strip()}")
    return "\n\n".join(lines).strip()


def _hash_messages(messages: list[dict]) -> str:
    normalized = [
        {"role": m.get("role", ""), "content": _extract_text(m.get("content"))}
        for m in messages
    ]
    blob = json.dumps(normalized, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


# The OpenAI protocol has no session id — clients resend the whole message
# list every turn. We map hash(messages so far) -> claude session_id so
# follow-up turns can ride on `claude --resume` instead of replaying history.
# OpenAI 协议没有会话 id，用"历史消息哈希 -> claude session_id"映射实现多轮续聊。
_SESSION_CACHE_LIMIT = 200
_session_cache: dict[str, str] = {}
_session_cache_order: list[str] = []


def _store_session(key: str, session_id: str) -> None:
    if key not in _session_cache:
        _session_cache_order.append(key)
        if len(_session_cache_order) > _SESSION_CACHE_LIMIT:
            oldest = _session_cache_order.pop(0)
            _session_cache.pop(oldest, None)
    _session_cache[key] = session_id


# ---------------------------------------------------------------------------
# Claude CLI subprocess
# ---------------------------------------------------------------------------

async def _iter_claude_events(prompt: str, session_id: Optional[str], include_thinking: bool):
    """Run the claude CLI once and yield normalized events:
    session / text / thinking / result / process_error / done."""
    if not CLAUDE_BIN:
        yield {"type": "process_error", "message": "claude CLI not found on PATH"}
        yield {"type": "done"}
        return

    # KEY FIX 1: the prompt is piped via stdin, NOT argv. On Windows the
    # claude binary is a .CMD wrapper; cmd.exe truncates arguments at the
    # first newline, silently dropping the rest of the command line
    # (including --output-format), which turns the stream into unparseable
    # plain text.
    # 关键修复 1：prompt 走 stdin。Windows 上 cmd.exe 会在换行处截断参数。
    args = [
        CLAUDE_BIN,
        "-p",
        "--output-format",
        "stream-json",
        "--include-partial-messages",
        "--verbose",
    ]
    if session_id:
        args += ["--resume", session_id]

    try:
        proc = await asyncio.create_subprocess_exec(
            *args,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=CLAUDE_CWD,
            limit=PIPE_LIMIT,  # KEY FIX 2 关键修复 2
        )
    except OSError as exc:
        yield {"type": "process_error", "message": f"failed to launch claude CLI: {exc}"}
        yield {"type": "done"}
        return

    try:
        assert proc.stdin is not None
        proc.stdin.write(prompt.encode("utf-8"))
        await proc.stdin.drain()
        proc.stdin.close()
    except (OSError, BrokenPipeError) as exc:
        proc.kill()
        yield {"type": "process_error", "message": f"failed to send prompt: {exc}"}
        yield {"type": "done"}
        return

    current_session_id = session_id

    try:
        assert proc.stdout is not None
        async for raw_line in proc.stdout:
            line = raw_line.decode("utf-8", errors="replace").strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue

            sid = obj.get("session_id")
            if sid and sid != current_session_id:
                current_session_id = sid
                yield {"type": "session", "session_id": current_session_id}

            obj_type = obj.get("type")

            if obj_type == "stream_event":
                event = obj.get("event", {})
                if event.get("type") == "content_block_delta":
                    delta = event.get("delta", {})
                    if delta.get("type") == "text_delta" and delta.get("text"):
                        yield {"type": "text", "text": delta["text"]}
                    elif delta.get("type") == "thinking_delta" and include_thinking and delta.get("thinking"):
                        yield {"type": "thinking", "text": delta["thinking"]}

            elif obj_type == "result":
                yield {
                    "type": "result",
                    "is_error": obj.get("is_error", False),
                    "result": obj.get("result", ""),
                    "session_id": obj.get("session_id"),
                }

        stderr_bytes = await proc.stderr.read() if proc.stderr else b""
        await proc.wait()

        if proc.returncode != 0:
            stderr_text = stderr_bytes.decode("utf-8", errors="replace").strip()
            yield {
                "type": "process_error",
                "message": f"claude exited with code {proc.returncode}",
                "stderr": stderr_text[-2000:],
            }

    except asyncio.CancelledError:
        # Client disconnected; don't leak the child process.
        if proc.returncode is None:
            proc.kill()
        raise
    finally:
        if proc.returncode is None:
            try:
                proc.kill()
            except ProcessLookupError:
                pass

    yield {"type": "done"}


# ---------------------------------------------------------------------------
# OpenAI-compatible SSE streaming
# ---------------------------------------------------------------------------

def _openai_chunk(completion_id: str, created_ts: int, model_name: str,
                  delta: dict, finish_reason: Optional[str] = None) -> str:
    payload = {
        "id": completion_id,
        "object": "chat.completion.chunk",
        "created": created_ts,
        "model": model_name,
        "choices": [{"index": 0, "delta": delta, "finish_reason": finish_reason}],
    }
    return f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"


async def _stream_response(prompt, session_id, model_name, include_thinking,
                           completion_id, created_ts, conversation_messages):
    yield _openai_chunk(completion_id, created_ts, model_name, {"role": "assistant"})

    assistant_text = ""
    final_session_id = session_id
    had_error = False
    result_text = ""

    # KEY FIX 3: pump claude events through a queue so we can emit keepalive
    # chunks when the CLI is silent (cold start / long tool turns). Many
    # frontends abort streams that go quiet for too long.
    # 关键修复 3：事件走队列，静默时发空 chunk 喂住前端的流式超时。
    queue: asyncio.Queue = asyncio.Queue()

    async def _produce():
        try:
            async for ev in _iter_claude_events(prompt, session_id, include_thinking):
                await queue.put(ev)
        except Exception as exc:
            await queue.put({"type": "process_error", "message": f"stream reader failed: {exc}"})
            await queue.put({"type": "done"})

    producer = asyncio.create_task(_produce())

    try:
        while True:
            try:
                event = await asyncio.wait_for(queue.get(), timeout=KEEPALIVE_SECONDS)
            except asyncio.TimeoutError:
                # keepalive: empty delta, ignored by clients but resets timers
                yield _openai_chunk(completion_id, created_ts, model_name, {"content": ""})
                continue

            kind = event["type"]
            if kind == "session":
                final_session_id = event["session_id"]
            elif kind == "text":
                assistant_text += event["text"]
                yield _openai_chunk(completion_id, created_ts, model_name, {"content": event["text"]})
            elif kind == "thinking":
                yield _openai_chunk(completion_id, created_ts, model_name, {"reasoning_content": event["text"]})
            elif kind == "result":
                if event.get("session_id"):
                    final_session_id = event["session_id"]
                if event.get("is_error"):
                    had_error = True
                if isinstance(event.get("result"), str):
                    result_text = event["result"]
            elif kind == "process_error":
                had_error = True
                msg = event.get("message", "claude CLI error")
                yield f"data: {json.dumps({'error': {'message': msg}}, ensure_ascii=False)}\n\n"
            elif kind == "done":
                break
    finally:
        # Client disconnect closes this generator here; cancelling the
        # producer kills the claude subprocess.
        producer.cancel()

    # Some turns never emit a text_delta (whole turn spent on tool calls);
    # the CLI's final result event still carries the reply text.
    if not assistant_text.strip() and result_text.strip():
        assistant_text = result_text
        yield _openai_chunk(completion_id, created_ts, model_name, {"content": result_text})

    yield _openai_chunk(completion_id, created_ts, model_name, {},
                        finish_reason="error" if had_error else "stop")

    if final_session_id and assistant_text.strip():
        next_key = _hash_messages(conversation_messages + [{"role": "assistant", "content": assistant_text}])
        _store_session(next_key, final_session_id)

    yield "data: [DONE]\n\n"


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.post("/chat/completions")
@app.post("/v1/chat/completions")
async def chat_completions(req: OpenAIChatCompletionsRequest):
    messages = [m.model_dump() for m in req.messages]
    if not messages:
        raise HTTPException(status_code=400, detail="messages must not be empty")

    # Multi-turn: look up a cached claude session for "all messages but the
    # last". Hit -> resume with just the new message; miss -> full transcript.
    prior_messages = messages[:-1]
    cached_session_id = _session_cache.get(_hash_messages(prior_messages))

    if cached_session_id:
        session_id = cached_session_id
        prompt = _extract_text(messages[-1].get("content"))
    else:
        session_id = None
        prompt = _build_transcript(messages)

    if not prompt.strip():
        raise HTTPException(status_code=400, detail="no usable text content in messages")

    model_name = (req.model or "").strip() or DEFAULT_MODEL_NAME
    include_thinking = bool(req.thinking)
    completion_id = f"chatcmpl-{uuid.uuid4().hex}"
    created_ts = int(time.time())

    if req.stream is False:
        # Non-streaming fallback (most frontends use streaming by default).
        assistant_text = ""
        final_session_id = session_id
        had_error = False
        error_message = ""
        result_text = ""

        async for event in _iter_claude_events(prompt, session_id, include_thinking):
            kind = event["type"]
            if kind == "session":
                final_session_id = event["session_id"]
            elif kind == "text":
                assistant_text += event["text"]
            elif kind == "result":
                if event.get("session_id"):
                    final_session_id = event["session_id"]
                if event.get("is_error"):
                    had_error = True
                if isinstance(event.get("result"), str):
                    result_text = event["result"]
            elif kind == "process_error":
                had_error = True
                error_message = event.get("message", "")

        if not assistant_text.strip() and result_text.strip():
            assistant_text = result_text
        if had_error and not assistant_text.strip():
            return JSONResponse(status_code=502, content={"error": {"message": error_message or "claude CLI error"}})
        if final_session_id and assistant_text.strip():
            next_key = _hash_messages(messages + [{"role": "assistant", "content": assistant_text}])
            _store_session(next_key, final_session_id)

        return {
            "id": completion_id,
            "object": "chat.completion",
            "created": created_ts,
            "model": model_name,
            "choices": [{
                "index": 0,
                "message": {"role": "assistant", "content": assistant_text},
                "finish_reason": "error" if had_error else "stop",
            }],
        }

    return StreamingResponse(
        _stream_response(prompt, session_id, model_name, include_thinking,
                         completion_id, created_ts, messages),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",  # disable proxy buffering, if any
            "Connection": "keep-alive",
        },
    )


@app.get("/models")
@app.get("/v1/models")
async def models():
    # Minimal OpenAI-style model list; some frontends probe this on setup.
    # 最小化的模型列表，部分前端配置时会请求它。
    return {
        "object": "list",
        "data": [{
            "id": DEFAULT_MODEL_NAME,
            "object": "model",
            "created": 0,
            "owned_by": "local",
        }],
    }


@app.get("/health")
async def health():
    return {"status": "ok", "claude_bin": CLAUDE_BIN, "python": sys.version}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("fastapi_bridge:app", host=HOST, port=PORT, reload=False)
