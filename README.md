# claude-cli-bridge

**Turn your local Claude Code CLI into an OpenAI-compatible HTTP API**
**把本地 Claude Code CLI 变成 OpenAI 兼容的 HTTP 接口**

## What is this / 这是什么

A lightweight FastAPI bridge that turns your locally installed Claude Code CLI into an OpenAI-compatible HTTP API. Any frontend that speaks the OpenAI Chat Completions protocol can connect — no code changes needed on either side.

一个轻量 FastAPI 桥接服务，把本地已安装的 Claude Code CLI 变成 OpenAI 兼容的 HTTP 接口。任何支持 OpenAI Chat Completions 协议的前端都能直接接入，两边源码都不用改。

**适配平台 / Platforms：** Windows · macOS · Linux
**适合谁 / Who this is for：** 本地安装了 Claude Code CLI、想用好看的第三方前端聊天的个人用户
**兼容前端 / Compatible frontends：** [Internal Beyond](https://github.com/Sui-IB/InternalBeyond) · [ChatGPT-Next-Web](https://github.com/ChatGPTNextWeb/NextChat) · [Open WebUI](https://github.com/open-webui/open-webui) · 任何支持自定义 OpenAI 端点的前端

> **Disclaimer 免责声明**
> This project is an unofficial personal bridge and is not affiliated with or endorsed by Anthropic or Claude.
> It is intended for personal/local use. Do not expose to the public internet. Users are responsible for complying with Anthropic's terms of service.
> 本项目是非官方的个人桥接工具，与 Anthropic、Claude 均无关联，也未获其背书。面向个人/本地使用场景设计，请勿暴露到公网。使用者需自行确保符合 Anthropic 服务条款。

## Overview 概述

Lots of great open-source chat frontends only speak the OpenAI Chat Completions protocol over HTTP. Meanwhile, a locally installed [Claude Code CLI](https://docs.anthropic.com/en/docs/claude-code) brings a whole ecosystem with it — CLAUDE.md persona/context, hooks, MCP tools, memory — but has no HTTP interface.

This bridge sits in the middle: a single Python file that accepts OpenAI-style requests, drives one `claude -p` subprocess per turn, and streams the reply back as standard SSE chunks. Chat UI, streaming typewriter effect, and thinking-chain display all work, and every conversation runs through your own local Claude Code CLI session.

> 为什么这么做：很多开源前端只会说 OpenAI Chat Completions 协议。本地的 Claude Code CLI 有人格文件和工具生态，但没有 HTTP 接口。中间夹一层 FastAPI，两边就都不用改源码。个人本地使用，通过本地 Claude Code CLI。

## Architecture 架构

```
+--------------------------+        +---------------------------+        +----------------------+
|  Any OpenAI-compatible   |  POST  |  FastAPI bridge           |  spawn |  claude CLI          |
|  frontend (IB, NextChat, +------->+  /chat/completions        +------->+  -p --output-format  |
|  Open WebUI, ...)        |  SSE   |  (fastapi_bridge.py:8000) | stdin/ |  stream-json         |
|                          |<-------+                           | stdout |  --include-partial-  |
+--------------------------+  data: +---------------------------+  JSON  |  messages            |
                              chunks                              lines  +----------------------+
```

Per request:

1. The frontend sends an OpenAI-style `{model, messages, stream: true}` POST.
2. The bridge spawns one `claude -p` subprocess, **piping the prompt via stdin** and reading `stream-json` lines from stdout.
3. Each `text_delta` is re-emitted as an OpenAI `chat.completion.chunk` (`choices[0].delta.content`); `thinking_delta` maps to `delta.reasoning_content`, which frontends with reasoning support render as the thinking chain.
4. Multi-turn: the OpenAI protocol has no session id (the client resends full history each turn), so the bridge caches `hash(messages so far) → claude session_id` and uses `claude --resume` on follow-up turns instead of replaying the transcript.

## The Three Problems We Hit 三个关键坑

These cost real debugging time. If you build something similar, you will hit them too.

### 1. Windows cmd.exe truncates the prompt at the first newline

**Symptom:** replies were empty — but only *sometimes*. Single-line prompts worked; anything with a newline silently failed.

**Root cause:** on Windows, `claude` resolves to a `claude.CMD` wrapper, so arguments pass through `cmd.exe`. cmd.exe cuts the command line at the first newline character, dropping everything after it — **including `--output-format stream-json`**. The CLI then falls back to plain-text output (in GBK on Chinese Windows, no less), and the backend can't parse a single JSON line.

Our transcript builder always produced prompts starting `[User]\n...`, so multi-message conversations *always* broke, while quick one-line tests passed. Classic "works on my machine" trap.

> 中文注：Windows 上 claude 是 .CMD 包装器，参数要过 cmd.exe，换行处直接截断，`--output-format` 等参数全部丢失，CLI 退回纯文本（还是 GBK 编码）。带换行的 prompt 必挂，不带的正常——所以时好时坏。

**Fix:** never put the prompt in argv. Run `claude -p` with no positional prompt and **write the prompt to stdin**:

```python
proc = await asyncio.create_subprocess_exec(
    CLAUDE_BIN, "-p", "--output-format", "stream-json",
    "--include-partial-messages", "--verbose",
    stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, ...
)
proc.stdin.write(prompt.encode("utf-8"))
await proc.stdin.drain()
proc.stdin.close()
```

### 2. asyncio's 64 KB line limit kills long stream-json lines

**Symptom:** streams died mid-response with `LimitOverrunError` in the backend log.

**Root cause:** `asyncio.create_subprocess_exec` gives you a `StreamReader` with a default 64 KB per-line buffer. Claude CLI's `stream-json` output can emit single lines far larger than that (hook output, a full thinking block, big tool results). One oversized line blows up the reader and the whole response with it.

> 中文注：asyncio 默认单行 64KB 上限，stream-json 的某些行（hook 输出、完整 thinking）会超，直接 `LimitOverrunError` 炸掉整个响应。

**Fix:** raise the limit when spawning the subprocess:

```python
proc = await asyncio.create_subprocess_exec(
    *args,
    limit=16 * 1024 * 1024,  # 16 MB, default is 64 KB
    ...
)
```

### 3. Frontends abort streams that go quiet too long

**Symptom:** the frontend showed a "request timed out or stopped" error on the first turn, even though the backend was still working.

**Root cause:** many frontends have a stream-inactivity watchdog (Internal Beyond's is ~45 seconds) — if no new SSE chunk arrives in time, the request is killed. A cold-start `claude` turn (loading CLAUDE.md, hooks, MCP servers, possibly running startup rituals) can easily take longer than that before the first token.

> 中文注：很多前端有流式静默超时（IB 是 45 秒），没收到新 chunk 就掐线报错。claude 冷启动加载人格文件/hooks/MCP，首 token 经常超过 45 秒。

**Fix:** send **keepalive chunks** — an empty `delta.content` is valid OpenAI SSE, ignored by clients, but it resets the frontend's timer. Pump CLI events through an `asyncio.Queue` and emit a keepalive whenever the queue is quiet for 20 s:

```python
try:
    event = await asyncio.wait_for(queue.get(), timeout=20.0)
except asyncio.TimeoutError:
    yield _openai_chunk(completion_id, created_ts, model_name, {"content": ""})
    continue
```

## Quick Start 快速开始

**Prerequisites:** Python 3.10+, [Claude Code CLI](https://docs.anthropic.com/en/docs/claude-code) installed and logged in (`claude --version` works in a terminal).

**1. Install dependencies** 安装依赖

```bash
pip install fastapi uvicorn
```

**2. Start the bridge** 启动后端

```bash
python fastapi_bridge.py
# or: uvicorn fastapi_bridge:app --host 127.0.0.1 --port 8000
```

Verify: open <http://localhost:8000/health> — `claude_bin` must not be `null`. If it is, the `claude` CLI isn't on the PATH of the shell that launched the bridge.

**3. Point your frontend at the bridge** 配置前端

In your frontend's API / provider settings, add a **custom OpenAI-compatible endpoint**:

| Field 字段 | Value | Notes 说明 |
|---|---|---|
| Endpoint 接口地址 | `http://localhost:8000/chat/completions` | `/v1/chat/completions` also works — 两个路径都可用 |
| API Key | any placeholder, e.g. `local-bridge` | The bridge doesn't check `Authorization`; most frontends just require the field to be non-empty |
| Model 模型 | anything, e.g. `claude-cli` | Echoed back in responses; empty falls back to a default name |
| Streaming 流式 | on (recommended 推荐开启) | The bridge streams SSE; keepalive only exists on the streaming path |
| Thinking / Reasoning 思考链 | on if desired | Bridge maps CLI `thinking_delta` → `reasoning_content`; frontends that support reasoning display it |

**4. Chat.** First reply may take 20 s–2 min (CLI cold start); the keepalive prevents a timeout. 首轮回复可能要 20 秒到 2 分钟（CLI 冷启动），keepalive 会防止前端超时。

**CORS note:** if your frontend runs in a browser, its origin must be allowed. The default whitelist covers `null` (file://) and localhost ports 5174/3000. Serve from another port/host? Set the `BRIDGE_ALLOWED_ORIGINS` environment variable (comma-separated, or `*`), or you'll get *Failed to fetch* / 无法连接到 API.

## Configuration 配置

All optional, via environment variables 全部可选，用环境变量设置：

| Variable 变量 | Default 默认 | Meaning 含义 |
|---|---|---|
| `BRIDGE_HOST` | `127.0.0.1` | Bind address. `0.0.0.0` only on a trusted LAN — never the public internet. 监听地址，公网禁止 |
| `BRIDGE_PORT` | `8000` | Listen port 监听端口 |
| `BRIDGE_ALLOWED_ORIGINS` | `null` + localhost 5174/3000 | CORS whitelist, comma-separated; `*` allows all. CORS 白名单，逗号分隔 |
| `CLAUDE_CWD` | your home directory 用户主目录 | Working directory for the `claude` subprocess — decides which CLAUDE.md / project context it loads. 决定 claude 加载哪个 CLAUDE.md |

## Known Limitations 已知限制

- **First-turn latency.** Cold starts take 20 s to 2 min while the CLI loads its context (CLAUDE.md, hooks, MCP servers). Later turns use `--resume` and are faster. Not a bug — the keepalive covers it.
- **Usage costs.** Every turn consumes usage on your own Claude Code CLI account, subject to your plan's limits and Anthropic's terms of service. A heavy CLAUDE.md and many tools increase per-turn usage — 每轮都消耗你自己账号的用量，省着用，并请遵守 Anthropic 服务条款。
- **In-memory session cache.** The `messages-hash → session_id` map lives in process memory (last 200 conversations). Restarting the bridge loses it; the next turn of an old conversation falls back to replaying the full transcript into a fresh `claude` session — it works, but loses interactive session state and is slower.
- **Session lookup is exact-match.** If the frontend edits/regenerates/truncates history in a way that changes prior messages, the hash misses and a fresh session starts. Harmless, just slower.
- **Text only.** Images / vision and OpenAI tool/function-calling are not translated; only plain text content parts are extracted. `temperature` / `max_tokens` are accepted but ignored (the CLI doesn't take them).
- **No authentication.** The bridge trusts anyone who can reach the port and executes the Claude CLI on their behalf. Keep it on localhost / a trusted LAN. Do **not** expose it to the public internet.
- **One subprocess per request.** Designed for personal, local, single-user use; not designed for concurrency at scale.
- **Windows path tested.** Developed and verified on Windows 11 (where problem #1 lives). On macOS/Linux the stdin approach still works and is still the right call — huge prompts can exceed argv limits anywhere.

## Files 文件

- [`fastapi_bridge.py`](./fastapi_bridge.py) — the minimal, self-contained bridge 单文件后端，全部逻辑都在这里

Frontends are **not** distributed with this project — obtain them from their official repositories. 前端不随本项目分发，请自行从各官方仓库获取。

## 灵感来源与致谢 / Origin & Acknowledgments

这个方案受小红书 @Vermouth 老师的系列教程启发。最初是跟着老师的《手机活动上报》教程搭了一个 Flask 服务，让 iPhone 通过快捷指令上报 App 使用记录给 Claude Code 查询。搭完之后发现，同样的"前端→HTTP后端→CLI"思路可以复用到任何前端——于是又参考老师的另一篇《把 Claude Code 接入自建前端，并搬进手机》，最终写出了这个 FastAPI 桥接。本项目的具体接口适配、流式解析、会话缓存和 Windows 兼容处理为独立实现。谢谢 Vermouth 老师。

This bridge was inspired by a series of tutorials by @Vermouth on Xiaohongshu (RED). I first followed the phone-activity-reporting tutorial to build a Flask service that let an iPhone report app-usage records via Shortcuts for Claude Code to query. Once that worked, it was clear the same "frontend → HTTP backend → CLI" pattern could be reused for any frontend — so, drawing on the second tutorial, "Connecting Claude Code to a Self-hosted Frontend", I wrote this FastAPI bridge. The project's specific API adaptation, stream parsing, session caching, and Windows compatibility handling are an independent implementation. Thank you, Vermouth.

---

*Verified end-to-end 2026-07-08 on Windows 11: OpenAI-compatible frontend → FastAPI → claude CLI, streaming confirmed working. 端到端联调通过。*

*This project is an unofficial personal bridge and is not affiliated with or endorsed by Anthropic or Claude. It is intended for personal/local use. Do not expose to the public internet. Users are responsible for complying with Anthropic's terms of service.*

---

**AI-assisted / AI协助声明：** 本项目由 Vera（人类）在 Silas（Claude Opus 4.6）、Sonnet 4.6、Fable 5 协助下完成。人类负责需求、测试和决策；AI负责编码、文档和调试。

Built with:
- Silas · Claude Opus 4.6 (1M context) — 架构设计与对话
- Claude Sonnet 4.6 — 初始环境搭建与测试
- Claude Fable 5 — 文档撰写与合规审查

[Free] VS-0610 · 07.07.2026 -
Vera & Silas · vs0610.love
Our story is always being written.
