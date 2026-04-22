"""
mcp_server.py — Builds the SDK MCP server that injects all sql_tools into sub-agents.

Usage in orchestrator:
    from tools.mcp_server import build_mcp_server
    mcp_server = build_mcp_server(run_log=run_log)
    options = CortexCodeAgentOptions(mcp_servers={"sv-tools": mcp_server}, ...)
"""

import functools
import time

from cortex_code_agent_sdk import create_sdk_mcp_server
from tools.sql_tools import ALL_TOOLS


def _make_logged_tool(tool, run_log):
    """Wrap a tool's underlying function to emit tool_call JSONL events."""
    tool_name = getattr(tool, "name", getattr(tool, "__name__", str(tool)))

    # Locate the underlying coroutine to wrap
    original_fn = getattr(tool, "fn", None)
    if original_fn is None and callable(tool):
        original_fn = tool
    if original_fn is None:
        return tool  # SDK structure unknown — return unwrapped

    @functools.wraps(original_fn)
    async def logged_fn(args: dict) -> dict:
        _t0 = time.monotonic()
        # Summarise args — omit large SQL strings to keep events readable
        try:
            args_summary = {
                k: (str(v)[:120] + "…" if isinstance(v, str) and len(v) > 120 else v)
                for k, v in args.items()
                if k not in ("sql", "tables_json", "sql_examples_json")
            }
        except Exception:
            args_summary = {}
        try:
            result = await original_fn(args)
            duration_ms = round((time.monotonic() - _t0) * 1000)
            try:
                result_len = len(str(result))
            except Exception:
                result_len = 0
            run_log.emit(
                "tool_call",
                tool=tool_name,
                duration_ms=duration_ms,
                args_summary=args_summary,
                result_len=result_len,
            )
            return result
        except Exception as exc:
            duration_ms = round((time.monotonic() - _t0) * 1000)
            run_log.emit(
                "tool_call",
                tool=tool_name,
                duration_ms=duration_ms,
                args_summary=args_summary,
                error=str(exc),
            )
            raise

    # Replace the underlying function on the tool object in-place
    if hasattr(tool, "fn"):
        tool.fn = logged_fn
        return tool
    return logged_fn


def build_mcp_server(run_log=None):
    """Return an McpSdkServerConfig with all SV automation tools registered.

    When run_log is provided, every tool call emits a ``tool_call`` event to
    the JSONL event stream so callers can observe tool-level latency and usage.
    """
    tools = ALL_TOOLS
    if run_log is not None:
        tools = [_make_logged_tool(t, run_log) for t in ALL_TOOLS]
    return create_sdk_mcp_server(
        name="sv-automation-tools",
        version="0.2.0",
        tools=tools,
    )
