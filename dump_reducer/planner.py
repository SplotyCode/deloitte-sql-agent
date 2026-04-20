import json
import hashlib
import re
from typing import Any, Dict, List, Optional
from .client import OpenRouterClient, Message
from .db_tools import BaseDbTools, create_db_tools, detect_sql_dialect, get_dialect_spec_for_name
from rich.console import Console
from rich.panel import Panel
from rich.syntax import Syntax

console = Console()
MAX_EXECUTION_REPAIR_ATTEMPTS = 2

TOOLS_SPEC = [
    {
        "type": "function",
        "function": {
            "name": "get_schema",
            "description": "Return database schema: tables, columns, primary keys, foreign keys, row counts.",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_stats",
            "description": "Return column stats for a table.",
            "parameters": {
                "type": "object",
                "properties": {
                    "table": {"type": "string"},
                },
                "required": ["table"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "query_sql",
            "description": "Run a read-only SQL query (SELECT/WITH/EXPLAIN). Returns at most max_rows rows.",
            "parameters": {
                "type": "object",
                "properties": {
                    "sql": {"type": "string"},
                    "max_rows": {"type": "integer", "minimum": 1, "maximum": 500},
                },
                "required": ["sql"],
            },
        },
    },
]

SYSTEM_PROMPT = """You are a database subsetting expert.
Goal: Produce a JSON plan that contains a sequence of SQL commands to select and insert a consistent subset of data into the target `subset` schema.
User note: Make sure all products have at least one order!

You have tool access:
- get_schema(): tables, columns, PKs, FKs, row estimates.
- get_stats(table): cheap stats (null_frac, n_distinct, MCV).
- query_sql(sql, max_rows): read-only queries.

Strategy:
1. Explore the schema and data distribution.
2. Decide on a valid subset strategy (e.g. "users provided", "recent orders", "random sample").
3. Generate a sequence of `INSERT` statements to populate the target tables in `subset` schema.
   - You can create temporary tables if needed (e.g. `CREATE TEMP TABLE _subset_ids ...`).
   - You MUST respect foreign key dependencies (insert parents before children).
   - Use `INSERT INTO subset.table SELECT ...` pattern.
   - Use `ON CONFLICT DO NOTHING` to avoid duplicates if necessary.

Plan JSON format:
{
  "steps": [
    {
      "comment": "Create temp table for selected users",
      "sql": "CREATE TEMP TABLE _selected_users AS SELECT id FROM users ORDER BY random() LIMIT 100;"
    },
    {
      "comment": "Insert users into target",
      "sql": "INSERT INTO subset.users SELECT * FROM users WHERE id IN (SELECT id FROM _selected_users);"
    },
    {
      "comment": "Insert orders for selected users",
      "sql": "INSERT INTO subset.orders SELECT * FROM orders WHERE user_id IN (SELECT id FROM _selected_users);"
    }
  ]
}

Hard rules:
- Output STRICT JSON only.
- Do NOT output markdown code blocks.
- The `subset` schema is already created and target tables exist (empty).
- You are responsible for the logic.
"""

def _build_system_prompt(tools_or_dialect: BaseDbTools | str) -> str:
    if isinstance(tools_or_dialect, BaseDbTools):
        return tools_or_dialect.build_system_prompt(SYSTEM_PROMPT)
    return SYSTEM_PROMPT + get_dialect_spec_for_name(tools_or_dialect).to_prompt()


def _detect_sql_dialect(db_url: str) -> str:
    return detect_sql_dialect(db_url)

def _normalize_plan(plan: Dict[str, Any]) -> str:
    return json.dumps(plan, sort_keys=True, separators=(",", ":"))


def _plan_hash(plan: Dict[str, Any]) -> str:
    return hashlib.sha256(_normalize_plan(plan).encode("utf-8")).hexdigest()


def _extract_plan_json(raw_content: str) -> Dict[str, Any]:
    content = raw_content.strip()

    try:
        return json.loads(content)
    except json.JSONDecodeError:
        pass

    fenced_match = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", content, re.DOTALL | re.IGNORECASE)
    if fenced_match:
        return json.loads(fenced_match.group(1))

    decoder = json.JSONDecoder()
    for index, char in enumerate(content):
        if char != "{":
            continue
        try:
            parsed, _end = decoder.raw_decode(content[index:])
            if isinstance(parsed, dict):
                return parsed
        except json.JSONDecodeError:
            continue

    raise json.JSONDecodeError("Could not extract JSON object from planner response.", content, 0)


def _collect_final_message(
    messages: List[Message],
    client: OpenRouterClient,
    tool_impl: Dict[str, Any],
    max_steps: int = 80,
) -> tuple[Message, int, int]:
    final_msg: Optional[Message] = None
    cache_hits = 0
    cache_misses = 0

    for step_index in range(max_steps):
        resp = client.chat(messages, TOOLS_SPEC)
        cache_info = resp.get("_cache", {})
        if cache_info.get("hit"):
            cache_hits += 1
        else:
            cache_misses += 1
        msg: Message = resp["choices"][0]["message"]

        if msg.get("reasoning"):
            console.print(
                Panel(
                    msg["reasoning"],
                    title=f"[bold blue]Thinking step {step_index + 1}[/bold blue]",
                    border_style="blue",
                )
            )

        messages.append(msg)

        tool_calls = msg.get("tool_calls") or []
        if tool_calls:
            for call in tool_calls:
                name = call["function"]["name"].strip()
                raw_args = call["function"].get("arguments") or "{}"
                try:
                    args = json.loads(raw_args) if isinstance(raw_args, str) else raw_args
                except json.JSONDecodeError:
                    args = [raw_args]
                try:
                    result = tool_impl[name](**args)
                    console.print(f"[bold green]Tool {name}[/bold green] called with: [cyan]{raw_args}[/cyan]")
                    console.print(f"Result: {result}")
                except Exception as e:
                    result = {"error": f"{type(e).__name__}: {e}"}
                    console.print(f"[bold red]Error calling tool {name}[/bold red] with {raw_args}: {e}")

                messages.append(
                    Message(
                        role="tool",
                        tool_call_id=call["id"],
                        name=name,
                        content=json.dumps(result),
                    )
                )
            continue

        final_msg = msg
        break

    if not final_msg or not final_msg.get("content"):
        raise RuntimeError("Agent did not produce a final plan JSON.")

    return final_msg, cache_hits, cache_misses


def _extract_final_plan(final_msg: Message) -> Dict[str, Any]:
    try:
        return _extract_plan_json(final_msg["content"] or "")
    except Exception as e:
        console.print(f"[bold red]Error extracting plan:[/bold red] {e} {final_msg.get('content')}")
        raise e


def _render_execution_failure_feedback(plan: Dict[str, Any], exc: Exception) -> str:
    details = str(exc).strip()
    return (
        "Your previous plan failed during execution and must be fully corrected. "
        "Return a complete replacement JSON plan, not a patch.\n\n"
        f"Execution failure:\n{details}\n\n"
        "Fix the root cause in the SQL plan. "
        "If the error is about duplicate keys, make the inserts idempotent or narrow the joins. "
        "If the error is about missing columns or tables, correct the SQL to match the actual schema. "
        "If the failure happened inside a multi-statement step, rewrite the full step safely.\n\n"
        f"Previous plan hash: {_plan_hash(plan)}"
    )


def _execute_plan(db_url: str, plan: Dict[str, Any], out_path: str) -> None:
    exec_tools = create_db_tools(db_url)

    console.rule("[bold magenta]Execution[/bold magenta]")
    console.print("[yellow]Setting up subset schema...[/yellow]")
    exec_tools.setup_subset_schema("subset", tables=plan.get("tables"))
    console.print("[yellow]Executing subset plan on database...[/yellow]")

    steps = plan.get("steps", [])
    for step_index, step in enumerate(steps, start=1):
        sql = step.get("sql", "").strip()
        if not sql:
            continue
        comment = step.get("comment", "").strip()
        console.print(f"Executing: [cyan]{sql}[/cyan]...")
        try:
            exec_tools.execute_sql(sql)
        except Exception as e:
            console.print(f"[bold red]Error executing step:[/bold red] {e}")
            step_label = f"step {step_index}"
            if comment:
                step_label += f" ({comment})"
            raise RuntimeError(f"{step_label} failed: {e}\nSQL:\n{sql}") from e

    console.print("[yellow]Cleaning up dangling references...[/yellow]")
    try:
        exec_tools.cleanup_dangling_references("subset")
    except Exception as e:
        raise RuntimeError(f"cleanup_dangling_references failed: {e}") from e

    console.print(f"[bold green]Dumping results to {out_path}...[/bold green]")
    try:
        exec_tools.dump_schema_data(schema="subset", output_path=out_path, tables=plan.get("tables"))
    except Exception as e:
        raise RuntimeError(f"dump_schema_data failed: {e}") from e

def run_agent_and_generate(
    db_url: str,
    api_key: str,
    model: str,
    target_rows: int,
    out_path: str,
    verify_ssl: bool = True,
    prompt_note: Optional[str] = None,
    cache_dir: Optional[str] = ".cache/openrouter",
    print_openrouter_stats: bool = True,
):
    tools = create_db_tools(db_url)
    dialect = tools.dialect_name()

    client = OpenRouterClient(api_key, model, verify=verify_ssl, cache_dir=cache_dir)

    user_prompt = (
        f"SQL dialect: {dialect}. "
        f"Target total rows (rough): {target_rows}. "
        f"Start by calling get_schema() and then get_stats for a few high-value tables."
    )
    if prompt_note:
        user_prompt += f" Additional benchmark / operator note: {prompt_note}"

    messages: List[Message] = [
        Message(role="system", content=_build_system_prompt(tools)),
        Message(role="user", content=user_prompt),
    ]

    tool_impl = {
        "get_schema": lambda **kwargs: tools.get_schema(),
        "get_stats": lambda table, **kwargs: tools.get_stats(table),
        "query_sql": lambda sql, max_rows=50, **kwargs: tools.query_sql(sql, max_rows=max_rows),
    }

    cache_hits = 0
    cache_misses = 0
    plan: Dict[str, Any] | None = None

    for attempt_index in range(MAX_EXECUTION_REPAIR_ATTEMPTS + 1):
        final_msg, hits, misses = _collect_final_message(messages, client, tool_impl)
        cache_hits += hits
        cache_misses += misses

        plan = _extract_final_plan(final_msg)
        console.print(
            Panel(Syntax(json.dumps(plan, indent=2), "json", theme="monokai"), title="[bold green]Final Plan[/bold green]")
        )

        try:
            _execute_plan(db_url, plan, out_path)
            break
        except Exception as e:
            if attempt_index >= MAX_EXECUTION_REPAIR_ATTEMPTS:
                raise
            console.print(f"[bold red]Plan execution failed; requesting repaired plan:[/bold red] {e}")
            messages.append(Message(role="user", content=_render_execution_failure_feedback(plan, e)))
    else:
        raise RuntimeError("Agent did not produce an executable plan.")

    assert plan is not None
    plan_hash = _plan_hash(plan)

    llm_stats_raw = client.get_stats()
    llm_stats = llm_stats_raw if isinstance(llm_stats_raw, dict) else {}
    if print_openrouter_stats:
        console.print(
            Panel(
                Syntax(json.dumps(llm_stats, indent=2, sort_keys=True), "json", theme="monokai"),
                title="[bold cyan]OpenRouter Stats[/bold cyan]",
            )
        )
    console.print("[bold green]Done.[/bold green]")
    return {
        "plan": plan,
        "plan_hash": plan_hash,
        "steps": len(plan.get("steps", [])),
        "tables": plan.get("tables", []),
        "cache_hits": cache_hits,
        "cache_misses": cache_misses,
        "messages": len(messages),
        "out_path": out_path,
        "prompt_note": prompt_note,
        "llm_stats": llm_stats,
    }
