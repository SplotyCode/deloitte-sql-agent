import json
from typing import List, Optional
from .client import OpenRouterClient, Message
from .db_tools import PgTools, SqliteTools, BaseDbTools
from rich.console import Console
from rich.panel import Panel
from rich.syntax import Syntax

console = Console()

TOOLS_SPEC = [
    {
        "type": "function",
        "function": {
            "name": "get_schema",
            "description": "Return database schema: tables, columns, primary keys, foreign keys, row estimates.",
            "parameters": {"type": "object", "properties": {}, "required": []},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_stats",
            "description": "Return column stats for a table (cheap summary).",
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
                    "max_rows": {"type": "integer", "minimum": 1, "maximum": 200},
                },
                "required": ["sql"],
            },
        },
    },
]

SYSTEM_PROMPT = """You are a database subsetting expert.
Goal: Produce a JSON plan that contains a sequence of SQL commands to select and insert a consistent subset of data into the target `subset` schema.

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

def run_agent_and_generate(db_url: str, api_key: str, model: str, target_rows: int, out_path: str, verify_ssl: bool = True):
    if db_url.startswith("postgres"):
        tools: BaseDbTools = PgTools(db_url)
    else:
        if db_url.startswith("sqlite://"):
             db_url = db_url.replace("sqlite://", "")
        tools: BaseDbTools = SqliteTools(db_url)
        
    client = OpenRouterClient(api_key, model, verify=verify_ssl)

    user_prompt = (
        f"Target total rows (rough): {target_rows}. "
        f"Start by calling get_schema() and then get_stats for a few high-value tables."
    )

    messages: List[Message] = [
        Message(role="system", content=SYSTEM_PROMPT),
        Message(role="user", content=user_prompt),
    ]

    tool_impl = {
        "get_schema": lambda **kwargs: tools.get_schema(),
        "get_stats": lambda table, **kwargs: tools.get_stats(table),
        "query_sql": lambda sql, max_rows=50, **kwargs: tools.query_sql(sql, max_rows=max_rows),
    }

    final_msg: Optional[Message] = None
    for _step in range(20):
        resp = client.chat(messages, TOOLS_SPEC)
        msg: Message = resp["choices"][0]["message"]

        if msg.get("reasoning"):
            console.print(Panel(msg["reasoning"], title=f"[bold blue]Thinking step {_step+1}[/bold blue]", border_style="blue"))

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

    plan_str = final_msg["content"].strip()
    if plan_str.startswith("```"):
        plan_str = plan_str.strip("`").strip()
        if plan_str.startswith("json"):
            plan_str = plan_str[4:].strip()

    plan = json.loads(plan_str)
    console.print(Panel(Syntax(json.dumps(plan, indent=2), "json", theme="monokai"), title="[bold green]Final Plan[/bold green]"))

    console.rule("[bold magenta]Execution[/bold magenta]")
    console.print("[yellow]Setting up subset schema...[/yellow]")
    tools.setup_subset_schema("subset", tables=plan.get("tables"))
    console.print("[yellow]Executing subset plan on database...[/yellow]")
    steps = plan.get("steps", [])
    for step in steps:
        sql = step.get("sql", "").strip()
        if not sql:
            continue
        console.print(f"Executing: [cyan]{sql}[/cyan]...")
        try:
            tools.execute_sql(sql)
        except Exception as e:
            console.print(f"[bold red]Error executing step:[/bold red] {e}")
            raise e

    console.print(f"[bold green]Dumping results to {out_path}...[/bold green]")
    tools.dump_schema_data(schema="subset", output_path=out_path, tables=plan.get("tables"))
        
    console.print("[bold green]Done.[/bold green]")

