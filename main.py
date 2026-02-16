import argparse
import os
import sys
from dump_reducer.planner import run_agent_and_generate

def main():
    ap = argparse.ArgumentParser(description="LLM-assisted DB subsetting planner (PostgreSQL/SQLite + OpenRouter tools).")
    ap.add_argument("--db-url", required=True, help="Database connection string or path. PostgreSQL: postgresql://user:pass@host:5432/db, SQLite: path/to/db.sqlite")
    ap.add_argument("--target-rows", type=int, default=1_000, help="Rough total row budget across tables.")
    ap.add_argument("--out", default="subset.sql", help="Output SQL file to create/populate subset schema.")
    ap.add_argument("--model", default="moonshotai/kimi-k2-thinking", help="OpenRouter model id (must support tool calling).")
    ap.add_argument("--api-key", default=os.getenv("OPENROUTER_API_KEY", ""), help="OpenRouter API key (or env OPENROUTER_API_KEY).")
    ap.add_argument("--no-verify-ssl", action="store_false", dest="verify_ssl", help="Disable SSL certificate verification for OpenRouter requests.")
    args = ap.parse_args()

    if not args.verify_ssl:
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    if not args.api_key:
        print("Missing OpenRouter API key. Set OPENROUTER_API_KEY or pass --api-key.", file=sys.stderr)
        sys.exit(2)

    run_agent_and_generate(
        db_url=args.db_url,
        api_key=args.api_key,
        model=args.model,
        target_rows=args.target_rows,
        out_path=args.out,
        verify_ssl=args.verify_ssl,
    )

if __name__ == "__main__":
    main()
