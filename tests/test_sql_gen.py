from dump_reducer.sql_gen import build_subset_sql

def test_build_subset_sql_init():
    pass

def test_build_subset_sql_sqlite():
    schema_obj = {
        "engine": "sqlite"
    }

    plan = {
        "steps": [
            {
                "comment": "Create temp table",
                "sql": "CREATE TEMP TABLE _subset_ids (id INTEGER);"
            },
            {
                "comment": "Insert data",
                "sql": "INSERT INTO subset_users SELECT * FROM users WHERE id IN (SELECT id FROM _subset_ids);"
            }
        ]
    }

    sql = build_subset_sql(schema_obj, plan, subset_schema="subset")

    assert "PRAGMA foreign_keys = OFF;" in sql
    assert "BEGIN TRANSACTION;" in sql
    assert "-- Step 1: Create temp table" in sql
    assert "CREATE TEMP TABLE _subset_ids (id INTEGER);" in sql
    assert "-- Step 2: Insert data" in sql
    assert "INSERT INTO subset_users" in sql
    assert "COMMIT;" in sql
    assert "PRAGMA foreign_keys = ON;" in sql


def test_build_subset_sql_postgres():
    schema_obj = {
        "engine": "postgres"
    }

    plan = {
        "steps": [
            {
                "comment": "Truncate target",
                "sql": "TRUNCATE TABLE subset.users CASCADE;"
            },
            {
                "comment": "Load data",
                "sql": "INSERT INTO subset.users SELECT * FROM public.users LIMIT 10;"
            }
        ]
    }

    sql = build_subset_sql(schema_obj, plan, subset_schema="subset")

    assert "SET session_replication_role = 'replica';" in sql
    assert "CREATE SCHEMA IF NOT EXISTS \"subset\";" in sql
    assert "-- Step 1: Truncate target" in sql
    assert "TRUNCATE TABLE subset.users CASCADE;" in sql
    assert "COMMIT;" in sql


def test_build_subset_sql_empty_plan():
    schema_obj = {"engine": "postgres"}
    plan = {}
    
    sql = build_subset_sql(schema_obj, plan)
    
    assert "BEGIN;" in sql
    assert "COMMIT;" in sql
    assert "Step 1" not in sql
