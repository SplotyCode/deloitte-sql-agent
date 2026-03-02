import sqlite3

from generate_sample_dump import SampleDumpConfig, generate_sample_dump


def _table_count(db_path):
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("SELECT count(*) FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
        return cur.fetchone()[0]
    finally:
        conn.close()


def _scalar(db_path, sql):
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(sql)
        return cur.fetchone()[0]
    finally:
        conn.close()


def test_generate_sample_dump_supports_large_table_counts_and_hidden_refs(tmp_path):
    db_path = tmp_path / "fixture.db"

    generate_sample_dump(
        db_filename=str(db_path),
        filename=str(tmp_path / "fixture.sql"),
        config=SampleDumpConfig(
            scale="tiny",
            scenario="support",
            min_tables=92,
            hidden_reference_mode="aggressive",
            seed=11,
            write_sql_dump=False,
        ),
    )

    assert _table_count(str(db_path)) == 92
    assert _scalar(str(db_path), "SELECT count(*) FROM relationship_hints") > 0
    assert _scalar(str(db_path), "SELECT count(*) FROM ml_training_examples WHERE hidden_refs_json IS NOT NULL") > 0
    assert _scalar(str(db_path), "SELECT count(*) FROM tickets WHERE shadow_order_id IS NOT NULL") > 0
    assert _scalar(str(db_path), "SELECT count(*) FROM challenge_satellite_01") > 0
    assert _scalar(str(db_path), "SELECT count(*) FROM scenario_metadata WHERE key = 'scenario' AND value = 'support'") == 1


def test_generate_sample_dump_can_disable_hidden_reference_population(tmp_path):
    db_path = tmp_path / "fixture_off.db"

    generate_sample_dump(
        db_filename=str(db_path),
        filename=str(tmp_path / "fixture_off.sql"),
        config=SampleDumpConfig(
            scale="tiny",
            scenario="enterprise",
            min_tables=89,
            hidden_reference_mode="off",
            seed=13,
            write_sql_dump=False,
        ),
    )

    assert _table_count(str(db_path)) == 89
    assert _scalar(str(db_path), "SELECT count(*) FROM relationship_hints") == 0
    assert _scalar(str(db_path), "SELECT count(*) FROM ml_training_examples WHERE hidden_refs_json IS NOT NULL") == 0
    assert _scalar(str(db_path), "SELECT count(*) FROM tickets WHERE shadow_order_id IS NOT NULL") == 0
    assert _scalar(str(db_path), "SELECT count(*) FROM tasks WHERE shadow_order_id IS NOT NULL") == 0
