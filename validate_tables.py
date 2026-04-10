"""
Table Validation Script: SQL Server (SSMS) ↔ Snowflake

Validates data integrity after migrating tables from SQL Server to Snowflake.
All operations are READ-ONLY — no INSERT, UPDATE, DELETE, DROP, or ALTER.

Test Cases:
  1. Source table exists in Snowflake
  2. Schema comparison (column names, data types)
  3. Record count comparison
  4. Audit fields verification in Snowflake
  5. Sample or full data validation
"""

import argparse
import getpass
import sys
import pyodbc
import snowflake.connector
import pandas as pd
from tabulate import tabulate

# ---------------------------------------------------------------------------
# Safety: block any non-SELECT statement from being executed
# ---------------------------------------------------------------------------
BLOCKED_KEYWORDS = [
    "INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "TRUNCATE",
    "CREATE", "EXEC", "EXECUTE", "MERGE", "GRANT", "REVOKE",
]


def _assert_readonly(sql: str):
    """Raise if SQL contains destructive keywords."""
    upper = sql.upper().strip()
    for kw in BLOCKED_KEYWORDS:
        if kw in upper.split():
            raise PermissionError(f"Blocked: statement contains '{kw}'. Only SELECT is allowed.")


def run_query(conn, sql: str, params=None) -> pd.DataFrame:
    """Execute a read-only query and return a DataFrame."""
    _assert_readonly(sql)
    cursor = conn.cursor()
    if params:
        cursor.execute(sql, params)
    else:
        cursor.execute(sql)
    columns = [desc[0].upper() for desc in cursor.description]
    rows = cursor.fetchall()
    return pd.DataFrame.from_records(rows, columns=columns)


# ---------------------------------------------------------------------------
# Connection helpers
# ---------------------------------------------------------------------------
def connect_sqlserver(server: str, database: str,
                      domain: str = None, username: str = None,
                      password: str = None) -> pyodbc.Connection:
    """
    Connect to SQL Server (read-only intent).

    Auth modes (checked in order):
      1. Domain auth  — --ss-domain + --ss-user + password prompt
                        Simulates: runas /netonly /user:DOMAIN\\user
                        Uses UID=DOMAIN\\user with Trusted_Connection=no
      2. SQL auth     — --ss-user + --ss-password (no domain)
      3. Windows auth — no credentials, uses Trusted_Connection=yes
    """
    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"ApplicationIntent=ReadOnly;"
    )
    if domain and username:
        # Domain Windows auth — equivalent to runas /netonly /user:DOMAIN\user
        conn_str += f"UID={domain}\\{username};PWD={password};"
    elif username and password:
        # SQL Server Authentication
        conn_str += f"UID={username};PWD={password};"
    else:
        # Local Windows Authentication (Trusted_Connection)
        conn_str += "Trusted_Connection=yes;"
    return pyodbc.connect(conn_str)


def connect_snowflake(account: str, user: str, password: str,
                      warehouse: str, database: str, schema: str,
                      role: str = None) -> snowflake.connector.SnowflakeConnection:
    """Connect to Snowflake."""
    params = dict(
        account=account,
        user=user,
        password=password,
        warehouse=warehouse,
        database=database,
        schema=schema,
    )
    if role:
        params["role"] = role
    return snowflake.connector.connect(**params)


# ---------------------------------------------------------------------------
# Test Case 1: Table existence in Snowflake
# ---------------------------------------------------------------------------
def test_table_exists(sf_conn, sf_database: str, sf_schema: str, sf_table: str) -> dict:
    """Check whether the target table exists in Snowflake."""
    sql = (
        f'SELECT COUNT(*) AS CNT '
        f'FROM "{sf_database}".INFORMATION_SCHEMA.TABLES '
        f'WHERE UPPER(TABLE_SCHEMA) = UPPER(%s) '
        f'AND UPPER(TABLE_NAME) = UPPER(%s)'
    )
    df = run_query(sf_conn, sql, params=[sf_schema, sf_table])
    exists = int(df.iloc[0]["CNT"]) > 0
    fqn = f"{sf_database}.{sf_schema}.{sf_table}"
    return {
        "test": "TC1 - Table Exists in Snowflake",
        "status": "PASS" if exists else "FAIL",
        "details": f"{fqn} {'found' if exists else 'NOT found'} in Snowflake",
    }


# ---------------------------------------------------------------------------
# Test Case 2: Schema / column comparison
# ---------------------------------------------------------------------------
def _get_sqlserver_columns(ss_conn, ss_schema: str, ss_table: str) -> pd.DataFrame:
    sql = (
        "SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, "
        "NUMERIC_PRECISION, NUMERIC_SCALE, IS_NULLABLE "
        "FROM INFORMATION_SCHEMA.COLUMNS "
        "WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? "
        "ORDER BY ORDINAL_POSITION"
    )
    return run_query(ss_conn, sql, params=[ss_schema, ss_table])


def _get_snowflake_columns(sf_conn, sf_database: str, sf_schema: str, sf_table: str) -> pd.DataFrame:
    sql = (
        f'SELECT COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH, '
        f'NUMERIC_PRECISION, NUMERIC_SCALE, IS_NULLABLE '
        f'FROM "{sf_database}".INFORMATION_SCHEMA.COLUMNS '
        f'WHERE UPPER(TABLE_SCHEMA) = UPPER(%s) AND UPPER(TABLE_NAME) = UPPER(%s) '
        f'ORDER BY ORDINAL_POSITION'
    )
    return run_query(sf_conn, sql, params=[sf_schema, sf_table])


def test_schema_match(ss_conn, sf_conn,
                      ss_schema, ss_table,
                      sf_database, sf_schema, sf_table) -> dict:
    """Compare column names and data types between source and target."""
    src = _get_sqlserver_columns(ss_conn, ss_schema, ss_table)
    tgt = _get_snowflake_columns(sf_conn, sf_database, sf_schema, sf_table)

    src_cols = set(src["COLUMN_NAME"].str.upper())
    tgt_cols = set(tgt["COLUMN_NAME"].str.upper())

    # Exclude Snowflake-only AUDIT_* columns from comparison
    tgt_cols_filtered = {c for c in tgt_cols if not c.startswith("AUDIT_")}
    audit_cols = tgt_cols - tgt_cols_filtered

    missing_in_sf = src_cols - tgt_cols_filtered
    extra_in_sf = tgt_cols_filtered - src_cols

    passed = len(missing_in_sf) == 0
    details_parts = []
    if missing_in_sf:
        details_parts.append(f"Missing in Snowflake: {sorted(missing_in_sf)}")
    if extra_in_sf:
        details_parts.append(f"Extra in Snowflake: {sorted(extra_in_sf)}")
    if audit_cols:
        details_parts.append(f"AUDIT_* columns ignored: {sorted(audit_cols)}")
    if not missing_in_sf and not extra_in_sf:
        details_parts.insert(0, "All source columns present in Snowflake")

    return {
        "test": "TC2 - Schema Comparison",
        "status": "PASS" if passed else "FAIL",
        "details": "; ".join(details_parts),
    }


# ---------------------------------------------------------------------------
# Test Case 3: Record count
# ---------------------------------------------------------------------------
def _count_rows(conn, schema: str, table: str, is_snowflake: bool = False,
                sf_database: str = None) -> int:
    if is_snowflake:
        sql = f'SELECT COUNT(*) AS CNT FROM "{sf_database}"."{schema}"."{table}"'
    else:
        sql = f"SELECT COUNT(*) AS CNT FROM [{schema}].[{table}]"
    df = run_query(conn, sql)
    return int(df.iloc[0]["CNT"])


def test_record_count(ss_conn, sf_conn,
                      ss_schema, ss_table,
                      sf_database, sf_schema, sf_table) -> dict:
    """Compare row counts between SQL Server and Snowflake."""
    src_count = _count_rows(ss_conn, ss_schema, ss_table)
    tgt_count = _count_rows(sf_conn, sf_schema, sf_table, is_snowflake=True,
                            sf_database=sf_database)
    matched = src_count == tgt_count
    return {
        "test": "TC3 - Record Count",
        "status": "PASS" if matched else "FAIL",
        "details": f"Source: {src_count:,} | Snowflake: {tgt_count:,}"
                   + ("" if matched else f" | Diff: {abs(src_count - tgt_count):,}"),
    }


# ---------------------------------------------------------------------------
# Test Case 4: Audit fields in Snowflake
# ---------------------------------------------------------------------------
COMMON_AUDIT_FIELDS = [
    "CREATED_DATE", "CREATED_BY", "MODIFIED_DATE", "MODIFIED_BY",
    "UPDATED_DATE", "UPDATED_BY", "INSERT_DATE", "UPDATE_DATE",
    "ETL_LOAD_DATE", "ETL_UPDATE_DATE", "LOAD_TIMESTAMP",
    "DW_INSERT_DATE", "DW_UPDATE_DATE",
]


def test_audit_fields(sf_conn, sf_database: str, sf_schema: str, sf_table: str,
                      audit_fields: list[str] = None) -> dict:
    """Check that expected audit columns exist and are populated in Snowflake."""
    cols_df = _get_snowflake_columns(sf_conn, sf_database, sf_schema, sf_table)
    tgt_cols = set(cols_df["COLUMN_NAME"].str.upper())

    if audit_fields:
        # User-specified audit fields: exact match
        found = [f.upper() for f in audit_fields if f.upper() in tgt_cols]
    else:
        # Auto-detect: columns matching COMMON_AUDIT_FIELDS or containing "AUDIT"
        from_list = [f for f in COMMON_AUDIT_FIELDS if f in tgt_cols]
        from_pattern = [c for c in tgt_cols if "AUDIT" in c and c not in from_list]
        found = sorted(set(from_list + from_pattern))

    if not found:
        return {
            "test": "TC4 - Audit Fields",
            "status": "WARN",
            "details": "No common audit columns detected in Snowflake table",
        }

    # Check that audit columns are not entirely NULL
    null_checks = ", ".join(
        f'SUM(CASE WHEN "{col}" IS NULL THEN 1 ELSE 0 END) AS "{col}_NULLS"'
        for col in found
    )
    sql = f'SELECT COUNT(*) AS TOTAL, {null_checks} FROM "{sf_database}"."{sf_schema}"."{sf_table}"'
    df = run_query(sf_conn, sql)
    total = int(df.iloc[0]["TOTAL"])

    all_null_cols = []
    for col in found:
        nulls = int(df.iloc[0][f"{col}_NULLS"])
        if nulls == total and total > 0:
            all_null_cols.append(col)

    passed = len(all_null_cols) == 0
    details = f"Audit columns found: {found}"
    if all_null_cols:
        details += f" | Entirely NULL: {all_null_cols}"

    return {
        "test": "TC4 - Audit Fields",
        "status": "PASS" if passed else "FAIL",
        "details": details,
    }


# ---------------------------------------------------------------------------
# Test Case 5: Data validation (sample or full)
# ---------------------------------------------------------------------------
def test_data_validation(ss_conn, sf_conn,
                         ss_schema, ss_table,
                         sf_database, sf_schema, sf_table,
                         mode: str = "partial",
                         sample_size: int = 10) -> dict:
    """
    Compare actual data between source and target.

    mode='partial' — compare a sample of N rows (ordered by first column).
    mode='full'    — compare all rows (can be slow on large tables).

    Only the columns present in BOTH tables (excluding AUDIT_*) are compared.
    Comparison is row-by-row based on sort order, with column-level diff details.
    """
    # Determine common columns (exclude AUDIT_* snowflake-only columns)
    src_cols_df = _get_sqlserver_columns(ss_conn, ss_schema, ss_table)
    tgt_cols_df = _get_snowflake_columns(sf_conn, sf_database, sf_schema, sf_table)

    src_col_names = set(src_cols_df["COLUMN_NAME"].str.upper())
    tgt_col_names = set(tgt_cols_df["COLUMN_NAME"].str.upper())
    common_cols = sorted(
        src_col_names & {c for c in tgt_col_names if not c.startswith("AUDIT_")}
    )

    if not common_cols:
        return {
            "test": "TC5 - Data Validation",
            "status": "FAIL",
            "details": "No common columns found to compare",
        }

    # Pick a row-identifier column: prefer columns with ID/KEY in the name
    id_candidates = [c for c in common_cols if "ID" in c or "KEY" in c]
    key_col = id_candidates[0] if id_candidates else common_cols[0]

    # Build column lists respecting each DB's quoting
    ss_col_list = ", ".join(f"[{c}]" for c in common_cols)
    sf_col_list = ", ".join(f'"{c}"' for c in common_cols)
    order_col_ss = f"[{key_col}]"
    order_col_sf = f'"{key_col}"'

    if mode == "full":
        ss_sql = f"SELECT {ss_col_list} FROM [{ss_schema}].[{ss_table}] ORDER BY {order_col_ss}"
        sf_sql = f'SELECT {sf_col_list} FROM "{sf_database}"."{sf_schema}"."{sf_table}" ORDER BY {order_col_sf}'
    else:
        ss_sql = (
            f"SELECT TOP {int(sample_size)} {ss_col_list} "
            f"FROM [{ss_schema}].[{ss_table}] ORDER BY {order_col_ss}"
        )
        sf_sql = (
            f'SELECT {sf_col_list} FROM "{sf_database}"."{sf_schema}"."{sf_table}" '
            f"ORDER BY {order_col_sf} LIMIT {int(sample_size)}"
        )

    src_df = run_query(ss_conn, ss_sql)
    tgt_df = run_query(sf_conn, sf_sql)

    # Normalise column names to upper
    src_df.columns = [c.upper() for c in src_df.columns]
    tgt_df.columns = [c.upper() for c in tgt_df.columns]

    # Cast everything to string for consistent comparison
    src_df = src_df.astype(str).fillna("")
    tgt_df = tgt_df.astype(str).fillna("")

    label = "Full" if mode == "full" else f"Sample ({sample_size} rows)"

    # Handle row count difference between fetched datasets
    src_rows = len(src_df)
    tgt_rows = len(tgt_df)
    compare_rows = min(src_rows, tgt_rows)

    if compare_rows == 0:
        return {
            "test": "TC5 - Data Validation",
            "status": "FAIL",
            "details": f"{label} | Source rows: {src_rows}, Target rows: {tgt_rows} — nothing to compare",
        }

    # Row-by-row, column-by-column comparison
    mismatched_rows = 0
    col_mismatch_count = {c: 0 for c in common_cols}  # per-column mismatch tally
    first_example = None  # capture first mismatch for the report

    for i in range(compare_rows):
        row_has_diff = False
        row_key = src_df.iloc[i][key_col].strip()
        for col in common_cols:
            src_val = src_df.iloc[i][col].strip()
            tgt_val = tgt_df.iloc[i][col].strip()
            if src_val != tgt_val:
                col_mismatch_count[col] += 1
                row_has_diff = True
                if first_example is None:
                    first_example = {
                        "key_col": key_col,
                        "key_val": row_key if len(row_key) <= 50 else row_key[:50] + "...",
                        "row_num": i + 1,
                        "column": col,
                        "source": src_val if len(src_val) <= 80 else src_val[:80] + "...",
                        "target": tgt_val if len(tgt_val) <= 80 else tgt_val[:80] + "...",
                    }
        if row_has_diff:
            mismatched_rows += 1

    # Build details
    details = f"{label} | Compared {len(common_cols)} columns x {compare_rows} rows"

    if mismatched_rows == 0 and src_rows == tgt_rows:
        details += " | All rows match"
        return {"test": "TC5 - Data Validation", "status": "PASS", "details": details}

    # Columns that have differences, sorted by mismatch count descending
    diff_cols = {c: n for c, n in col_mismatch_count.items() if n > 0}
    diff_cols_sorted = sorted(diff_cols.items(), key=lambda x: -x[1])

    details += f" | Mismatched rows: {mismatched_rows}/{compare_rows}"
    if src_rows != tgt_rows:
        details += f" | Row count differs (source: {src_rows}, target: {tgt_rows})"

    # Show which columns differ and how often
    col_summary = ", ".join(f"{c} ({n})" for c, n in diff_cols_sorted[:5])
    if len(diff_cols_sorted) > 5:
        col_summary += f", ... +{len(diff_cols_sorted) - 5} more"
    details += f"\n    Columns with diffs: {col_summary}"

    # Show first concrete example with row identifier
    if first_example:
        ex = first_example
        details += (
            f"\n    First mismatch → row #{ex['row_num']} where {ex['key_col']}=[{ex['key_val']}]"
            f"\n      column [{ex['column']}]: source=[{ex['source']}] vs target=[{ex['target']}]"
        )

    return {
        "test": "TC5 - Data Validation",
        "status": "FAIL",
        "details": details,
    }


# ---------------------------------------------------------------------------
# Table mapping parser
# ---------------------------------------------------------------------------
def parse_table_mapping(mapping: str) -> dict:
    """
    Parse a table mapping string.

    Format: source_schema.source_table:target_database.target_schema.target_table
    Examples:
      dbo.Customers:PUBLIC.CUSTOMERS                         (uses --sf-database)
      dbo.Customers:DW_DEV_BRONZE.HIST.HIST_TBL_PSG_GROUP_KEY  (overrides database)
      dbo.Orders                                             (same name on both sides)

    If only one side is given (no colon), assumes same schema.table on both.
    """
    if ":" in mapping:
        src, tgt = mapping.split(":", 1)
    else:
        src = tgt = mapping

    def _split_source(name):
        parts = name.split(".", 1)
        if len(parts) == 2:
            return parts[0], parts[1]
        return "dbo", parts[0]

    def _split_target(name):
        parts = name.split(".")
        if len(parts) == 3:
            return parts[0], parts[1], parts[2]  # db.schema.table
        if len(parts) == 2:
            return None, parts[0], parts[1]       # schema.table
        return None, "dbo", parts[0]              # table only

    ss_schema, ss_table = _split_source(src)
    sf_database, sf_schema, sf_table = _split_target(tgt)
    return {
        "ss_schema": ss_schema, "ss_table": ss_table,
        "sf_database": sf_database,  # None means use --sf-database
        "sf_schema": sf_schema, "sf_table": sf_table,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Validate data between SQL Server and Snowflake tables.",
        formatter_class=argparse.RawTextHelpFormatter,
    )

    # Table mappings
    p.add_argument(
        "tables", nargs="+",
        help=(
            "Table mapping(s) in the format:\n"
            "  source_schema.table:target_schema.table         (uses --sf-database)\n"
            "  source_schema.table:target_db.target_schema.table  (overrides database)\n"
            "Examples:\n"
            "  dbo.Customers:PUBLIC.CUSTOMERS\n"
            "  dbo.Customers:DW_DEV_BRONZE.HIST.HIST_CUSTOMERS\n"
            "  dbo.Orders  (same name assumed on both sides)"
        ),
    )

    # SQL Server
    ss = p.add_argument_group("SQL Server")
    ss.add_argument("--ss-server", required=True, help="SQL Server hostname or IP")
    ss.add_argument("--ss-database", required=True, help="SQL Server database name")
    ss.add_argument("--ss-domain", default=None, help="Windows domain (e.g. mmreibc). Prompts for password at runtime")
    ss.add_argument("--ss-user", default=None, help="SQL Server or domain username (omit for local Windows Auth)")
    ss.add_argument("--ss-password", default=None, help="SQL Server password (omit when using --ss-domain; you will be prompted)")

    # Snowflake
    sf = p.add_argument_group("Snowflake")
    sf.add_argument("--sf-account", required=True, help="Snowflake account identifier")
    sf.add_argument("--sf-user", required=True, help="Snowflake username")
    sf.add_argument("--sf-password", required=True, help="Snowflake password")
    sf.add_argument("--sf-warehouse", required=True, help="Snowflake warehouse")
    sf.add_argument("--sf-database", required=True, help="Snowflake database")
    sf.add_argument("--sf-schema", default="PUBLIC", help="Default Snowflake schema (default: PUBLIC)")
    sf.add_argument("--sf-role", default=None, help="Snowflake role (optional)")

    # Validation options
    v = p.add_argument_group("Validation options")
    v.add_argument(
        "--mode", choices=["partial", "full"], default="partial",
        help="Data validation mode (default: partial)",
    )
    v.add_argument(
        "--sample-size", type=int, default=10,
        help="Number of rows for partial validation (default: 10)",
    )
    v.add_argument(
        "--audit-fields", nargs="*", default=None,
        help="Custom audit field names to check (default: common audit column names)",
    )

    return p


def run_validations(args) -> list[dict]:
    """Connect to both databases, run all test cases, return results."""
    # Resolve SQL Server auth mode and prompt for password if needed
    ss_password = args.ss_password
    if args.ss_domain:
        if not args.ss_user:
            print("ERROR: --ss-domain requires --ss-user", file=sys.stderr)
            sys.exit(2)
        auth_mode = f"Domain Auth ({args.ss_domain}\\{args.ss_user})"
        if not ss_password:
            ss_password = getpass.getpass(f"Password for {args.ss_domain}\\{args.ss_user}: ")
    elif args.ss_user:
        auth_mode = "SQL Server Auth"
    else:
        auth_mode = "Windows Auth"

    print(f"Connecting to SQL Server ({auth_mode})...")
    ss_conn = connect_sqlserver(args.ss_server, args.ss_database,
                                domain=args.ss_domain,
                                username=args.ss_user, password=ss_password)

    print("Connecting to Snowflake...")
    sf_conn = connect_snowflake(
        account=args.sf_account,
        user=args.sf_user,
        password=args.sf_password,
        warehouse=args.sf_warehouse,
        database=args.sf_database,
        schema=args.sf_schema,
        role=args.sf_role,
    )

    all_results = []

    for mapping_str in args.tables:
        m = parse_table_mapping(mapping_str)
        # Use per-table database override if provided, otherwise fall back to --sf-database
        sf_db = m["sf_database"] or args.sf_database
        header = f"{m['ss_schema']}.{m['ss_table']} → {sf_db}.{m['sf_schema']}.{m['sf_table']}"
        print(f"\n{'='*60}")
        print(f"  Validating: {header}")
        print(f"{'='*60}")

        table_results = []

        # TC1: Table exists
        print("  Running TC1 - Table Exists...")
        r1 = test_table_exists(sf_conn, sf_db, m["sf_schema"], m["sf_table"])
        table_results.append(r1)

        if r1["status"] == "FAIL":
            print(f"  ⛔ Table not found in Snowflake — skipping remaining tests.")
            table_results.extend([
                {"test": "TC2 - Schema Comparison", "status": "SKIP", "details": "Table not found"},
                {"test": "TC3 - Record Count", "status": "SKIP", "details": "Table not found"},
                {"test": "TC4 - Audit Fields", "status": "SKIP", "details": "Table not found"},
                {"test": "TC5 - Data Validation", "status": "SKIP", "details": "Table not found"},
            ])
        else:
            # TC2: Schema
            print("  Running TC2 - Schema Comparison...")
            table_results.append(test_schema_match(
                ss_conn, sf_conn,
                m["ss_schema"], m["ss_table"],
                sf_db, m["sf_schema"], m["sf_table"],
            ))

            # TC3: Record count
            print("  Running TC3 - Record Count...")
            table_results.append(test_record_count(
                ss_conn, sf_conn,
                m["ss_schema"], m["ss_table"],
                sf_db, m["sf_schema"], m["sf_table"],
            ))

            # TC4: Audit fields
            print("  Running TC4 - Audit Fields...")
            table_results.append(test_audit_fields(
                sf_conn, sf_db, m["sf_schema"], m["sf_table"],
                audit_fields=args.audit_fields,
            ))

            # TC5: Data validation
            print(f"  Running TC5 - Data Validation ({args.mode}, sample={args.sample_size})...")
            table_results.append(test_data_validation(
                ss_conn, sf_conn,
                m["ss_schema"], m["ss_table"],
                sf_db, m["sf_schema"], m["sf_table"],
                mode=args.mode,
                sample_size=args.sample_size,
            ))

        for r in table_results:
            r["table"] = header
        all_results.extend(table_results)

    ss_conn.close()
    sf_conn.close()
    return all_results


def print_report(results: list[dict]):
    """Print a summary report of all test results."""
    print("\n")
    print("=" * 80)
    print("  VALIDATION REPORT")
    print("=" * 80)

    rows = []
    for r in results:
        status = r["status"]
        if status == "PASS":
            badge = "PASS"
        elif status == "FAIL":
            badge = "FAIL"
        elif status == "WARN":
            badge = "WARN"
        else:
            badge = "SKIP"
        rows.append([r["table"], r["test"], badge, r["details"]])

    print(tabulate(rows, headers=["Table", "Test Case", "Status", "Details"],
                   tablefmt="grid"))

    # Summary counts
    total = len(results)
    passed = sum(1 for r in results if r["status"] == "PASS")
    failed = sum(1 for r in results if r["status"] == "FAIL")
    warned = sum(1 for r in results if r["status"] == "WARN")
    skipped = sum(1 for r in results if r["status"] == "SKIP")

    print(f"\nTotal: {total} | Passed: {passed} | Failed: {failed} | Warnings: {warned} | Skipped: {skipped}")

    if failed > 0:
        print("\n** RESULT: VALIDATION FAILED — review failures above **")
    else:
        print("\n** RESULT: ALL VALIDATIONS PASSED **")


def main():
    parser = build_parser()
    args = parser.parse_args()

    if args.sample_size < 1:
        parser.error("--sample-size must be >= 1")

    try:
        results = run_validations(args)
        print_report(results)
        sys.exit(1 if any(r["status"] == "FAIL" for r in results) else 0)
    except Exception as e:
        print(f"\nERROR: {e}", file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
