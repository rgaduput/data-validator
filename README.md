# Table Validator — SQL Server ↔ Snowflake

Validates data integrity after migrating tables from SQL Server (SSMS) to Snowflake. All operations are **read-only** — no destructive commands are executed.

## Test Cases

| # | Test | Description |
|---|------|-------------|
| TC1 | Table Exists | Checks if the target table exists in Snowflake |
| TC2 | Schema Comparison | Compares column names between source and target, flags missing/extra columns |
| TC3 | Record Count | Compares row counts on both sides |
| TC4 | Audit Fields | Checks that audit columns exist and are populated in Snowflake |
| TC5 | Data Validation | Compares actual row data — partial (sample) or full |

## Prerequisites

- Python 3.10+
- ODBC Driver 17 for SQL Server
- Network access to both SQL Server and Snowflake

## Installation

```bash
pip install -r requirements.txt
```

## SQL Server Authentication

The script supports two authentication modes for SQL Server:

- **Windows Auth** (default) — omit `--ss-user` and `--ss-password`. Uses `Trusted_Connection=yes`. Requires the machine to be domain-joined.
- **SQL Server Auth** — provide `--ss-user` and `--ss-password`. Use this when running from a non-domain machine (e.g. WSL, VMs, or cross-network).

## Usage

### Basic — single table with Windows Auth

```bash
python validate_tables.py dbo.Customers \
  --ss-server "SQLSERVER01" \
  --ss-database "SalesDB" \
  --sf-account "org-account" \
  --sf-user "user" \
  --sf-password "pass" \
  --sf-warehouse "WH" \
  --sf-database "SALES_DB" \
  --sf-schema "PUBLIC"
```

### Basic — single table with SQL Server Auth

```bash
python validate_tables.py dbo.Customers \
  --ss-server "SQLSERVER01" \
  --ss-database "SalesDB" \
  --ss-user "sql_username" \
  --ss-password "sql_password" \
  --sf-account "org-account" \
  --sf-user "user" \
  --sf-password "pass" \
  --sf-warehouse "WH" \
  --sf-database "SALES_DB" \
  --sf-schema "PUBLIC"
```

### Different names between source and target

Use `:` to map source → target when schemas or table names differ:

```bash
python validate_tables.py "dbo.Customers:RAW.CUSTOMERS" \
  --ss-server "SQLSERVER01" \
  --ss-database "SalesDB" \
  --ss-user "sql_username" \
  --ss-password "sql_password" \
  --sf-account "org-account" \
  --sf-user "user" \
  --sf-password "pass" \
  --sf-warehouse "WH" \
  --sf-database "SALES_DB" \
  --sf-schema "RAW"
```

### Multiple tables

Pass multiple mappings as positional arguments:

```bash
python validate_tables.py \
  "dbo.Customers:RAW.CUSTOMERS" \
  "dbo.Orders:RAW.ORDERS" \
  "dbo.Products:RAW.PRODUCTS" \
  --ss-server "SQLSERVER01" \
  --ss-database "SalesDB" \
  --ss-user "sql_username" \
  --ss-password "sql_password" \
  --sf-account "org-account" \
  --sf-user "user" \
  --sf-password "pass" \
  --sf-warehouse "WH" \
  --sf-database "SALES_DB" \
  --sf-schema "RAW"
```

### Full data validation

```bash
python validate_tables.py dbo.Customers:PUBLIC.CUSTOMERS \
  --mode full \
  --ss-server "SQLSERVER01" \
  --ss-database "SalesDB" \
  --ss-user "sql_username" \
  --ss-password "sql_password" \
  --sf-account "org-account" \
  --sf-user "user" \
  --sf-password "pass" \
  --sf-warehouse "WH" \
  --sf-database "SALES_DB"
```

### Partial validation with custom sample size

```bash
python validate_tables.py dbo.Customers:PUBLIC.CUSTOMERS \
  --mode partial \
  --sample-size 100 \
  --ss-server "SQLSERVER01" \
  --ss-database "SalesDB" \
  --ss-user "sql_username" \
  --ss-password "sql_password" \
  --sf-account "org-account" \
  --sf-user "user" \
  --sf-password "pass" \
  --sf-warehouse "WH" \
  --sf-database "SALES_DB"
```

### Custom audit fields

By default the script checks for common audit column names (e.g. `CREATED_DATE`, `ETL_LOAD_DATE`). To specify your own:

```bash
python validate_tables.py dbo.Customers:PUBLIC.CUSTOMERS \
  --audit-fields ETL_LOAD_DATE ETL_UPDATE_DATE CREATED_BY \
  --ss-server "SQLSERVER01" \
  --ss-database "SalesDB" \
  --ss-user "sql_username" \
  --ss-password "sql_password" \
  --sf-account "org-account" \
  --sf-user "user" \
  --sf-password "pass" \
  --sf-warehouse "WH" \
  --sf-database "SALES_DB"
```

## All Parameters

| Parameter | Required | Default | Description |
|-----------|----------|---------|-------------|
| `tables` (positional) | Yes | — | One or more table mappings: `source_schema.table:target_schema.table` |
| `--ss-server` | Yes | — | SQL Server hostname or IP |
| `--ss-database` | Yes | — | SQL Server database name |
| `--ss-user` | No | — | SQL Server username (omit for Windows Auth) |
| `--ss-password` | No | — | SQL Server password (omit for Windows Auth) |
| `--sf-account` | Yes | — | Snowflake account identifier |
| `--sf-user` | Yes | — | Snowflake username |
| `--sf-password` | Yes | — | Snowflake password |
| `--sf-warehouse` | Yes | — | Snowflake warehouse |
| `--sf-database` | Yes | — | Snowflake database |
| `--sf-schema` | No | `PUBLIC` | Default Snowflake schema |
| `--sf-role` | No | — | Snowflake role |
| `--mode` | No | `partial` | `partial` (sample rows) or `full` (all rows) |
| `--sample-size` | No | `10` | Number of rows to compare in partial mode |
| `--audit-fields` | No | common names | Space-separated list of audit column names to check |

## Output

The script prints a grid report at the end:

```
================================================================================
  VALIDATION REPORT
================================================================================
+------------------------------------+-------------------------+----------+----------------------------------+
| Table                              | Test Case               | Status   | Details                          |
+====================================+=========================+==========+==================================+
| dbo.Customers → PUBLIC.CUSTOMERS   | TC1 - Table Exists      | PASS     | PUBLIC.CUSTOMERS found           |
+------------------------------------+-------------------------+----------+----------------------------------+
| dbo.Customers → PUBLIC.CUSTOMERS   | TC2 - Schema Comparison | PASS     | All source columns present       |
+------------------------------------+-------------------------+----------+----------------------------------+
| dbo.Customers → PUBLIC.CUSTOMERS   | TC3 - Record Count      | PASS     | Source: 1,000 | Snowflake: 1,000 |
+------------------------------------+-------------------------+----------+----------------------------------+
| dbo.Customers → PUBLIC.CUSTOMERS   | TC4 - Audit Fields      | PASS     | Audit columns found: [...]       |
+------------------------------------+-------------------------+----------+----------------------------------+
| dbo.Customers → PUBLIC.CUSTOMERS   | TC5 - Data Validation   | PASS     | Sample (10 rows) | All rows match|
+------------------------------------+-------------------------+----------+----------------------------------+

Total: 5 | Passed: 5 | Failed: 0 | Warnings: 0 | Skipped: 0

** RESULT: ALL VALIDATIONS PASSED **
```

**Exit codes:** `0` = all passed, `1` = one or more failures, `2` = runtime error.

## Safety

- Every SQL statement is validated against a blocklist of destructive keywords (`INSERT`, `UPDATE`, `DELETE`, `DROP`, `ALTER`, etc.) before execution.
- SQL Server connection uses `ApplicationIntent=ReadOnly`.
- No credentials are hardcoded — all are passed as command-line arguments.
