# Genie Code Migration Demo

**Convert Oracle, Synapse, and SQL Server code to Databricks — live, inside the notebook editor.**

This repo contains a complete, ready-to-run demo showing how Databricks Genie Code (Databricks One) can interactively convert legacy SQL/stored procedure code to Databricks SQL or PySpark.

---

## What's Inside

```
├── notebooks/
│   ├── 00_Demo_Playbook.py        # Master playbook with demo flow and prompt cheat sheet
│   ├── 01_Oracle_Migration.py     # Oracle SQL + PL/SQL source code (6 patterns)
│   ├── 02_Synapse_Migration.py    # Azure Synapse T-SQL source code (5 patterns)
│   └── 03_SQLServer_Migration.py  # SQL Server T-SQL source code (5 patterns)
├── skills/
│   ├── oracle-migration/SKILL.md     # Genie Code skill: Oracle conversion rules
│   ├── synapse-migration/SKILL.md    # Genie Code skill: Synapse conversion rules
│   └── sqlserver-migration/SKILL.md  # Genie Code skill: SQL Server conversion rules
└── README.md                      # This file (playbook)
```

## Source Code Patterns Covered

### Oracle (`01_Oracle_Migration`)

| # | Pattern | Key Conversions |
|---|---------|-----------------|
| 1 | DDL with Oracle types | `VARCHAR2` → `STRING`, `NUMBER` → `DECIMAL`, `CLOB` → `STRING`, remove `TABLESPACE`, sequences → identity columns |
| 2 | PL/SQL Stored Procedure (payroll) | Cursors → set-based SQL, `NVL()` → `COALESCE()`, `DBMS_OUTPUT` → remove, exception handling → SQL Scripting handlers |
| 3 | MERGE / SCD Type 2 | `MERGE INTO` maps almost directly, `NVL()` → `COALESCE()`, `SYSTIMESTAMP` → `CURRENT_TIMESTAMP()`, sequences → `UUID()` |
| 4 | Analytical queries | `DECODE()` → `CASE WHEN`, `LISTAGG()` → `ARRAY_JOIN(COLLECT_LIST())`, `NVL2()` → `IF()`, `RATIO_TO_REPORT()` → manual division, `ROWNUM` → `ROW_NUMBER()` |
| 5 | Hierarchical query (org chart) | `CONNECT BY` / `START WITH` → recursive CTE, `SYS_CONNECT_BY_PATH` → concatenation, `CONNECT_BY_ISLEAF` → existence check |
| 6 | PL/SQL Package (bulk ETL) | `FORALL` / `BULK COLLECT` → `INSERT INTO ... SELECT`, packages → separate procedures, associative arrays → DataFrames |

### Synapse (`02_Synapse_Migration`)

| # | Pattern | Key Conversions |
|---|---------|-----------------|
| 1 | DDL with distribution/columnstore | Remove `DISTRIBUTION`, `CLUSTERED COLUMNSTORE INDEX`; `NVARCHAR(MAX)` → `STRING`, `MONEY` → `DECIMAL(19,4)`, `BIT` → `BOOLEAN`, `IDENTITY` → `GENERATED ALWAYS AS IDENTITY` |
| 2 | CTAS (Create Table As Select) | Remove `WITH (DISTRIBUTION = ...)` → `CREATE OR REPLACE TABLE ... AS SELECT`; `IIF()` → `IF()`, `FORMAT()` → `DATE_FORMAT()`, `EOMONTH()` → `LAST_DAY()`, `DATEDIFF()` |
| 3 | Stored procedure with temp tables | `#temp` → `TEMPORARY VIEW`; `ISNULL()` → `COALESCE()`, `@@ROWCOUNT` → `ROW_COUNT()`, `RAISERROR` → `SIGNAL`, `TRY_CAST()` → `TRY_CAST()` (same), `SET NOCOUNT ON` → remove |
| 4 | PolyBase external tables | External data source + file format + external table → `read_files()` or Auto Loader; `abfss://` paths work directly in Databricks |
| 5 | RFM analytics with STRING_AGG | `STRING_AGG()` → `ARRAY_JOIN(COLLECT_LIST())`, `TOP n` → `LIMIT n`, `FORMAT(num, 'N2')` → `FORMAT_NUMBER()`, nested `IIF()` → nested `IF()` or `CASE WHEN` |

### SQL Server (`03_SQLServer_Migration`)

| # | Pattern | Key Conversions |
|---|---------|-----------------|
| 1 | DDL with triggers and computed columns | Computed columns → `GENERATED ALWAYS AS`, triggers → Delta CDF or ETL logic, `SYSNAME` → `STRING`, `NEWSEQUENTIALID()` → `UUID()`, filtered indexes → remove |
| 2 | Stored proc with CROSS APPLY | `CROSS APPLY` → `JOIN LATERAL` or window functions, `STRING_SPLIT()` → `EXPLODE(SPLIT())`, `TRY_CONVERT()` → `TRY_CAST()`, `CHOOSE()` → `CASE WHEN`, `OUTPUT inserted/deleted` → Delta CDF |
| 3 | Dynamic PIVOT + JSON processing | `EXEC sp_executesql` → `EXECUTE IMMEDIATE`, `OPENJSON ... WITH` → `FROM_JSON()`, `JSON_VALUE()` → `GET_JSON_OBJECT()` or `:` notation, `ISJSON()` → validation logic |
| 4 | Recursive CTE + MERGE | `;WITH RECURSIVE` (remove leading `;`), `REPLICATE()` → `REPEAT()`, `FORMAT(num, 'N0')` → `FORMAT_NUMBER()`, `OPTION (MAXRECURSION)` → remove, `NOT MATCHED BY SOURCE` supported in Databricks MERGE |
| 5 | Temporal tables | `FOR SYSTEM_TIME AS OF` → `TIMESTAMP AS OF` (Delta Time Travel), `SYSTEM_VERSIONING` → Delta handles versioning natively, computed `margin_pct` → `GENERATED ALWAYS AS` |

---

## Setup Instructions

### Step 1: Import Notebooks

Upload the 4 notebooks to your Databricks workspace:

```bash
# Using Databricks CLI
PROFILE="your-profile"

databricks workspace mkdirs /Shared/genie-code-migration-demo --profile "$PROFILE"

databricks workspace import /Shared/genie-code-migration-demo/00_Demo_Playbook \
  --file notebooks/00_Demo_Playbook.py \
  --format SOURCE --language PYTHON --overwrite --profile "$PROFILE"

databricks workspace import /Shared/genie-code-migration-demo/01_Oracle_Migration \
  --file notebooks/01_Oracle_Migration.py \
  --format SOURCE --language PYTHON --overwrite --profile "$PROFILE"

databricks workspace import /Shared/genie-code-migration-demo/02_Synapse_Migration \
  --file notebooks/02_Synapse_Migration.py \
  --format SOURCE --language PYTHON --overwrite --profile "$PROFILE"

databricks workspace import /Shared/genie-code-migration-demo/03_SQLServer_Migration \
  --file notebooks/03_SQLServer_Migration.py \
  --format SOURCE --language PYTHON --overwrite --profile "$PROFILE"
```

Or just clone this repo and use the Databricks UI: **Workspace → Import → URL** and paste the raw GitHub link.

### Step 2: Verify

- Open any of the migration notebooks
- Click the Genie Code sparkle icon on the right
- Select **Agent** mode (toggle at bottom of panel)
- Type `/migrate` — it works out of the box, no additional setup needed
- You're ready to demo

**That's it.** `/migrate` is a built-in Genie Code feature. No skills, no plugins, no configuration required.

### Step 3 (Optional): Install Custom Migration Skills

The `skills/` folder contains optional Genie Code Skills that add explicit dialect-specific conversion rules. These are **not required** — `/migrate` works without them — but they can improve accuracy for edge cases and let you encode your organization's specific conventions (catalog names, naming patterns, preferred approaches).

```bash
# Upload skills to the workspace root (optional)
for skill in oracle-migration synapse-migration sqlserver-migration; do
  databricks workspace mkdirs "/Workspace/.assistant/skills/$skill" --profile "$PROFILE"
  databricks workspace import "/Workspace/.assistant/skills/$skill/SKILL.md" \
    --file "skills/$skill/SKILL.md" --format AUTO --overwrite --profile "$PROFILE"
done
```

Skills auto-load in Agent mode when Genie Code detects migration context. Users can also invoke them explicitly with `@oracle-migration`, `@synapse-migration`, or `@sqlserver-migration`.

---

## Demo Playbook (Step-by-Step)

### Pre-Demo (2 min)

1. Open the workspace and navigate to `/Shared/genie-code-migration-demo/`
2. Open `01_Oracle_Migration`, `02_Synapse_Migration`, `03_SQLServer_Migration` in separate browser tabs
3. Open Genie Code panel, confirm Agent mode is selected
4. Have the `00_Demo_Playbook` open in another tab as your cheat sheet

### Act 1: Oracle Migration (5 min)

**Opening line:**
> "Let's say your team has thousands of Oracle PL/SQL stored procedures. Traditionally this takes months of manual rewriting. Let me show you what Genie Code can do."

1. Open `01_Oracle_Migration`
2. Scroll to **Section 2** — PL/SQL Stored Procedure (payroll calculation)
3. In Genie Code, type: `/migrate`
4. Or prompt: *"Convert this Oracle PL/SQL procedure to Databricks SQL Scripting. Map Oracle types, handle the cursor with set-based SQL, and replace DBMS_OUTPUT with PRINT."*
5. **Point out the conversions** as they appear:
   - `NVL()` → `COALESCE()`
   - `SYSDATE` → `CURRENT_TIMESTAMP()`
   - Cursor → set-based SQL
   - Exception handling → `DECLARE EXIT HANDLER`
6. **Bonus** (if time): Flash Section 5 — `CONNECT BY` hierarchy → recursive CTE

### Act 2: Synapse Migration (5 min)

**Opening line:**
> "Many of you are migrating from Azure Synapse. Watch how Genie Code handles Synapse-specific constructs that have no direct equivalent."

1. Open `02_Synapse_Migration`
2. Scroll to **Section 1** — DDL with DISTRIBUTION and COLUMNSTORE
3. Prompt: *"Convert these Synapse table definitions to Databricks Delta tables. Remove distribution strategies and columnstore indexes."*
4. **Point out:**
   - `DISTRIBUTION = HASH(...)` → removed (Delta handles this)
   - `CLUSTERED COLUMNSTORE INDEX` → removed (Parquet is columnar)
   - `NVARCHAR(MAX)` → `STRING`, `MONEY` → `DECIMAL(19,4)`, `BIT` → `BOOLEAN`
5. Jump to **Section 3** — Stored Procedure with temp tables
6. Prompt: *"Convert this ETL procedure. Replace temp tables with temporary views and convert all Synapse functions."*
7. **Point out:** `#stg_sales` → temp view, `ISNULL()` → `COALESCE()`, `IIF()` → `IF()`

### Act 3: SQL Server Migration (5 min)

**Opening line:**
> "SQL Server is everywhere. Let me show the trickiest patterns — CROSS APPLY, JSON processing, temporal tables."

1. Open `03_SQLServer_Migration`
2. Scroll to **Section 2** — Stored Procedure with CROSS APPLY
3. Prompt: *"Convert this SQL Server stored procedure to Databricks. Handle CROSS APPLY, STRING_SPLIT, and the OUTPUT clause."*
4. **Point out:**
   - `CROSS APPLY` → `JOIN LATERAL` or window function
   - `STRING_SPLIT()` → `EXPLODE(SPLIT())`
   - `OUTPUT inserted/deleted` → Delta CDF alternative
5. **Bonus**: Flash Section 5 — Temporal tables → Delta Time Travel
   > "SQL Server temporal tables? That's just Delta Time Travel — `TIMESTAMP AS OF`. Built in."

### Wrap-Up (2 min)

Show the migration approach summary:

| Approach | Best For | Speed |
|---|---|---|
| **Genie Code `/migrate`** | Interactive, ad-hoc, last-mile | Seconds/file |
| **Genie Code + Skills** | Org-specific patterns | Seconds + quality boost |
| **Lakebridge** (rule-based) | Bulk transpilation (1000s of files) | Minutes |
| **Lakebridge Switch** (LLM) | Complex edge cases | Seconds/file |

**Closing line:**
> "The best migration uses Lakebridge for bulk conversion and Genie Code for the remainder. Together, 95%+ of code converts automatically."

**Propose next steps:**
1. Run Lakebridge Analyzer on their existing codebase (free)
2. Pick 10-20 representative files and convert live with Genie Code
3. Scale with Lakebridge + Genie Code for full migration

---

## Custom Skills Reference

The included skills teach Genie Code dialect-specific conversion rules:

| Skill | File | What It Adds |
|---|---|---|
| `oracle-migration` | `skills/oracle-migration/SKILL.md` | Oracle→Databricks type mappings, function mappings, PL/SQL patterns, hierarchical query conversion, date format tokens |
| `synapse-migration` | `skills/synapse-migration/SKILL.md` | Synapse→Databricks type mappings, CTAS patterns, PolyBase conversion, temp table handling, stored proc conversion |
| `sqlserver-migration` | `skills/sqlserver-migration/SKILL.md` | SQL Server→Databricks type mappings, CROSS APPLY patterns, JSON processing, temporal tables, trigger alternatives, OUTPUT clause handling |

Skills are installed at `Workspace/.assistant/skills/<name>/SKILL.md` and auto-load in Agent mode.

### Creating Your Own Skills

To add organization-specific rules (naming conventions, preferred patterns, catalog structure):

1. Create `Workspace/.assistant/skills/my-org-migration/SKILL.md`
2. Add YAML frontmatter with `name` and `description`
3. Add your conversion rules in markdown
4. Start a new Genie Code chat to pick up the skill

See [Genie Code Skills docs](https://docs.databricks.com/aws/en/genie-code/skills) for details.

---

## Prompt Cheat Sheet

Copy-paste these into Genie Code during demos:

### General
```
/migrate
```

### Oracle
```
Convert this Oracle PL/SQL to Databricks SQL Scripting. Replace cursors with set-based SQL, map Oracle data types, and handle exception blocks.
```
```
Convert this Oracle CONNECT BY hierarchical query to a Databricks recursive CTE.
```
```
Convert this Oracle MERGE statement to Databricks MERGE INTO for Delta Lake. Replace NVL with COALESCE and sequences with identity columns.
```

### Synapse
```
Convert this Synapse T-SQL to Databricks SQL. Remove distribution and columnstore clauses. Map NVARCHAR to STRING, MONEY to DECIMAL, BIT to BOOLEAN.
```
```
Convert this Synapse stored procedure to a Databricks Python notebook. Replace temp tables with DataFrames and convert all Synapse functions.
```
```
Convert this Synapse PolyBase external table to Databricks Auto Loader pattern for incremental ingestion from ADLS Gen2.
```

### SQL Server
```
Convert this SQL Server T-SQL to Databricks SQL. Handle CROSS APPLY, STRING_SPLIT, TRY_CONVERT, and the OUTPUT clause.
```
```
Convert this SQL Server temporal table query to use Delta Lake Time Travel (TIMESTAMP AS OF).
```
```
Convert this SQL Server dynamic PIVOT to Databricks SQL PIVOT syntax.
```

---

## Pre-Deployed Instance

These notebooks and skills are already deployed in:

- **Workspace:** `fevm-serverless-stable-m3qkky.cloud.databricks.com`
- **Notebooks:** `/Shared/genie-code-migration-demo/`
- **Skills:** `/Workspace/.assistant/skills/{oracle,synapse,sqlserver}-migration/`

---

*Created by Douglas Garcia Torres, Solutions Architect, Databricks EMEA*
