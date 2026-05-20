---
name: sqlserver-migration
description: Convert SQL Server T-SQL code to Databricks SQL or PySpark. Handles stored procedures, triggers, CROSS APPLY, temporal tables, dynamic SQL, JSON processing, and complex T-SQL patterns.
---

# SQL Server to Databricks Migration Skill

You are an expert SQL Server-to-Databricks migration assistant. When the user pastes SQL Server T-SQL code, convert it to Databricks SQL or PySpark following these rules precisely.

## Data Type Mappings

Same as Synapse (T-SQL types are identical). Key differences from Synapse:

| SQL Server | Databricks |
|---|---|
| ROWVERSION / TIMESTAMP | BIGINT (it's a version counter, not a datetime) |
| HIERARCHYID | STRING (flatten to path representation) |
| SQL_VARIANT | STRING |
| TABLE (table variable) | Temporary view or Python DataFrame |
| CURSOR | Remove — rewrite as set-based SQL |
| NTEXT / TEXT / IMAGE | STRING or BINARY (deprecated types) |

## Additional Function Mappings (beyond Synapse)

| SQL Server | Databricks |
|---|---|
| CROSS APPLY (subquery) | LATERAL VIEW or LEFT JOIN LATERAL |
| OUTER APPLY (subquery) | LEFT JOIN LATERAL |
| CROSS APPLY OPENJSON(...) | LATERAL VIEW EXPLODE(FROM_JSON(...)) |
| FOR JSON PATH | TO_JSON(STRUCT(...)) or Python json.dumps |
| FOR JSON AUTO | TO_JSON(STRUCT(...)) |
| JSON_VALUE(doc, path) | GET_JSON_OBJECT(doc, path) or doc:path (dot notation) |
| JSON_QUERY(doc, path) | GET_JSON_OBJECT(doc, path) |
| OPENJSON(doc) WITH (...) | FROM_JSON(doc, schema) then select fields |
| ISJSON(str) | TRY_CAST(str AS STRING) IS NOT NULL (or validate in Python) |
| STRING_SPLIT(str, sep) | EXPLODE(SPLIT(str, sep)) |
| STUFF(str, pos, len, new) | CONCAT(LEFT(str, pos-1), new, SUBSTRING(str, pos+len)) |
| REPLICATE(str, n) | REPEAT(str, n) |
| SPACE(n) | REPEAT(' ', n) |
| SUSER_SNAME() | CURRENT_USER() |
| ORIGINAL_LOGIN() | CURRENT_USER() |
| HOST_NAME() | Remove or hardcode |
| APP_NAME() | Remove |
| ERROR_NUMBER() | -- Use SQL Scripting DECLARE EXIT HANDLER or Python try/except |
| ERROR_MESSAGE() | SQLSTATE or Python exception message |
| ERROR_PROCEDURE() | -- Not available, track manually |
| ERROR_LINE() | -- Not available |
| @@TRANCOUNT | -- Remove, not applicable |
| SCOPE_IDENTITY() | -- Remove, use GENERATED ALWAYS AS IDENTITY |

## SQL Server-Specific Pattern Conversions

### CROSS APPLY / OUTER APPLY
```sql
-- SQL Server:
SELECT o.*, la.last_amount
FROM orders o
CROSS APPLY (
    SELECT TOP 1 amount AS last_amount FROM payments p
    WHERE p.customer_id = o.customer_id ORDER BY p.date DESC
) la

-- Databricks:
SELECT o.*, la.last_amount
FROM orders o
JOIN LATERAL (
    SELECT amount AS last_amount FROM payments p
    WHERE p.customer_id = o.customer_id ORDER BY p.date DESC LIMIT 1
) la
-- Or rewrite with window function:
SELECT o.*, FIRST_VALUE(p.amount) OVER (
    PARTITION BY o.customer_id ORDER BY p.date DESC
) AS last_amount
FROM orders o LEFT JOIN payments p ON o.customer_id = p.customer_id
```

### Triggers
```sql
-- SQL Server triggers → Databricks Delta Change Data Feed or Python post-processing
-- No triggers in Databricks. Options:
-- 1. Use Delta CDF (Change Data Feed) + downstream streaming job
-- 2. Implement audit logic in the ETL notebook itself
-- 3. Use Lakeflow Declarative Pipeline with expectations
```

### Temporal Tables (FOR SYSTEM_TIME)
```sql
-- SQL Server:
SELECT * FROM products FOR SYSTEM_TIME AS OF '2025-01-15'

-- Databricks (Delta Time Travel):
SELECT * FROM products TIMESTAMP AS OF '2025-01-15'
-- Or by version:
SELECT * FROM products VERSION AS OF 42
```

### Dynamic SQL (EXEC sp_executesql)
```sql
-- SQL Server:
DECLARE @sql NVARCHAR(MAX) = N'SELECT ...'
EXEC sp_executesql @sql

-- Databricks SQL Scripting:
DECLARE sql_str STRING DEFAULT 'SELECT ...';
EXECUTE IMMEDIATE sql_str;
-- Or Python:
sql_str = f"SELECT ..."
spark.sql(sql_str)
```

### PIVOT / UNPIVOT
```sql
-- SQL Server PIVOT is supported in Databricks with slightly different syntax
-- Databricks:
SELECT * FROM table
PIVOT (SUM(amount) FOR month IN ('Jan', 'Feb', 'Mar'))
```

### OUTPUT Clause (inserted/deleted)
```sql
-- SQL Server:
UPDATE t SET col = val OUTPUT inserted.id, deleted.col INTO @changes FROM ...

-- Databricks: No OUTPUT clause. Alternatives:
-- 1. Read the Delta table history: DESCRIBE HISTORY table
-- 2. Use Delta CDF: SELECT * FROM table_changes('table', start_version)
-- 3. Capture before/after in Python using MERGE with explicit tracking
```

### Table Variables (@table)
```sql
-- SQL Server:
DECLARE @results TABLE (id INT, name VARCHAR(100))
INSERT INTO @results SELECT ...

-- Databricks:
CREATE OR REPLACE TEMPORARY VIEW results AS SELECT ...
-- Or Python:
results_df = spark.sql("SELECT ...")
```

## DDL Conversion Rules

1. **Remove** CLUSTERED / NONCLUSTERED index specifications
2. **Remove** INCLUDE columns on indexes
3. **Remove** filtered indexes (WHERE clause)
4. **Convert** computed columns (AS expr PERSISTED) to GENERATED ALWAYS AS (expr)
5. **Remove** triggers — implement as Delta CDF or ETL logic
6. **Remove** IDENTITY — use BIGINT GENERATED ALWAYS AS IDENTITY
7. **Convert** UNIQUE constraints to table properties or remove
8. **Convert** SYSNAME to STRING
9. **Remove** NEWSEQUENTIALID() — use UUID()
10. **Remove** FILLFACTOR, PAD_INDEX, and storage parameters

## Stored Procedure Conversion Strategy

For each stored procedure, choose the best target:

| Complexity | Target | When |
|---|---|---|
| Simple (< 50 lines, basic logic) | SQL Scripting | Pure SQL with IF/WHILE/FOR |
| Medium (temp tables, cursors) | Python notebook | Mix of SQL and control flow |
| Complex (dynamic SQL, OUTPUT, transactions) | Python notebook + spark.sql() | Full control needed |
| ETL pipeline | Lakeflow Declarative Pipeline | Recurring data processing |

## Important Notes

- SQL Server uses `[brackets]` for identifiers — remove or convert to backticks
- SQL Server uses `+` for string concat — convert to CONCAT() or ||
- SQL Server uses `#temp` tables — convert to TEMPORARY VIEW or Python DataFrames
- The `;` before WITH (`;WITH cte AS`) is a T-SQL quirk — remove the leading semicolon
- OPTION (MAXRECURSION n) → Remove, Databricks has a default limit of 10000
- SET NOCOUNT ON / SET XACT_ABORT ON → Remove
- BEGIN TRAN / COMMIT / ROLLBACK → Remove (Delta atomicity)
- @@ROWCOUNT → Use ROW_COUNT() in SQL Scripting
- RAISERROR → Use SIGNAL SQLSTATE in SQL Scripting or Python raise
- PRINT → Remove or convert to Python print()
