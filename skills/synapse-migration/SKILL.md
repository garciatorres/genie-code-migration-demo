---
name: synapse-migration
description: Convert Azure Synapse Analytics T-SQL code to Databricks SQL or PySpark. Handles CTAS, PolyBase, distribution strategies, temp tables, and Synapse-specific functions.
---

# Synapse to Databricks Migration Skill

You are an expert Azure Synapse-to-Databricks migration assistant. When the user pastes Synapse T-SQL code, convert it to Databricks SQL or PySpark following these rules precisely.

## Data Type Mappings

| Synapse | Databricks |
|---------|------------|
| BIGINT | BIGINT |
| INT | INT |
| SMALLINT | SMALLINT |
| TINYINT | TINYINT |
| BIT | BOOLEAN |
| DECIMAL(p,s) / NUMERIC(p,s) | DECIMAL(p,s) |
| MONEY | DECIMAL(19,4) |
| SMALLMONEY | DECIMAL(10,4) |
| FLOAT | DOUBLE |
| REAL | FLOAT |
| DATE | DATE |
| DATETIME | TIMESTAMP |
| DATETIME2(p) | TIMESTAMP |
| DATETIMEOFFSET | TIMESTAMP |
| TIME | STRING (HH:mm:ss format) |
| CHAR(n) | STRING |
| VARCHAR(n) | STRING |
| NCHAR(n) | STRING |
| NVARCHAR(n) | STRING |
| NVARCHAR(MAX) | STRING |
| TEXT / NTEXT | STRING |
| BINARY(n) | BINARY |
| VARBINARY(n) | BINARY |
| UNIQUEIDENTIFIER | STRING (use UUID() for generation) |
| SYSNAME | STRING |
| SQL_VARIANT | STRING |
| XML | STRING |
| GEOGRAPHY / GEOMETRY | STRING |
| IMAGE | BINARY |

## Function Mappings

| Synapse | Databricks |
|---------|------------|
| GETDATE() | CURRENT_TIMESTAMP() |
| SYSDATETIME() | CURRENT_TIMESTAMP() |
| GETUTCDATE() | CURRENT_TIMESTAMP() (Databricks uses UTC by default) |
| ISNULL(a, b) | COALESCE(a, b) |
| IIF(cond, a, b) | IF(cond, a, b) or CASE WHEN cond THEN a ELSE b END |
| FORMAT(date, fmt) | DATE_FORMAT(date, fmt) — convert .NET format strings |
| CONVERT(type, expr, style) | CAST(expr AS type) or DATE_FORMAT for date styles |
| TRY_CONVERT(type, expr) | TRY_CAST(expr AS type) |
| TRY_CAST(type, expr) | TRY_CAST(expr AS type) — same |
| DATEDIFF(part, start, end) | DATEDIFF(start, end) for days, or TIMESTAMPDIFF(part, start, end) |
| DATEADD(part, n, date) | DATE_ADD(date, n) for days, or date + INTERVAL n part |
| DATEPART(part, date) | EXTRACT(part FROM date) or specific functions (YEAR(), MONTH(), etc.) |
| EOMONTH(date) | LAST_DAY(date) |
| OBJECT_ID('tempdb..#table') | -- Remove, use CREATE OR REPLACE TEMPORARY VIEW |
| @@ROWCOUNT | -- Use ROW_COUNT() in SQL Scripting or track in Python |
| STRING_AGG(col, sep) | ARRAY_JOIN(COLLECT_LIST(col), sep) |
| STRING_SPLIT(str, sep) | EXPLODE(SPLIT(str, sep)) |
| QUOTENAME(col) | CONCAT('`', col, '`') |
| CHOOSE(idx, v1, v2, ...) | CASE idx WHEN 1 THEN v1 WHEN 2 THEN v2 ... END |
| TOP n | LIMIT n |
| NEWID() | UUID() |
| NEWSEQUENTIALID() | UUID() |
| SUSER_SNAME() | CURRENT_USER() |
| LEN(s) | LENGTH(RTRIM(s)) (LEN trims trailing spaces) |
| CHARINDEX(sub, s) | LOCATE(sub, s) |

## .NET Format Strings to Databricks

| Synapse FORMAT | Databricks DATE_FORMAT |
|---|---|
| yyyy-MM-dd | yyyy-MM-dd |
| yyyy-MM | yyyy-MM |
| dd/MM/yyyy | dd/MM/yyyy |
| N2 (number format) | FORMAT_NUMBER(expr, 2) |
| N0 | FORMAT_NUMBER(expr, 0) |

## CONVERT Styles to Databricks

| CONVERT Style | Pattern | Databricks |
|---|---|---|
| 23 / 120 | yyyy-mm-dd | DATE_FORMAT(date, 'yyyy-MM-dd') |
| 103 | dd/mm/yyyy | DATE_FORMAT(date, 'dd/MM/yyyy') |
| 108 | hh:mi:ss | DATE_FORMAT(date, 'HH:mm:ss') |

## DDL Conversion Rules

1. **Remove** WITH (DISTRIBUTION = ...) — Databricks handles distribution automatically
2. **Remove** CLUSTERED COLUMNSTORE INDEX — Delta uses columnar storage by default
3. **Remove** CLUSTERED INDEX (...) — convert to CLUSTER BY or Z-ORDER
4. **Convert** PARTITION (...) to PARTITIONED BY in Delta (date-based partitioning)
5. **Remove** IDENTITY(1,1) — use BIGINT GENERATED ALWAYS AS IDENTITY
6. **Remove** square brackets [...] — use backticks or plain names
7. **Convert** [dbo].[table] to catalog.schema.table (Unity Catalog)
8. **Convert** computed columns (AS expr PERSISTED) to GENERATED ALWAYS AS (expr)
9. **Convert** DEFAULT GETDATE() to DEFAULT CURRENT_TIMESTAMP()
10. **Remove** filtered indexes (WHERE clause in CREATE INDEX) — not applicable

## Synapse-Specific Pattern Conversions

### CTAS (Create Table As Select)
```
-- Synapse:
CREATE TABLE t WITH (DISTRIBUTION = HASH(col), CLUSTERED COLUMNSTORE INDEX) AS SELECT ...
-- Databricks:
CREATE OR REPLACE TABLE t AS SELECT ...
```

### Temp Tables (#tables)
```
-- Synapse:
CREATE TABLE #temp WITH (DISTRIBUTION = HASH(col)) AS SELECT ...
-- Databricks:
CREATE OR REPLACE TEMPORARY VIEW temp AS SELECT ...
-- Or use Python: df = spark.sql("SELECT ...")
```

### PolyBase External Tables
```
-- Synapse:
CREATE EXTERNAL DATA SOURCE / FILE FORMAT / TABLE
-- Databricks:
CREATE TABLE USING ... LOCATION or Auto Loader (cloud_files)
```

### Stored Procedures
```
-- Synapse:
CREATE PROCEDURE sp_name @param TYPE AS BEGIN ... END
-- Databricks:
-- Option A: SQL Scripting
DECLARE param TYPE DEFAULT value;
BEGIN ... END;
-- Option B: Python notebook function
```

## Important Notes

- Remove SET NOCOUNT ON / SET XACT_ABORT ON — not applicable
- Remove BEGIN TRANSACTION / COMMIT / ROLLBACK — Delta handles atomicity
- Convert RAISERROR/THROW to RAISE or Python raise
- Convert @@ROWCOUNT tracking to SQL Scripting ROW_COUNT() or Python tracking
- Synapse MERGE supports 3 clauses (MATCHED, NOT MATCHED BY TARGET, NOT MATCHED BY SOURCE) — Databricks MERGE INTO supports all three
- PolyBase external tables → Use Auto Loader (cloud_files) for streaming ingestion or CREATE TABLE USING for one-time loads
- For ADLS paths: abfss:// works directly in Databricks with UC external locations
