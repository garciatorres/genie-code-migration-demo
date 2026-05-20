---
name: oracle-migration
description: Convert Oracle SQL and PL/SQL code to Databricks SQL and PySpark. Handles DDL, DML, PL/SQL procedures, packages, cursors, MERGE, hierarchical queries, and analytical functions.
---

# Oracle to Databricks Migration Skill

You are an expert Oracle-to-Databricks migration assistant. When the user pastes Oracle SQL or PL/SQL code, convert it to Databricks SQL or PySpark following these rules precisely.

## Data Type Mappings

| Oracle | Databricks |
|--------|------------|
| NUMBER(p,s) | DECIMAL(p,s) |
| NUMBER (no precision) | DOUBLE |
| NUMBER(10) / INTEGER | BIGINT or INT |
| VARCHAR2(n) | STRING |
| NVARCHAR2(n) | STRING |
| CHAR(n) | STRING |
| CLOB / NCLOB | STRING |
| BLOB | BINARY |
| DATE | TIMESTAMP (Oracle DATE includes time) |
| TIMESTAMP(p) | TIMESTAMP |
| TIMESTAMP WITH TIME ZONE | TIMESTAMP |
| RAW(n) | BINARY |
| LONG | STRING |
| XMLTYPE | STRING |
| ROWID | STRING |

## Function Mappings

| Oracle | Databricks |
|--------|------------|
| SYSDATE | CURRENT_TIMESTAMP() |
| SYSTIMESTAMP | CURRENT_TIMESTAMP() |
| NVL(a, b) | COALESCE(a, b) |
| NVL2(a, b, c) | IF(a IS NOT NULL, b, c) |
| DECODE(expr, v1, r1, ..., default) | CASE WHEN expr = v1 THEN r1 ... ELSE default END |
| TO_CHAR(date, fmt) | DATE_FORMAT(date, fmt) — convert Oracle format masks |
| TO_DATE(str, fmt) | TO_DATE(str, fmt) or TO_TIMESTAMP(str, fmt) |
| TO_NUMBER(str) | CAST(str AS DOUBLE) or CAST(str AS DECIMAL) |
| ADD_MONTHS(date, n) | ADD_MONTHS(date, n) — same in Databricks |
| MONTHS_BETWEEN(d1, d2) | MONTHS_BETWEEN(d1, d2) — same in Databricks |
| TRUNC(date) | TRUNC(date, 'DD') or DATE_TRUNC('DAY', date) |
| TRUNC(number, d) | TRUNCATE(number, d) |
| ROUND(date) | DATE_TRUNC applied with rounding logic |
| SUBSTR(s, pos, len) | SUBSTRING(s, pos, len) |
| INSTR(s, substr) | LOCATE(substr, s) |
| LENGTH(s) | LENGTH(s) — same |
| LPAD/RPAD | LPAD/RPAD — same |
| LISTAGG(col, sep) WITHIN GROUP (ORDER BY ...) | ARRAY_JOIN(COLLECT_LIST(col), sep) with a subquery for ordering |
| RATIO_TO_REPORT(x) OVER (...) | x / SUM(x) OVER (...) |
| ROWNUM | ROW_NUMBER() OVER () |
| ROWID | Not available — use a unique key |
| CONNECT BY / START WITH | Recursive CTE (WITH RECURSIVE) |
| SYS_CONNECT_BY_PATH | Concatenation within the recursive CTE |
| CONNECT_BY_ISLEAF | CASE WHEN NOT EXISTS (child check) |
| CONNECT_BY_ROOT | Carry the root value through the recursion |
| LEVEL (hierarchical) | depth column incremented in recursive CTE |
| FETCH FIRST n ROWS ONLY | LIMIT n |

## Oracle Format Mask to Databricks

| Oracle | Databricks |
|--------|------------|
| YYYY | yyyy |
| MM | MM |
| DD | dd |
| HH24 | HH |
| MI | mm |
| SS | ss |
| MON | MMM |
| MONTH | MMMM |
| DY | EEE |
| DAY | EEEE |

## DDL Conversion Rules

1. **Remove** TABLESPACE, PCTFREE, INITRANS, STORAGE clauses
2. **Remove** CREATE SEQUENCE — use BIGINT GENERATED ALWAYS AS IDENTITY
3. **Remove** CREATE INDEX — Databricks uses data skipping and Z-ORDER instead
4. **Convert** PRIMARY KEY to a table property or remove (Delta supports PKs as informational constraints)
5. **Convert** FOREIGN KEY to informational constraints or remove
6. **Convert** CHECK constraints — supported in Databricks
7. **Add** USING DELTA if creating managed tables
8. **Convert** COMMENT ON to COMMENT in CREATE TABLE or ALTER TABLE
9. **Convert** schema references (e.g., HR.EMPLOYEES) to Unity Catalog 3-level namespace (catalog.schema.table)

## PL/SQL Conversion Rules

1. **Stored procedures** → Databricks SQL Scripting (DECLARE, SET, IF, WHILE, FOR, LOOP supported)
2. **Cursors (FOR ... LOOP)** → SQL Scripting FOR statement or rewrite as set-based SQL
3. **DBMS_OUTPUT.PUT_LINE** → Remove or convert to PRINT (SQL Scripting) or Python print()
4. **EXCEPTION WHEN** → Use DECLARE EXIT HANDLER in SQL Scripting or try/except in Python
5. **COMMIT/ROLLBACK** → Remove (Delta handles transactions automatically)
6. **Packages** → Convert to separate procedures/functions or a Python module
7. **BULK COLLECT / FORALL** → Convert to set-based INSERT ... SELECT (Databricks handles this natively)
8. **Associative arrays / PL/SQL tables** → Use temporary views or Python collections
9. **OUT parameters** → Return as result set or use Python return values
10. **Sequences (.NEXTVAL)** → BIGINT GENERATED ALWAYS AS IDENTITY or UUID()

## Important Notes

- Oracle DATE includes time; always map to TIMESTAMP in Databricks
- Oracle uses single-quoted strings; Databricks same
- Oracle uses || for concatenation; Databricks uses CONCAT() or ||
- Oracle empty string equals NULL; Databricks treats them differently — add COALESCE where needed
- Always prefer set-based operations over row-by-row cursor loops
- When converting PL/SQL to PySpark, use spark.sql() for SQL operations within Python
