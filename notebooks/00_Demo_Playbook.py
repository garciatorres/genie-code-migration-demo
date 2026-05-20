# Databricks notebook source
# MAGIC %md
# MAGIC # Code Migration with Genie Code -- Demo Playbook
# MAGIC ### From Oracle, Synapse, and SQL Server to Databricks
# MAGIC
# MAGIC **Duration:** 15-30 minutes | **Audience:** Technical decision-makers, architects, developers
# MAGIC
# MAGIC **What this demo shows:** Databricks Genie Code (Databricks One) can interactively convert legacy SQL code from Oracle, Azure Synapse, and SQL Server to Databricks SQL -- right inside the notebook editor.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Key Messages
# MAGIC
# MAGIC | Point | Message |
# MAGIC |-------|---------|
# MAGIC | **Speed** | Convert code in seconds, not weeks |
# MAGIC | **Context-aware** | Genie Code understands your Unity Catalog metadata, table schemas, and business context |
# MAGIC | **Extensible** | Custom migration Skills teach Genie Code your organization's specific patterns and conventions |
# MAGIC | **Integrated** | No external tools needed -- migration happens where you develop |
# MAGIC | **Complementary** | Pairs with Lakebridge for bulk transpilation; Genie Code handles the last-mile cleanup |
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Demo Setup Checklist
# MAGIC
# MAGIC Before running the demo:
# MAGIC
# MAGIC - [ ] Ensure Genie Code is enabled on the workspace (should be on by default)
# MAGIC - [ ] Open the Genie Code panel (sparkle icon on right side)
# MAGIC - [ ] Select **Agent** mode (toggle at bottom of the panel)
# MAGIC - [ ] Verify the migration skills are installed:
# MAGIC   - `Workspace/.assistant/skills/oracle-migration/SKILL.md`
# MAGIC   - `Workspace/.assistant/skills/synapse-migration/SKILL.md`
# MAGIC   - `Workspace/.assistant/skills/sqlserver-migration/SKILL.md`
# MAGIC - [ ] Open the 3 source notebooks in tabs so you can switch quickly

# COMMAND ----------

# MAGIC %md
# MAGIC ## Demo Flow
# MAGIC
# MAGIC ### Act 1: Oracle Migration (5 min)
# MAGIC **Open:** `01_Oracle_Migration`
# MAGIC
# MAGIC **Talking points:**
# MAGIC > "Let's say your team has thousands of Oracle PL/SQL stored procedures. Traditionally this takes months of manual rewriting. Let me show you what Genie Code can do."
# MAGIC
# MAGIC **Demo steps:**
# MAGIC 1. Scroll to **Section 2** (PL/SQL Stored Procedure -- payroll calculation)
# MAGIC 2. In Genie Code panel, type: **`/migrate`**
# MAGIC 3. When it asks for context, say: *"Convert this Oracle PL/SQL procedure to Databricks SQL Scripting. Map Oracle types, handle the cursor with set-based SQL, and replace DBMS_OUTPUT with PRINT."*
# MAGIC 4. **Point out the conversions:**
# MAGIC    - `NVL()` → `COALESCE()`
# MAGIC    - `SYSDATE` / `SYSTIMESTAMP` → `CURRENT_TIMESTAMP()`
# MAGIC    - Cursor loop → set-based INSERT ... SELECT
# MAGIC    - `DBMS_OUTPUT.PUT_LINE` → removed or `PRINT`
# MAGIC    - Exception handling → `DECLARE EXIT HANDLER`
# MAGIC 5. **Bonus:** Quick flash of Section 5 (CONNECT BY hierarchy → recursive CTE)
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Act 2: Synapse Migration (5 min)
# MAGIC **Open:** `02_Synapse_Migration`
# MAGIC
# MAGIC **Talking points:**
# MAGIC > "Many of you are migrating from Azure Synapse. This is the most common migration we see in the Nordics. Watch how Genie Code handles Synapse-specific constructs."
# MAGIC
# MAGIC **Demo steps:**
# MAGIC 1. Scroll to **Section 1** (DDL with DISTRIBUTION and COLUMNSTORE)
# MAGIC 2. In Genie Code panel, type: *"Convert these Synapse table definitions to Databricks Delta tables. Remove distribution strategies and columnstore indexes."*
# MAGIC 3. **Point out:**
# MAGIC    - `WITH (DISTRIBUTION = HASH(...))` → removed (Delta handles this)
# MAGIC    - `CLUSTERED COLUMNSTORE INDEX` → removed (Parquet is columnar)
# MAGIC    - `NVARCHAR(MAX)` → `STRING`
# MAGIC    - `MONEY` → `DECIMAL(19,4)`
# MAGIC    - `BIT` → `BOOLEAN`
# MAGIC    - `IDENTITY(1,1)` → `GENERATED ALWAYS AS IDENTITY`
# MAGIC 4. Then jump to **Section 3** (Stored Procedure with #temp tables)
# MAGIC 5. Prompt: *"Convert this ETL procedure to a Databricks notebook. Replace temp tables with temporary views and convert Synapse functions."*
# MAGIC 6. **Point out:**
# MAGIC    - `#stg_sales` → `TEMPORARY VIEW`
# MAGIC    - `ISNULL()` → `COALESCE()`
# MAGIC    - `IIF()` → `IF()` or `CASE WHEN`
# MAGIC    - `FORMAT()` → `DATE_FORMAT()`
# MAGIC    - `@@ROWCOUNT` → tracked with variables
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ### Act 3: SQL Server Migration (5 min)
# MAGIC **Open:** `03_SQLServer_Migration`
# MAGIC
# MAGIC **Talking points:**
# MAGIC > "SQL Server is everywhere. Let me show how Genie Code handles the trickiest T-SQL patterns -- CROSS APPLY, JSON processing, temporal tables."
# MAGIC
# MAGIC **Demo steps:**
# MAGIC 1. Scroll to **Section 2** (Stored Procedure with CROSS APPLY)
# MAGIC 2. Prompt: *"Convert this SQL Server stored procedure to Databricks. Handle CROSS APPLY, STRING_SPLIT, and the OUTPUT clause."*
# MAGIC 3. **Point out:**
# MAGIC    - `CROSS APPLY` → `JOIN LATERAL` or window function
# MAGIC    - `STRING_SPLIT()` → `EXPLODE(SPLIT())`
# MAGIC    - `TRY_CONVERT()` → `TRY_CAST()`
# MAGIC    - `CHOOSE()` → `CASE WHEN`
# MAGIC    - `OUTPUT inserted/deleted INTO` → Delta CDF alternative
# MAGIC 4. **Bonus:** Flash Section 5 (Temporal Tables → Delta Time Travel)
# MAGIC    > "SQL Server temporal tables? That's just Delta Time Travel -- `TIMESTAMP AS OF`. Built into the platform."
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prompt Cheat Sheet
# MAGIC
# MAGIC Copy-paste these prompts into Genie Code during the demo:
# MAGIC
# MAGIC ### General
# MAGIC ```
# MAGIC /migrate
# MAGIC ```
# MAGIC
# MAGIC ### Oracle
# MAGIC ```
# MAGIC Convert this Oracle PL/SQL to Databricks SQL Scripting. Replace cursors with set-based SQL, map Oracle data types, and handle exception blocks.
# MAGIC ```
# MAGIC
# MAGIC ```
# MAGIC Convert this Oracle CONNECT BY hierarchical query to a Databricks recursive CTE.
# MAGIC ```
# MAGIC
# MAGIC ```
# MAGIC Convert this Oracle MERGE statement to Databricks MERGE INTO for Delta Lake. Replace NVL with COALESCE and sequences with identity columns.
# MAGIC ```
# MAGIC
# MAGIC ### Synapse
# MAGIC ```
# MAGIC Convert this Synapse T-SQL to Databricks SQL. Remove distribution and columnstore clauses. Map NVARCHAR to STRING, MONEY to DECIMAL, BIT to BOOLEAN.
# MAGIC ```
# MAGIC
# MAGIC ```
# MAGIC Convert this Synapse stored procedure to a Databricks Python notebook. Replace temp tables with DataFrames and convert all Synapse functions.
# MAGIC ```
# MAGIC
# MAGIC ```
# MAGIC Convert this Synapse PolyBase external table to Databricks Auto Loader pattern for incremental ingestion from ADLS Gen2.
# MAGIC ```
# MAGIC
# MAGIC ### SQL Server
# MAGIC ```
# MAGIC Convert this SQL Server T-SQL to Databricks SQL. Handle CROSS APPLY, STRING_SPLIT, TRY_CONVERT, and the OUTPUT clause.
# MAGIC ```
# MAGIC
# MAGIC ```
# MAGIC Convert this SQL Server temporal table query to use Delta Lake Time Travel (TIMESTAMP AS OF).
# MAGIC ```
# MAGIC
# MAGIC ```
# MAGIC Convert this SQL Server dynamic PIVOT to Databricks SQL PIVOT syntax.
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Closing / Wrap-Up
# MAGIC
# MAGIC ### Key Takeaway Slide
# MAGIC
# MAGIC | Migration Approach | Best For | Speed |
# MAGIC |---|---|---|
# MAGIC | **Genie Code `/migrate`** | Interactive, ad-hoc, last-mile cleanup | Seconds per file |
# MAGIC | **Genie Code + Custom Skills** | Organization-specific patterns, conventions | Seconds + quality boost |
# MAGIC | **Lakebridge Morpheus** | Bulk T-SQL transpilation (rule-based) | Minutes for 1000s of files |
# MAGIC | **Lakebridge BladeBridge** | Complex Oracle/Teradata (rule-based) | Minutes for 1000s of files |
# MAGIC | **Lakebridge Switch** | Complex edge cases (LLM-powered) | Seconds per file |
# MAGIC
# MAGIC > "The best migration strategy uses **Lakebridge for bulk conversion** and **Genie Code for the remainder** -- the edge cases, the cleanup, the last-mile fixes. Together, they cover 95%+ of code automatically."
# MAGIC
# MAGIC ### Next Steps to Propose
# MAGIC 1. **Assessment:** Run Lakebridge Analyzer on your existing codebase (free, open-source)
# MAGIC 2. **Pilot:** Pick 10-20 representative SQL files and convert them with Genie Code live
# MAGIC 3. **Scale:** Use Lakebridge + Genie Code for the full migration
# MAGIC
# MAGIC ---
# MAGIC *Demo created by Douglas Garcia Torres, Solutions Architect, Databricks EMEA*
