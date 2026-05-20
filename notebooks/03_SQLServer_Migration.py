# Databricks notebook source
# MAGIC %md
# MAGIC # SQL Server T-SQL to Databricks Migration
# MAGIC ### Genie Code `/migrate` Demo
# MAGIC
# MAGIC This notebook contains **real-world SQL Server T-SQL patterns** including stored procedures,
# MAGIC triggers, CROSS APPLY, system functions, and complex ETL logic.
# MAGIC
# MAGIC **How to use this demo:**
# MAGIC 1. Select the source code cell below
# MAGIC 2. Open Genie Code panel (sparkle icon) and select **Agent** mode
# MAGIC 3. Type `/migrate` or prompt: *"Convert this SQL Server T-SQL to Databricks SQL"*
# MAGIC 4. Watch Genie Code convert the code live
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. SQL Server DDL -- Tables with Constraints, Computed Columns, Triggers
# MAGIC SQL Server-specific features: `IDENTITY`, computed columns, `NEWSEQUENTIALID()`, triggers.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- SOURCE: SQL Server 2019 -- Order Management System
# MAGIC -- CONVERT TO: Databricks SQL (Delta Lake)
# MAGIC -- ============================================================
# MAGIC
# MAGIC CREATE TABLE [sales].[orders] (
# MAGIC     [order_id]          BIGINT IDENTITY(1,1)    NOT NULL,
# MAGIC     [order_guid]        UNIQUEIDENTIFIER        DEFAULT NEWSEQUENTIALID(),
# MAGIC     [customer_id]       INT                     NOT NULL,
# MAGIC     [order_date]        DATETIME2(3)            DEFAULT SYSDATETIME(),
# MAGIC     [ship_date]         DATE                    NULL,
# MAGIC     [status]            TINYINT                 DEFAULT 1,
# MAGIC     [subtotal]          MONEY                   NOT NULL,
# MAGIC     [tax_rate]          DECIMAL(5,4)            DEFAULT 0.25,
# MAGIC     [tax_amount]        AS CAST([subtotal] * [tax_rate] AS MONEY) PERSISTED,
# MAGIC     [total_amount]      AS CAST([subtotal] * (1 + [tax_rate]) AS MONEY) PERSISTED,
# MAGIC     [shipping_address]  NVARCHAR(MAX),
# MAGIC     [is_priority]       BIT                     DEFAULT 0,
# MAGIC     [notes]             NTEXT,
# MAGIC     [created_by]        SYSNAME                 DEFAULT SUSER_SNAME(),
# MAGIC     [modified_at]       DATETIME2               DEFAULT SYSDATETIME(),
# MAGIC     CONSTRAINT [PK_orders] PRIMARY KEY CLUSTERED ([order_id]),
# MAGIC     CONSTRAINT [FK_orders_customer] FOREIGN KEY ([customer_id])
# MAGIC         REFERENCES [sales].[customers]([customer_id]),
# MAGIC     CONSTRAINT [CK_orders_status] CHECK ([status] IN (1,2,3,4,5)),
# MAGIC     INDEX [IX_orders_customer] NONCLUSTERED ([customer_id]) INCLUDE ([order_date], [total_amount]),
# MAGIC     INDEX [IX_orders_date] NONCLUSTERED ([order_date]) WHERE [status] < 4
# MAGIC );
# MAGIC
# MAGIC -- Trigger for audit trail
# MAGIC CREATE TRIGGER [sales].[trg_orders_audit]
# MAGIC ON [sales].[orders]
# MAGIC AFTER UPDATE
# MAGIC AS
# MAGIC BEGIN
# MAGIC     SET NOCOUNT ON;
# MAGIC     INSERT INTO [audit].[order_changes] (order_id, field_name, old_value, new_value, changed_at, changed_by)
# MAGIC     SELECT
# MAGIC         i.order_id,
# MAGIC         'status',
# MAGIC         CAST(d.status AS NVARCHAR(10)),
# MAGIC         CAST(i.status AS NVARCHAR(10)),
# MAGIC         SYSDATETIME(),
# MAGIC         SUSER_SNAME()
# MAGIC     FROM inserted i
# MAGIC     INNER JOIN deleted d ON i.order_id = d.order_id
# MAGIC     WHERE i.status <> d.status;
# MAGIC END;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. SQL Server Stored Procedure -- Complex Business Logic with CROSS APPLY
# MAGIC Real-world order processing with `CROSS APPLY`, `TRY_CONVERT`, `STRING_SPLIT`, and `OUTPUT`.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- SOURCE: SQL Server -- Order Processing Pipeline
# MAGIC -- CONVERT TO: Databricks SQL Scripting or PySpark
# MAGIC -- ============================================================
# MAGIC
# MAGIC CREATE OR ALTER PROCEDURE [sales].[sp_process_orders]
# MAGIC     @order_ids NVARCHAR(MAX),     -- comma-separated list
# MAGIC     @operator_id INT,
# MAGIC     @process_date DATE = NULL,
# MAGIC     @dry_run BIT = 0
# MAGIC AS
# MAGIC BEGIN
# MAGIC     SET NOCOUNT ON;
# MAGIC     SET XACT_ABORT ON;
# MAGIC
# MAGIC     DECLARE @processed_count INT = 0;
# MAGIC     DECLARE @error_count INT = 0;
# MAGIC     DECLARE @batch_id UNIQUEIDENTIFIER = NEWID();
# MAGIC
# MAGIC     IF @process_date IS NULL
# MAGIC         SET @process_date = CAST(SYSDATETIME() AS DATE);
# MAGIC
# MAGIC     -- Parse order IDs from comma-separated string
# MAGIC     CREATE TABLE #order_list (order_id BIGINT);
# MAGIC     INSERT INTO #order_list (order_id)
# MAGIC     SELECT TRY_CONVERT(BIGINT, LTRIM(RTRIM(value)))
# MAGIC     FROM STRING_SPLIT(@order_ids, ',')
# MAGIC     WHERE TRY_CONVERT(BIGINT, LTRIM(RTRIM(value))) IS NOT NULL;
# MAGIC
# MAGIC     -- Validate orders exist and are in correct status
# MAGIC     IF NOT EXISTS (SELECT 1 FROM #order_list)
# MAGIC     BEGIN
# MAGIC         RAISERROR('No valid order IDs provided', 16, 1);
# MAGIC         RETURN;
# MAGIC     END
# MAGIC
# MAGIC     BEGIN TRY
# MAGIC         BEGIN TRANSACTION;
# MAGIC
# MAGIC         -- Step 1: Enrich orders with customer and product details using CROSS APPLY
# MAGIC         SELECT
# MAGIC             o.order_id,
# MAGIC             o.customer_id,
# MAGIC             o.order_date,
# MAGIC             o.total_amount,
# MAGIC             c.customer_name,
# MAGIC             c.credit_limit,
# MAGIC             latest_payment.last_payment_date,
# MAGIC             latest_payment.last_payment_amount,
# MAGIC             line_summary.total_items,
# MAGIC             line_summary.total_weight,
# MAGIC             IIF(o.total_amount > c.credit_limit, 1, 0) AS exceeds_credit,
# MAGIC             DATEDIFF(DAY, o.order_date, @process_date) AS days_pending,
# MAGIC             CHOOSE(o.status, 'New', 'Confirmed', 'Shipped', 'Delivered', 'Cancelled') AS status_name,
# MAGIC             TRY_CONVERT(VARCHAR(10), o.order_date, 23) AS order_date_iso
# MAGIC         INTO #enriched_orders
# MAGIC         FROM [sales].[orders] o
# MAGIC         INNER JOIN #order_list ol ON o.order_id = ol.order_id
# MAGIC         INNER JOIN [sales].[customers] c ON o.customer_id = c.customer_id
# MAGIC         CROSS APPLY (
# MAGIC             SELECT TOP 1
# MAGIC                 p.payment_date AS last_payment_date,
# MAGIC                 p.amount AS last_payment_amount
# MAGIC             FROM [sales].[payments] p
# MAGIC             WHERE p.customer_id = o.customer_id
# MAGIC             ORDER BY p.payment_date DESC
# MAGIC         ) latest_payment
# MAGIC         CROSS APPLY (
# MAGIC             SELECT
# MAGIC                 COUNT(*) AS total_items,
# MAGIC                 SUM(ISNULL(li.weight, 0) * li.quantity) AS total_weight
# MAGIC             FROM [sales].[order_lines] li
# MAGIC             WHERE li.order_id = o.order_id
# MAGIC         ) line_summary
# MAGIC         WHERE o.status IN (1, 2);  -- New or Confirmed only
# MAGIC
# MAGIC         IF @dry_run = 0
# MAGIC         BEGIN
# MAGIC             -- Step 2: Update order status with OUTPUT clause
# MAGIC             DECLARE @status_changes TABLE (order_id BIGINT, old_status TINYINT, new_status TINYINT);
# MAGIC
# MAGIC             UPDATE o
# MAGIC             SET o.status = 3,  -- Shipped
# MAGIC                 o.ship_date = @process_date,
# MAGIC                 o.modified_at = SYSDATETIME()
# MAGIC             OUTPUT inserted.order_id, deleted.status, inserted.status
# MAGIC             INTO @status_changes
# MAGIC             FROM [sales].[orders] o
# MAGIC             INNER JOIN #enriched_orders eo ON o.order_id = eo.order_id
# MAGIC             WHERE eo.exceeds_credit = 0;
# MAGIC
# MAGIC             SET @processed_count = @@ROWCOUNT;
# MAGIC
# MAGIC             -- Step 3: Log credit-exceeded orders
# MAGIC             INSERT INTO [sales].[order_holds] (order_id, hold_reason, hold_date, operator_id)
# MAGIC             SELECT order_id, 'Credit limit exceeded', @process_date, @operator_id
# MAGIC             FROM #enriched_orders
# MAGIC             WHERE exceeds_credit = 1;
# MAGIC
# MAGIC             SET @error_count = @@ROWCOUNT;
# MAGIC         END
# MAGIC
# MAGIC         COMMIT TRANSACTION;
# MAGIC
# MAGIC         -- Return summary
# MAGIC         SELECT
# MAGIC             @batch_id AS batch_id,
# MAGIC             @processed_count AS orders_shipped,
# MAGIC             @error_count AS orders_on_hold,
# MAGIC             @process_date AS process_date,
# MAGIC             DATEDIFF(MILLISECOND, SYSDATETIME(), SYSDATETIME()) AS duration_ms;
# MAGIC
# MAGIC     END TRY
# MAGIC     BEGIN CATCH
# MAGIC         IF @@TRANCOUNT > 0
# MAGIC             ROLLBACK TRANSACTION;
# MAGIC
# MAGIC         INSERT INTO [dbo].[error_log] (batch_id, error_number, error_message, error_procedure, error_line, logged_at)
# MAGIC         VALUES (@batch_id, ERROR_NUMBER(), ERROR_MESSAGE(), ERROR_PROCEDURE(), ERROR_LINE(), SYSDATETIME());
# MAGIC
# MAGIC         THROW;
# MAGIC     END CATCH
# MAGIC END;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. SQL Server -- Dynamic Pivot and JSON Processing
# MAGIC T-SQL patterns with `PIVOT`, `FOR JSON`, `OPENJSON`, and dynamic SQL.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- SOURCE: SQL Server -- Sales Pivot Report + JSON Processing
# MAGIC -- CONVERT TO: Databricks SQL
# MAGIC -- ============================================================
# MAGIC
# MAGIC -- Part A: Dynamic PIVOT for monthly sales by category
# MAGIC DECLARE @columns NVARCHAR(MAX) = '';
# MAGIC DECLARE @sql NVARCHAR(MAX);
# MAGIC
# MAGIC SELECT @columns = @columns + QUOTENAME(FORMAT(sale_date, 'yyyy-MM')) + ','
# MAGIC FROM (SELECT DISTINCT FORMAT(sale_date, 'yyyy-MM') AS month_str FROM [dbo].[fact_sales]
# MAGIC       WHERE sale_date >= DATEADD(MONTH, -6, GETDATE())) months
# MAGIC ORDER BY month_str;
# MAGIC
# MAGIC SET @columns = LEFT(@columns, LEN(@columns) - 1);
# MAGIC
# MAGIC SET @sql = N'
# MAGIC SELECT category, ' + @columns + N'
# MAGIC FROM (
# MAGIC     SELECT p.category, FORMAT(s.sale_date, ''yyyy-MM'') AS sale_month, s.total_amount
# MAGIC     FROM [dbo].[fact_sales] s
# MAGIC     INNER JOIN [dbo].[dim_product] p ON s.product_id = p.product_id
# MAGIC ) src
# MAGIC PIVOT (SUM(total_amount) FOR sale_month IN (' + @columns + N')) pvt
# MAGIC ORDER BY category;';
# MAGIC
# MAGIC EXEC sp_executesql @sql;
# MAGIC
# MAGIC -- Part B: JSON processing (parse API response stored as JSON)
# MAGIC SELECT
# MAGIC     o.order_id,
# MAGIC     o.order_date,
# MAGIC     j.item_name,
# MAGIC     j.quantity,
# MAGIC     j.unit_price,
# MAGIC     j.quantity * j.unit_price AS line_total,
# MAGIC     TRY_CONVERT(DECIMAL(10,2), j.discount) AS discount_amount,
# MAGIC     JSON_VALUE(o.shipping_address, '$.city') AS ship_city,
# MAGIC     JSON_VALUE(o.shipping_address, '$.country') AS ship_country,
# MAGIC     ISJSON(o.shipping_address) AS is_valid_json
# MAGIC FROM [sales].[orders] o
# MAGIC CROSS APPLY OPENJSON(o.order_lines_json)
# MAGIC     WITH (
# MAGIC         item_name   NVARCHAR(200)   '$.name',
# MAGIC         quantity     INT             '$.qty',
# MAGIC         unit_price   DECIMAL(10,2)   '$.price',
# MAGIC         discount     NVARCHAR(20)    '$.discount'
# MAGIC     ) j
# MAGIC WHERE o.order_date >= DATEADD(DAY, -30, GETDATE())
# MAGIC   AND ISJSON(o.order_lines_json) = 1;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. SQL Server -- CTE with Recursive Hierarchy + MERGE
# MAGIC Employee hierarchy with recursive CTE and upsert pattern.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- SOURCE: SQL Server -- Recursive Org Hierarchy + MERGE
# MAGIC -- CONVERT TO: Databricks SQL
# MAGIC -- ============================================================
# MAGIC
# MAGIC -- Part A: Recursive CTE for org hierarchy
# MAGIC ;WITH org_hierarchy AS (
# MAGIC     -- Anchor: top-level managers
# MAGIC     SELECT
# MAGIC         employee_id,
# MAGIC         employee_name,
# MAGIC         manager_id,
# MAGIC         department,
# MAGIC         salary,
# MAGIC         CAST(employee_name AS NVARCHAR(MAX)) AS reporting_path,
# MAGIC         0 AS depth,
# MAGIC         CAST(RIGHT('000000' + CAST(employee_id AS VARCHAR(6)), 6) AS VARCHAR(MAX)) AS sort_path
# MAGIC     FROM [hr].[employees]
# MAGIC     WHERE manager_id IS NULL
# MAGIC
# MAGIC     UNION ALL
# MAGIC
# MAGIC     -- Recursive: subordinates
# MAGIC     SELECT
# MAGIC         e.employee_id,
# MAGIC         e.employee_name,
# MAGIC         e.manager_id,
# MAGIC         e.department,
# MAGIC         e.salary,
# MAGIC         h.reporting_path + ' > ' + e.employee_name,
# MAGIC         h.depth + 1,
# MAGIC         h.sort_path + '/' + RIGHT('000000' + CAST(e.employee_id AS VARCHAR(6)), 6)
# MAGIC     FROM [hr].[employees] e
# MAGIC     INNER JOIN org_hierarchy h ON e.manager_id = h.employee_id
# MAGIC ),
# MAGIC team_stats AS (
# MAGIC     SELECT
# MAGIC         h.employee_id,
# MAGIC         h.employee_name,
# MAGIC         h.depth,
# MAGIC         h.reporting_path,
# MAGIC         COUNT(*) OVER (PARTITION BY h.manager_id) AS team_size,
# MAGIC         SUM(h.salary) OVER (PARTITION BY h.manager_id) AS team_salary_total,
# MAGIC         AVG(h.salary) OVER (PARTITION BY h.department) AS dept_avg_salary,
# MAGIC         PERCENT_RANK() OVER (PARTITION BY h.department ORDER BY h.salary) AS salary_percentile
# MAGIC     FROM org_hierarchy h
# MAGIC )
# MAGIC SELECT *,
# MAGIC     REPLICATE('  ', depth) + employee_name AS indented_name,
# MAGIC     IIF(salary > dept_avg_salary, 'Above Average', 'Below Average') AS salary_position,
# MAGIC     FORMAT(salary, 'N0') AS salary_formatted
# MAGIC FROM team_stats
# MAGIC ORDER BY sort_path
# MAGIC OPTION (MAXRECURSION 20);
# MAGIC
# MAGIC -- Part B: MERGE upsert pattern
# MAGIC MERGE [hr].[employee_summary] AS tgt
# MAGIC USING (
# MAGIC     SELECT
# MAGIC         employee_id,
# MAGIC         employee_name,
# MAGIC         department,
# MAGIC         salary,
# MAGIC         SYSDATETIME() AS updated_at
# MAGIC     FROM [hr].[employees]
# MAGIC     WHERE modified_date >= DATEADD(DAY, -1, GETDATE())
# MAGIC ) AS src
# MAGIC ON tgt.employee_id = src.employee_id
# MAGIC WHEN MATCHED AND (tgt.salary <> src.salary OR tgt.department <> src.department) THEN
# MAGIC     UPDATE SET
# MAGIC         tgt.employee_name = src.employee_name,
# MAGIC         tgt.department = src.department,
# MAGIC         tgt.salary = src.salary,
# MAGIC         tgt.updated_at = src.updated_at
# MAGIC WHEN NOT MATCHED BY TARGET THEN
# MAGIC     INSERT (employee_id, employee_name, department, salary, created_at, updated_at)
# MAGIC     VALUES (src.employee_id, src.employee_name, src.department, src.salary, SYSDATETIME(), SYSDATETIME())
# MAGIC WHEN NOT MATCHED BY SOURCE AND tgt.is_active = 1 THEN
# MAGIC     UPDATE SET tgt.is_active = 0, tgt.deactivated_at = SYSDATETIME();

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. SQL Server -- Temporal Tables and System Versioning
# MAGIC SQL Server temporal tables pattern for point-in-time querying.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- SOURCE: SQL Server -- Temporal Table Queries
# MAGIC -- CONVERT TO: Databricks SQL (Delta Lake Time Travel)
# MAGIC -- ============================================================
# MAGIC
# MAGIC -- Create temporal table (system-versioned)
# MAGIC CREATE TABLE [inventory].[product_prices] (
# MAGIC     [product_id]        INT NOT NULL PRIMARY KEY,
# MAGIC     [product_name]      NVARCHAR(200) NOT NULL,
# MAGIC     [unit_price]        DECIMAL(10,2) NOT NULL,
# MAGIC     [cost_price]        DECIMAL(10,2),
# MAGIC     [margin_pct]        AS CAST(([unit_price] - ISNULL([cost_price], 0)) / [unit_price] * 100 AS DECIMAL(5,2)),
# MAGIC     [valid_from]        DATETIME2 GENERATED ALWAYS AS ROW START,
# MAGIC     [valid_to]          DATETIME2 GENERATED ALWAYS AS ROW END,
# MAGIC     PERIOD FOR SYSTEM_TIME ([valid_from], [valid_to])
# MAGIC )
# MAGIC WITH (SYSTEM_VERSIONING = ON (HISTORY_TABLE = [inventory].[product_prices_history]));
# MAGIC
# MAGIC -- Query: What were prices on a specific date?
# MAGIC SELECT
# MAGIC     product_id,
# MAGIC     product_name,
# MAGIC     unit_price,
# MAGIC     cost_price,
# MAGIC     margin_pct,
# MAGIC     valid_from,
# MAGIC     valid_to
# MAGIC FROM [inventory].[product_prices]
# MAGIC FOR SYSTEM_TIME AS OF '2025-01-15T00:00:00'
# MAGIC WHERE unit_price > 100;
# MAGIC
# MAGIC -- Query: Price change history over a period
# MAGIC SELECT
# MAGIC     product_id,
# MAGIC     product_name,
# MAGIC     unit_price,
# MAGIC     valid_from,
# MAGIC     valid_to,
# MAGIC     LAG(unit_price) OVER (PARTITION BY product_id ORDER BY valid_from) AS previous_price,
# MAGIC     unit_price - LAG(unit_price) OVER (PARTITION BY product_id ORDER BY valid_from) AS price_change,
# MAGIC     FORMAT(
# MAGIC         (unit_price - LAG(unit_price) OVER (PARTITION BY product_id ORDER BY valid_from))
# MAGIC         / NULLIF(LAG(unit_price) OVER (PARTITION BY product_id ORDER BY valid_from), 0) * 100,
# MAGIC     'N2') + '%' AS pct_change
# MAGIC FROM [inventory].[product_prices]
# MAGIC FOR SYSTEM_TIME BETWEEN '2024-01-01' AND '2025-12-31'
# MAGIC ORDER BY product_id, valid_from;
