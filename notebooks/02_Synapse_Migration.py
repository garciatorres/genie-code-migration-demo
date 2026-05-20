# Databricks notebook source
# MAGIC %md
# MAGIC # Azure Synapse T-SQL to Databricks Migration
# MAGIC ### Genie Code `/migrate` Demo
# MAGIC
# MAGIC This notebook contains **real-world Azure Synapse Analytics T-SQL patterns** including
# MAGIC distribution strategies, CTAS, PolyBase, and Synapse-specific syntax.
# MAGIC
# MAGIC **How to use this demo:**
# MAGIC 1. Select the source code cell below
# MAGIC 2. Open Genie Code panel (sparkle icon) and select **Agent** mode
# MAGIC 3. Type `/migrate` or prompt: *"Convert this Synapse T-SQL to Databricks SQL"*
# MAGIC 4. Watch Genie Code convert the code live
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Synapse DDL -- Distributed Tables with Columnstore
# MAGIC Synapse uses `DISTRIBUTION`, `CLUSTERED COLUMNSTORE INDEX`, and `NVARCHAR(MAX)`.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- SOURCE: Azure Synapse Analytics (Dedicated SQL Pool)
# MAGIC -- CONVERT TO: Databricks SQL (Delta Lake)
# MAGIC -- ============================================================
# MAGIC
# MAGIC CREATE TABLE [dbo].[fact_sales] (
# MAGIC     [sale_id]           BIGINT          NOT NULL,
# MAGIC     [customer_id]       INT             NOT NULL,
# MAGIC     [product_id]        INT             NOT NULL,
# MAGIC     [store_id]          SMALLINT        NOT NULL,
# MAGIC     [sale_date]         DATE            NOT NULL,
# MAGIC     [sale_datetime]     DATETIME2(3)    NOT NULL,
# MAGIC     [quantity]          SMALLINT        NOT NULL,
# MAGIC     [unit_price]        DECIMAL(10,2)   NOT NULL,
# MAGIC     [discount_pct]      DECIMAL(5,4)    DEFAULT 0,
# MAGIC     [total_amount]      MONEY           NOT NULL,
# MAGIC     [tax_amount]        SMALLMONEY,
# MAGIC     [payment_method]    NVARCHAR(50),
# MAGIC     [is_online]         BIT             DEFAULT 0,
# MAGIC     [notes]             NVARCHAR(MAX),
# MAGIC     [row_hash]          VARBINARY(32),
# MAGIC     [etl_load_id]       UNIQUEIDENTIFIER DEFAULT NEWID()
# MAGIC )
# MAGIC WITH (
# MAGIC     DISTRIBUTION = HASH([customer_id]),
# MAGIC     CLUSTERED COLUMNSTORE INDEX,
# MAGIC     PARTITION ([sale_date] RANGE RIGHT FOR VALUES (
# MAGIC         '2024-01-01', '2024-04-01', '2024-07-01', '2024-10-01',
# MAGIC         '2025-01-01', '2025-04-01', '2025-07-01', '2025-10-01'
# MAGIC     ))
# MAGIC );
# MAGIC
# MAGIC CREATE TABLE [dbo].[dim_customer] (
# MAGIC     [customer_sk]       INT IDENTITY(1,1)   NOT NULL,
# MAGIC     [customer_id]       INT                 NOT NULL,
# MAGIC     [full_name]         NVARCHAR(200)       NOT NULL,
# MAGIC     [email]             NVARCHAR(255),
# MAGIC     [date_of_birth]     DATE,
# MAGIC     [credit_score]      SMALLINT,
# MAGIC     [risk_segment]      NVARCHAR(20),
# MAGIC     [customer_since]    DATE,
# MAGIC     [is_active]         BIT                 DEFAULT 1,
# MAGIC     [valid_from]        DATETIME2           DEFAULT GETDATE(),
# MAGIC     [valid_to]          DATETIME2           DEFAULT '9999-12-31',
# MAGIC     [is_current]        BIT                 DEFAULT 1
# MAGIC )
# MAGIC WITH (
# MAGIC     DISTRIBUTION = REPLICATE,
# MAGIC     CLUSTERED INDEX ([customer_sk])
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Synapse CTAS -- Create Table As Select with Distribution
# MAGIC The signature Synapse pattern: rebuild tables with optimal distribution.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- SOURCE: Synapse CTAS -- Rebuild Fact Table
# MAGIC -- CONVERT TO: Databricks CREATE OR REPLACE TABLE ... AS SELECT
# MAGIC -- ============================================================
# MAGIC
# MAGIC CREATE TABLE [dbo].[fact_sales_optimized]
# MAGIC WITH (
# MAGIC     DISTRIBUTION = HASH([customer_id]),
# MAGIC     CLUSTERED COLUMNSTORE INDEX,
# MAGIC     PARTITION ([sale_date] RANGE RIGHT FOR VALUES (
# MAGIC         '2025-01-01', '2025-04-01', '2025-07-01', '2025-10-01'
# MAGIC     ))
# MAGIC )
# MAGIC AS
# MAGIC SELECT
# MAGIC     s.[sale_id],
# MAGIC     s.[customer_id],
# MAGIC     s.[product_id],
# MAGIC     s.[store_id],
# MAGIC     s.[sale_date],
# MAGIC     s.[quantity],
# MAGIC     s.[unit_price],
# MAGIC     s.[discount_pct],
# MAGIC     CAST(s.[total_amount] AS DECIMAL(18,2)) AS [total_amount],
# MAGIC     CAST(s.[tax_amount] AS DECIMAL(10,2)) AS [tax_amount],
# MAGIC     s.[payment_method],
# MAGIC     s.[is_online],
# MAGIC     c.[full_name] AS [customer_name],
# MAGIC     c.[risk_segment],
# MAGIC     p.[product_name],
# MAGIC     p.[category],
# MAGIC     DATEDIFF(DAY, c.[customer_since], s.[sale_date]) AS [customer_tenure_days],
# MAGIC     IIF(s.[is_online] = 1, 'Online', 'In-Store') AS [channel],
# MAGIC     FORMAT(s.[sale_date], 'yyyy-MM') AS [sale_month],
# MAGIC     EOMONTH(s.[sale_date]) AS [month_end_date],
# MAGIC     DATEPART(WEEKDAY, s.[sale_date]) AS [day_of_week],
# MAGIC     CONVERT(VARCHAR(10), s.[sale_date], 120) AS [sale_date_str]
# MAGIC FROM [dbo].[fact_sales] s
# MAGIC INNER JOIN [dbo].[dim_customer] c ON s.[customer_id] = c.[customer_id] AND c.[is_current] = 1
# MAGIC INNER JOIN [dbo].[dim_product] p ON s.[product_id] = p.[product_id]
# MAGIC WHERE s.[sale_date] >= '2025-01-01'
# MAGIC   AND s.[total_amount] > 0;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Synapse Stored Procedure -- ETL with Temp Tables and Error Handling
# MAGIC Classic Synapse ETL pattern with `#temp` tables, `@@ROWCOUNT`, and `RAISERROR`.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- SOURCE: Synapse Stored Procedure -- Daily ETL Pipeline
# MAGIC -- CONVERT TO: Databricks SQL Scripting or Python notebook
# MAGIC -- ============================================================
# MAGIC
# MAGIC CREATE PROCEDURE [dbo].[sp_daily_sales_etl]
# MAGIC     @load_date DATE = NULL,
# MAGIC     @debug_mode BIT = 0
# MAGIC AS
# MAGIC BEGIN
# MAGIC     SET NOCOUNT ON;
# MAGIC
# MAGIC     DECLARE @start_time DATETIME2 = GETDATE();
# MAGIC     DECLARE @row_count INT;
# MAGIC     DECLARE @error_msg NVARCHAR(4000);
# MAGIC
# MAGIC     IF @load_date IS NULL
# MAGIC         SET @load_date = CAST(DATEADD(DAY, -1, GETDATE()) AS DATE);
# MAGIC
# MAGIC     IF @debug_mode = 1
# MAGIC         PRINT 'Starting ETL for date: ' + CONVERT(VARCHAR(10), @load_date, 120);
# MAGIC
# MAGIC     BEGIN TRY
# MAGIC         -- Step 1: Stage raw data into temp table
# MAGIC         IF OBJECT_ID('tempdb..#stg_sales') IS NOT NULL
# MAGIC             DROP TABLE #stg_sales;
# MAGIC
# MAGIC         CREATE TABLE #stg_sales
# MAGIC         WITH (DISTRIBUTION = HASH(customer_id))
# MAGIC         AS
# MAGIC         SELECT
# MAGIC             s.*,
# MAGIC             ISNULL(c.risk_segment, 'Unknown') AS risk_segment,
# MAGIC             ISNULL(c.credit_score, 0) AS credit_score,
# MAGIC             COALESCE(p.category, 'Uncategorized') AS product_category,
# MAGIC             IIF(s.total_amount > 1000, 'High Value', 'Standard') AS txn_tier,
# MAGIC             DATEDIFF(MONTH, c.customer_since, s.sale_date) AS customer_months,
# MAGIC             ROW_NUMBER() OVER (PARTITION BY s.customer_id ORDER BY s.sale_datetime DESC) AS rn
# MAGIC         FROM [dbo].[raw_sales] s
# MAGIC         LEFT JOIN [dbo].[dim_customer] c ON s.customer_id = c.customer_id AND c.is_current = 1
# MAGIC         LEFT JOIN [dbo].[dim_product] p ON s.product_id = p.product_id
# MAGIC         WHERE s.sale_date = @load_date
# MAGIC           AND s.total_amount > 0
# MAGIC           AND TRY_CAST(s.sale_id AS BIGINT) IS NOT NULL;
# MAGIC
# MAGIC         SET @row_count = @@ROWCOUNT;
# MAGIC
# MAGIC         IF @debug_mode = 1
# MAGIC             PRINT 'Staged ' + CAST(@row_count AS VARCHAR(20)) + ' rows';
# MAGIC
# MAGIC         IF @row_count = 0
# MAGIC         BEGIN
# MAGIC             RAISERROR('No data found for date %s', 16, 1, @load_date);
# MAGIC             RETURN;
# MAGIC         END
# MAGIC
# MAGIC         -- Step 2: Deduplicate
# MAGIC         DELETE FROM #stg_sales WHERE rn > 1;
# MAGIC
# MAGIC         -- Step 3: Merge into target
# MAGIC         MERGE [dbo].[fact_sales_daily] AS tgt
# MAGIC         USING #stg_sales AS src
# MAGIC         ON tgt.sale_id = src.sale_id
# MAGIC         WHEN MATCHED THEN
# MAGIC             UPDATE SET
# MAGIC                 tgt.total_amount = src.total_amount,
# MAGIC                 tgt.risk_segment = src.risk_segment,
# MAGIC                 tgt.txn_tier = src.txn_tier,
# MAGIC                 tgt.updated_at = GETDATE()
# MAGIC         WHEN NOT MATCHED THEN
# MAGIC             INSERT (sale_id, customer_id, product_id, sale_date, total_amount,
# MAGIC                     risk_segment, txn_tier, product_category, created_at)
# MAGIC             VALUES (src.sale_id, src.customer_id, src.product_id, src.sale_date,
# MAGIC                     src.total_amount, src.risk_segment, src.txn_tier,
# MAGIC                     src.product_category, GETDATE());
# MAGIC
# MAGIC         -- Step 4: Update watermark
# MAGIC         UPDATE [dbo].[etl_watermark]
# MAGIC         SET last_load_date = @load_date,
# MAGIC             last_load_time = GETDATE(),
# MAGIC             rows_processed = @row_count
# MAGIC         WHERE pipeline_name = 'daily_sales_etl';
# MAGIC
# MAGIC         -- Step 5: Log success
# MAGIC         INSERT INTO [dbo].[etl_log] (pipeline_name, load_date, status, rows_processed, duration_sec, logged_at)
# MAGIC         VALUES ('daily_sales_etl', @load_date, 'SUCCESS', @row_count,
# MAGIC                 DATEDIFF(SECOND, @start_time, GETDATE()), GETDATE());
# MAGIC
# MAGIC     END TRY
# MAGIC     BEGIN CATCH
# MAGIC         SET @error_msg = ERROR_MESSAGE();
# MAGIC
# MAGIC         INSERT INTO [dbo].[etl_log] (pipeline_name, load_date, status, error_message, logged_at)
# MAGIC         VALUES ('daily_sales_etl', @load_date, 'FAILED', @error_msg, GETDATE());
# MAGIC
# MAGIC         THROW;
# MAGIC     END CATCH
# MAGIC END;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Synapse PolyBase External Table -- Data Lake Ingestion
# MAGIC Reading from ADLS Gen2 via PolyBase external tables.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- SOURCE: Synapse PolyBase -- External Table on ADLS Gen2
# MAGIC -- CONVERT TO: Databricks Auto Loader / External Location
# MAGIC -- ============================================================
# MAGIC
# MAGIC -- External data source
# MAGIC CREATE EXTERNAL DATA SOURCE [adls_raw_data]
# MAGIC WITH (
# MAGIC     TYPE = HADOOP,
# MAGIC     LOCATION = 'abfss://raw@datalakestatnett.dfs.core.windows.net',
# MAGIC     CREDENTIAL = [adls_managed_identity]
# MAGIC );
# MAGIC
# MAGIC -- External file format
# MAGIC CREATE EXTERNAL FILE FORMAT [parquet_format]
# MAGIC WITH (
# MAGIC     FORMAT_TYPE = PARQUET,
# MAGIC     DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
# MAGIC );
# MAGIC
# MAGIC -- External table
# MAGIC CREATE EXTERNAL TABLE [staging].[ext_daily_transactions] (
# MAGIC     [transaction_id]    BIGINT,
# MAGIC     [account_id]        INT,
# MAGIC     [transaction_date]  DATE,
# MAGIC     [amount]            DECIMAL(18,2),
# MAGIC     [currency]          NVARCHAR(3),
# MAGIC     [description]       NVARCHAR(500),
# MAGIC     [category]          NVARCHAR(50),
# MAGIC     [is_reconciled]     BIT
# MAGIC )
# MAGIC WITH (
# MAGIC     LOCATION = '/transactions/daily/',
# MAGIC     DATA_SOURCE = [adls_raw_data],
# MAGIC     FILE_FORMAT = [parquet_format],
# MAGIC     REJECT_TYPE = VALUE,
# MAGIC     REJECT_VALUE = 10
# MAGIC );
# MAGIC
# MAGIC -- CTAS from external table into managed table
# MAGIC CREATE TABLE [dbo].[stg_daily_transactions]
# MAGIC WITH (
# MAGIC     DISTRIBUTION = HASH([account_id]),
# MAGIC     CLUSTERED COLUMNSTORE INDEX
# MAGIC )
# MAGIC AS
# MAGIC SELECT
# MAGIC     [transaction_id],
# MAGIC     [account_id],
# MAGIC     [transaction_date],
# MAGIC     [amount],
# MAGIC     [currency],
# MAGIC     ISNULL([description], 'N/A') AS [description],
# MAGIC     ISNULL([category], 'Uncategorized') AS [category],
# MAGIC     ISNULL([is_reconciled], 0) AS [is_reconciled],
# MAGIC     GETDATE() AS [loaded_at]
# MAGIC FROM [staging].[ext_daily_transactions]
# MAGIC WHERE [transaction_date] >= DATEADD(DAY, -7, GETDATE());

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Synapse Analytics Query -- Window Functions and STRING_AGG
# MAGIC Complex analytical query with Synapse-specific patterns.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- SOURCE: Synapse Analytics -- Customer RFM Segmentation
# MAGIC -- CONVERT TO: Databricks SQL
# MAGIC -- ============================================================
# MAGIC
# MAGIC WITH customer_metrics AS (
# MAGIC     SELECT
# MAGIC         c.customer_id,
# MAGIC         c.full_name,
# MAGIC         c.risk_segment,
# MAGIC         COUNT(DISTINCT s.sale_id) AS total_orders,
# MAGIC         SUM(s.total_amount) AS total_revenue,
# MAGIC         AVG(CAST(s.total_amount AS FLOAT)) AS avg_order_value,
# MAGIC         DATEDIFF(DAY, MAX(s.sale_date), GETDATE()) AS days_since_last_order,
# MAGIC         DATEDIFF(DAY, MIN(s.sale_date), MAX(s.sale_date)) AS customer_lifespan_days,
# MAGIC         COUNT(DISTINCT FORMAT(s.sale_date, 'yyyy-MM')) AS active_months,
# MAGIC         STRING_AGG(DISTINCT p.category, ', ') WITHIN GROUP (ORDER BY p.category) AS purchased_categories,
# MAGIC         PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY s.total_amount) OVER (PARTITION BY c.risk_segment) AS median_order_by_segment
# MAGIC     FROM [dbo].[dim_customer] c
# MAGIC     INNER JOIN [dbo].[fact_sales] s ON c.customer_id = s.customer_id
# MAGIC     INNER JOIN [dbo].[dim_product] p ON s.product_id = p.product_id
# MAGIC     WHERE c.is_current = 1
# MAGIC       AND s.sale_date >= DATEADD(YEAR, -2, GETDATE())
# MAGIC     GROUP BY c.customer_id, c.full_name, c.risk_segment
# MAGIC ),
# MAGIC rfm_scores AS (
# MAGIC     SELECT *,
# MAGIC         NTILE(5) OVER (ORDER BY days_since_last_order ASC) AS recency_score,
# MAGIC         NTILE(5) OVER (ORDER BY total_orders DESC) AS frequency_score,
# MAGIC         NTILE(5) OVER (ORDER BY total_revenue DESC) AS monetary_score,
# MAGIC         IIF(days_since_last_order <= 30, 'Active',
# MAGIC             IIF(days_since_last_order <= 90, 'Warm',
# MAGIC                 IIF(days_since_last_order <= 180, 'Cooling', 'Dormant')
# MAGIC             )
# MAGIC         ) AS activity_status
# MAGIC     FROM customer_metrics
# MAGIC )
# MAGIC SELECT TOP 100
# MAGIC     customer_id,
# MAGIC     full_name,
# MAGIC     risk_segment,
# MAGIC     total_orders,
# MAGIC     FORMAT(total_revenue, 'N2') AS total_revenue_fmt,
# MAGIC     FORMAT(avg_order_value, 'N2') AS avg_order_fmt,
# MAGIC     activity_status,
# MAGIC     recency_score,
# MAGIC     frequency_score,
# MAGIC     monetary_score,
# MAGIC     (recency_score + frequency_score + monetary_score) AS rfm_total,
# MAGIC     purchased_categories,
# MAGIC     IIF((recency_score + frequency_score + monetary_score) >= 12, 'Champion',
# MAGIC         IIF((recency_score + frequency_score + monetary_score) >= 9, 'Loyal',
# MAGIC             IIF((recency_score + frequency_score + monetary_score) >= 6, 'Potential', 'At Risk')
# MAGIC         )
# MAGIC     ) AS rfm_segment
# MAGIC FROM rfm_scores
# MAGIC ORDER BY rfm_total DESC, total_revenue DESC;
