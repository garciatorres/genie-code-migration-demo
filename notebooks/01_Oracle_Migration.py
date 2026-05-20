# Databricks notebook source
# MAGIC %md
# MAGIC # Oracle SQL/PL-SQL to Databricks Migration
# MAGIC ### Genie Code `/migrate` Demo
# MAGIC
# MAGIC This notebook contains **real-world Oracle SQL and PL/SQL patterns** commonly found in enterprise Oracle databases.
# MAGIC
# MAGIC **How to use this demo:**
# MAGIC 1. Select the source code cell below
# MAGIC 2. Open Genie Code panel (sparkle icon) and select **Agent** mode
# MAGIC 3. Type `/migrate` or prompt: *"Convert this Oracle SQL to Databricks SQL"*
# MAGIC 4. Watch Genie Code convert the code live
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Oracle DDL -- Table Creation with Oracle-Specific Types
# MAGIC Oracle uses `NUMBER`, `VARCHAR2`, `DATE`, `CLOB`, tablespace placement, and sequences.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- SOURCE: Oracle 19c -- Customer Data Warehouse
# MAGIC -- CONVERT TO: Databricks SQL (Delta Lake)
# MAGIC -- ============================================================
# MAGIC
# MAGIC CREATE TABLE HR.EMPLOYEES (
# MAGIC     EMPLOYEE_ID     NUMBER(10)       NOT NULL,
# MAGIC     FIRST_NAME      VARCHAR2(50),
# MAGIC     LAST_NAME       VARCHAR2(100)    NOT NULL,
# MAGIC     EMAIL           VARCHAR2(255)    UNIQUE,
# MAGIC     PHONE_NUMBER    VARCHAR2(20),
# MAGIC     HIRE_DATE       DATE             DEFAULT SYSDATE,
# MAGIC     JOB_ID          VARCHAR2(10)     NOT NULL,
# MAGIC     SALARY          NUMBER(12,2),
# MAGIC     COMMISSION_PCT  NUMBER(4,2),
# MAGIC     MANAGER_ID      NUMBER(10),
# MAGIC     DEPARTMENT_ID   NUMBER(10),
# MAGIC     CREATED_AT      TIMESTAMP(6)     DEFAULT SYSTIMESTAMP,
# MAGIC     NOTES           CLOB,
# MAGIC     CONSTRAINT PK_EMPLOYEES PRIMARY KEY (EMPLOYEE_ID),
# MAGIC     CONSTRAINT FK_EMP_DEPT FOREIGN KEY (DEPARTMENT_ID)
# MAGIC         REFERENCES HR.DEPARTMENTS(DEPARTMENT_ID),
# MAGIC     CONSTRAINT CHK_SALARY CHECK (SALARY > 0)
# MAGIC )
# MAGIC TABLESPACE USERS
# MAGIC PCTFREE 10
# MAGIC INITRANS 2;
# MAGIC
# MAGIC -- Oracle sequence for auto-increment
# MAGIC CREATE SEQUENCE HR.SEQ_EMPLOYEE_ID
# MAGIC     START WITH 1000
# MAGIC     INCREMENT BY 1
# MAGIC     NOCACHE
# MAGIC     NOCYCLE;
# MAGIC
# MAGIC -- Oracle index with tablespace
# MAGIC CREATE INDEX IDX_EMP_DEPT ON HR.EMPLOYEES(DEPARTMENT_ID)
# MAGIC     TABLESPACE INDX;
# MAGIC
# MAGIC COMMENT ON TABLE HR.EMPLOYEES IS 'Main employee master table';
# MAGIC COMMENT ON COLUMN HR.EMPLOYEES.COMMISSION_PCT IS 'Commission percentage for sales roles';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Oracle PL/SQL Stored Procedure -- Complex ETL Logic
# MAGIC This procedure calculates monthly payroll with cursor loops, exception handling, and `DBMS_OUTPUT`.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- SOURCE: Oracle PL/SQL -- Monthly Payroll Calculation
# MAGIC -- CONVERT TO: Databricks SQL Scripting or PySpark
# MAGIC -- ============================================================
# MAGIC
# MAGIC CREATE OR REPLACE PROCEDURE HR.CALC_MONTHLY_PAYROLL (
# MAGIC     p_department_id IN NUMBER,
# MAGIC     p_pay_month     IN DATE,
# MAGIC     p_total_cost    OUT NUMBER
# MAGIC ) AS
# MAGIC     v_base_salary    NUMBER(12,2);
# MAGIC     v_bonus          NUMBER(12,2);
# MAGIC     v_tax_rate       NUMBER(5,4) := 0.3;
# MAGIC     v_total          NUMBER(12,2) := 0;
# MAGIC     v_emp_count      NUMBER := 0;
# MAGIC
# MAGIC     CURSOR c_employees IS
# MAGIC         SELECT e.EMPLOYEE_ID, e.SALARY, e.COMMISSION_PCT,
# MAGIC                NVL(e.COMMISSION_PCT, 0) AS comm,
# MAGIC                MONTHS_BETWEEN(SYSDATE, e.HIRE_DATE) AS tenure_months
# MAGIC         FROM HR.EMPLOYEES e
# MAGIC         WHERE e.DEPARTMENT_ID = p_department_id
# MAGIC           AND e.HIRE_DATE <= p_pay_month
# MAGIC         ORDER BY e.SALARY DESC;
# MAGIC
# MAGIC     r_emp c_employees%ROWTYPE;
# MAGIC
# MAGIC BEGIN
# MAGIC     DBMS_OUTPUT.PUT_LINE('Processing payroll for dept: ' || p_department_id);
# MAGIC
# MAGIC     FOR r_emp IN c_employees LOOP
# MAGIC         v_base_salary := r_emp.SALARY;
# MAGIC
# MAGIC         -- Tenure bonus: 2% per year of service
# MAGIC         v_bonus := v_base_salary * (TRUNC(r_emp.tenure_months / 12) * 0.02);
# MAGIC
# MAGIC         -- Commission
# MAGIC         v_bonus := v_bonus + (v_base_salary * r_emp.comm);
# MAGIC
# MAGIC         -- Cap bonus at 50% of base
# MAGIC         v_bonus := LEAST(v_bonus, v_base_salary * 0.5);
# MAGIC
# MAGIC         -- Net after tax
# MAGIC         v_total := v_total + (v_base_salary + v_bonus) * (1 - v_tax_rate);
# MAGIC         v_emp_count := v_emp_count + 1;
# MAGIC
# MAGIC         -- Log to audit table
# MAGIC         INSERT INTO HR.PAYROLL_AUDIT (
# MAGIC             EMPLOYEE_ID, PAY_MONTH, BASE_SALARY, BONUS, TAX_RATE, NET_PAY, PROCESSED_AT
# MAGIC         ) VALUES (
# MAGIC             r_emp.EMPLOYEE_ID, p_pay_month, v_base_salary, v_bonus,
# MAGIC             v_tax_rate, (v_base_salary + v_bonus) * (1 - v_tax_rate), SYSTIMESTAMP
# MAGIC         );
# MAGIC     END LOOP;
# MAGIC
# MAGIC     p_total_cost := v_total;
# MAGIC
# MAGIC     COMMIT;
# MAGIC     DBMS_OUTPUT.PUT_LINE('Processed ' || v_emp_count || ' employees. Total: ' || v_total);
# MAGIC
# MAGIC EXCEPTION
# MAGIC     WHEN NO_DATA_FOUND THEN
# MAGIC         DBMS_OUTPUT.PUT_LINE('No employees found for department ' || p_department_id);
# MAGIC         p_total_cost := 0;
# MAGIC     WHEN OTHERS THEN
# MAGIC         ROLLBACK;
# MAGIC         DBMS_OUTPUT.PUT_LINE('Error: ' || SQLERRM);
# MAGIC         RAISE;
# MAGIC END CALC_MONTHLY_PAYROLL;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Oracle MERGE (Upsert) -- Slowly Changing Dimension Type 2
# MAGIC Classic SCD Type 2 pattern using Oracle's `MERGE` with multiple `WHEN` clauses.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- SOURCE: Oracle -- SCD Type 2 Customer Dimension
# MAGIC -- CONVERT TO: Databricks MERGE INTO (Delta Lake)
# MAGIC -- ============================================================
# MAGIC
# MAGIC MERGE INTO DW.DIM_CUSTOMER tgt
# MAGIC USING (
# MAGIC     SELECT
# MAGIC         s.CUSTOMER_ID,
# MAGIC         s.CUSTOMER_NAME,
# MAGIC         s.ADDRESS,
# MAGIC         s.CITY,
# MAGIC         s.COUNTRY,
# MAGIC         s.CREDIT_LIMIT,
# MAGIC         s.SEGMENT,
# MAGIC         TO_CHAR(SYSDATE, 'YYYY-MM-DD') AS EFFECTIVE_DATE
# MAGIC     FROM STG.STG_CUSTOMERS s
# MAGIC ) src
# MAGIC ON (tgt.CUSTOMER_ID = src.CUSTOMER_ID AND tgt.IS_CURRENT = 'Y')
# MAGIC WHEN MATCHED AND (
# MAGIC     NVL(tgt.CUSTOMER_NAME, '~') != NVL(src.CUSTOMER_NAME, '~') OR
# MAGIC     NVL(tgt.ADDRESS, '~') != NVL(src.ADDRESS, '~') OR
# MAGIC     NVL(tgt.CREDIT_LIMIT, -1) != NVL(src.CREDIT_LIMIT, -1) OR
# MAGIC     NVL(tgt.SEGMENT, '~') != NVL(src.SEGMENT, '~')
# MAGIC ) THEN UPDATE SET
# MAGIC     tgt.IS_CURRENT = 'N',
# MAGIC     tgt.END_DATE = src.EFFECTIVE_DATE,
# MAGIC     tgt.UPDATED_AT = SYSTIMESTAMP
# MAGIC WHEN NOT MATCHED THEN INSERT (
# MAGIC     SURROGATE_KEY, CUSTOMER_ID, CUSTOMER_NAME, ADDRESS, CITY, COUNTRY,
# MAGIC     CREDIT_LIMIT, SEGMENT, IS_CURRENT, START_DATE, END_DATE, CREATED_AT
# MAGIC ) VALUES (
# MAGIC     HR.SEQ_CUSTOMER_SK.NEXTVAL, src.CUSTOMER_ID, src.CUSTOMER_NAME,
# MAGIC     src.ADDRESS, src.CITY, src.COUNTRY, src.CREDIT_LIMIT, src.SEGMENT,
# MAGIC     'Y', src.EFFECTIVE_DATE, TO_DATE('9999-12-31', 'YYYY-MM-DD'), SYSTIMESTAMP
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Oracle Analytical Query -- Window Functions with Oracle Syntax
# MAGIC Oracle-specific analytical functions like `RATIO_TO_REPORT`, `LISTAGG`, `CONNECT BY`, `DECODE`.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- SOURCE: Oracle -- Sales Analytics with Oracle-specific functions
# MAGIC -- CONVERT TO: Databricks SQL
# MAGIC -- ============================================================
# MAGIC
# MAGIC SELECT
# MAGIC     d.DEPARTMENT_NAME,
# MAGIC     e.EMPLOYEE_ID,
# MAGIC     e.FIRST_NAME || ' ' || e.LAST_NAME AS FULL_NAME,
# MAGIC     e.SALARY,
# MAGIC     DECODE(e.JOB_ID,
# MAGIC         'SA_MAN', 'Sales Manager',
# MAGIC         'SA_REP', 'Sales Rep',
# MAGIC         'IT_PROG', 'Developer',
# MAGIC         'Other') AS JOB_TITLE,
# MAGIC     ROUND(RATIO_TO_REPORT(e.SALARY) OVER (PARTITION BY e.DEPARTMENT_ID), 4) AS SALARY_SHARE,
# MAGIC     RANK() OVER (PARTITION BY e.DEPARTMENT_ID ORDER BY e.SALARY DESC) AS SALARY_RANK,
# MAGIC     LISTAGG(e.FIRST_NAME, ', ') WITHIN GROUP (ORDER BY e.FIRST_NAME)
# MAGIC         OVER (PARTITION BY e.DEPARTMENT_ID) AS DEPT_MEMBERS,
# MAGIC     NVL2(e.COMMISSION_PCT, 'Commissioned', 'Salaried') AS PAY_TYPE,
# MAGIC     ADD_MONTHS(e.HIRE_DATE, 12) AS FIRST_REVIEW_DATE,
# MAGIC     TRUNC(MONTHS_BETWEEN(SYSDATE, e.HIRE_DATE) / 12) AS YEARS_OF_SERVICE,
# MAGIC     ROWNUM AS ROW_NUM
# MAGIC FROM HR.EMPLOYEES e
# MAGIC JOIN HR.DEPARTMENTS d ON e.DEPARTMENT_ID = d.DEPARTMENT_ID
# MAGIC WHERE e.SALARY > 5000
# MAGIC   AND e.HIRE_DATE >= ADD_MONTHS(SYSDATE, -60)
# MAGIC ORDER BY d.DEPARTMENT_NAME, e.SALARY DESC
# MAGIC FETCH FIRST 50 ROWS ONLY;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Oracle Hierarchical Query -- CONNECT BY / START WITH
# MAGIC The classic Oracle hierarchical query pattern for org charts.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- SOURCE: Oracle -- Org Chart Hierarchy
# MAGIC -- CONVERT TO: Databricks SQL (Recursive CTE)
# MAGIC -- ============================================================
# MAGIC
# MAGIC SELECT
# MAGIC     LEVEL AS DEPTH,
# MAGIC     EMPLOYEE_ID,
# MAGIC     FIRST_NAME || ' ' || LAST_NAME AS EMPLOYEE_NAME,
# MAGIC     MANAGER_ID,
# MAGIC     LPAD(' ', (LEVEL - 1) * 4) || FIRST_NAME || ' ' || LAST_NAME AS ORG_TREE,
# MAGIC     SYS_CONNECT_BY_PATH(FIRST_NAME, ' -> ') AS REPORTING_PATH,
# MAGIC     CONNECT_BY_ISLEAF AS IS_LEAF,
# MAGIC     CONNECT_BY_ROOT FIRST_NAME AS TOP_MANAGER
# MAGIC FROM HR.EMPLOYEES
# MAGIC START WITH MANAGER_ID IS NULL
# MAGIC CONNECT BY PRIOR EMPLOYEE_ID = MANAGER_ID
# MAGIC ORDER SIBLINGS BY LAST_NAME;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Oracle PL/SQL Package -- Bulk Operations with FORALL
# MAGIC Advanced PL/SQL with bulk collect, FORALL, and associative arrays.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- SOURCE: Oracle PL/SQL Package -- Bulk Data Processing
# MAGIC -- CONVERT TO: PySpark or Databricks SQL Scripting
# MAGIC -- ============================================================
# MAGIC
# MAGIC CREATE OR REPLACE PACKAGE BODY ETL.DATA_LOADER AS
# MAGIC
# MAGIC     PROCEDURE LOAD_DAILY_TRANSACTIONS (
# MAGIC         p_load_date IN DATE
# MAGIC     ) IS
# MAGIC         TYPE t_txn_tab IS TABLE OF STG.RAW_TRANSACTIONS%ROWTYPE;
# MAGIC         v_txn_data t_txn_tab;
# MAGIC
# MAGIC         v_batch_size CONSTANT NUMBER := 10000;
# MAGIC         v_total_rows NUMBER := 0;
# MAGIC     BEGIN
# MAGIC         OPEN cur_transactions FOR
# MAGIC             SELECT * FROM STG.RAW_TRANSACTIONS
# MAGIC             WHERE TXN_DATE = TRUNC(p_load_date)
# MAGIC               AND PROCESSED_FLAG = 'N';
# MAGIC
# MAGIC         LOOP
# MAGIC             FETCH cur_transactions BULK COLLECT INTO v_txn_data LIMIT v_batch_size;
# MAGIC             EXIT WHEN v_txn_data.COUNT = 0;
# MAGIC
# MAGIC             FORALL i IN 1..v_txn_data.COUNT
# MAGIC                 INSERT INTO DW.FACT_TRANSACTIONS (
# MAGIC                     TXN_ID, CUSTOMER_ID, PRODUCT_ID, TXN_AMOUNT,
# MAGIC                     TXN_DATE, TXN_TYPE, CURRENCY_CODE, LOADED_AT
# MAGIC                 ) VALUES (
# MAGIC                     v_txn_data(i).TXN_ID,
# MAGIC                     v_txn_data(i).CUSTOMER_ID,
# MAGIC                     v_txn_data(i).PRODUCT_ID,
# MAGIC                     v_txn_data(i).TXN_AMOUNT,
# MAGIC                     v_txn_data(i).TXN_DATE,
# MAGIC                     v_txn_data(i).TXN_TYPE,
# MAGIC                     v_txn_data(i).CURRENCY_CODE,
# MAGIC                     SYSTIMESTAMP
# MAGIC                 );
# MAGIC
# MAGIC             FORALL i IN 1..v_txn_data.COUNT
# MAGIC                 UPDATE STG.RAW_TRANSACTIONS
# MAGIC                 SET PROCESSED_FLAG = 'Y', PROCESSED_AT = SYSTIMESTAMP
# MAGIC                 WHERE TXN_ID = v_txn_data(i).TXN_ID;
# MAGIC
# MAGIC             v_total_rows := v_total_rows + v_txn_data.COUNT;
# MAGIC             COMMIT;
# MAGIC         END LOOP;
# MAGIC
# MAGIC         CLOSE cur_transactions;
# MAGIC         DBMS_OUTPUT.PUT_LINE('Loaded ' || v_total_rows || ' transactions');
# MAGIC
# MAGIC     EXCEPTION
# MAGIC         WHEN OTHERS THEN
# MAGIC             ROLLBACK;
# MAGIC             INSERT INTO ETL.ERROR_LOG (ERROR_MSG, ERROR_TIME, PROCEDURE_NAME)
# MAGIC             VALUES (SQLERRM, SYSTIMESTAMP, 'LOAD_DAILY_TRANSACTIONS');
# MAGIC             COMMIT;
# MAGIC             RAISE;
# MAGIC     END LOAD_DAILY_TRANSACTIONS;
# MAGIC
# MAGIC END DATA_LOADER;
