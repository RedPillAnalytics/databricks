// Databricks notebook source
// MAGIC %md
// MAGIC 
// MAGIC ## Overview
// MAGIC 
// MAGIC We are using a Databricks account provisioned in AWS, so all of our filesystems are in S3. We are using the built-in Databricks File System (DBFS) filesystem, which is simply a bucket configured when Databricks was provisioned.
// MAGIC 
// MAGIC ## Company
// MAGIC 
// MAGIC Imagine you are a Brokerage company and you would like to start using Databricks to build a Sales-based data warehouse. We have things like Trades, Customers, and Employees.
// MAGIC 
// MAGIC ## Source Data
// MAGIC Our trading system is not very modern, but we've written an extraction job that delivers CSV files to S3 buckets. In the code samples below, we'll be demonstrating using Python, Scala and SQL. We specify the language using the `%LANGUAGE` syntax. Python, Scala, SQL, and R are all supported, as is Markdown (`%md`) for documentation purposes.
// MAGIC 
// MAGIC The first file we're bringing in is the `TRADE` table.

// COMMAND ----------

// MAGIC %python
// MAGIC # File location and type
// MAGIC file_location = "/FileStore/tables/brokerage_trade.csv"
// MAGIC file_type = "csv"
// MAGIC 
// MAGIC # CSV options
// MAGIC infer_schema = "true"
// MAGIC first_row_is_header = "true"
// MAGIC delimiter = ","
// MAGIC 
// MAGIC # The applied options are for CSV files. For other file types, these will be ignored.
// MAGIC trade = spark.read.format(file_type) \
// MAGIC   .option("inferSchema", infer_schema) \
// MAGIC   .option("header", first_row_is_header) \
// MAGIC   .option("sep", delimiter) \
// MAGIC   .load(file_location)
// MAGIC 
// MAGIC display(trade)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## Delta Lake Table
// MAGIC 
// MAGIC We want to land our source data in Databricks using Delta Lake. I can do this using the dataframe I just created. I'm still using Python.

// COMMAND ----------

// MAGIC %python
// MAGIC trade.write.format("delta").mode("overwrite").save('/delta/trade')

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## Now Let's Start Using SQL!
// MAGIC 
// MAGIC We can use the `%sql` magic to work in the world's favorite language.

// COMMAND ----------

// MAGIC %sql
// MAGIC DROP TABLE IF EXISTS trade;
// MAGIC 
// MAGIC CREATE TABLE trade
// MAGIC USING DELTA
// MAGIC LOCATION '/delta/trade';
// MAGIC 
// MAGIC select * from trade;

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## Normalized Model
// MAGIC 
// MAGIC Since our source model is normalized, we need to bring in a few foreign key relationship tables, including `TRADE_TYPE` and `STATUS_TYPE`.

// COMMAND ----------

// MAGIC %python
// MAGIC file_location = "/FileStore/tables/brokerage_status_type.csv"
// MAGIC 
// MAGIC status_type = spark.read.format(file_type) \
// MAGIC   .option("inferSchema", infer_schema) \
// MAGIC   .option("header", first_row_is_header) \
// MAGIC   .option("sep", delimiter) \
// MAGIC   .load(file_location)
// MAGIC 
// MAGIC status_type.write.format("delta").mode("overwrite").save('/delta/status_type')
// MAGIC 
// MAGIC file_location = "/FileStore/tables/brokerage_trade_type.csv"
// MAGIC trade_type = spark.read.format(file_type) \
// MAGIC   .option("inferSchema", infer_schema) \
// MAGIC   .option("header", first_row_is_header) \
// MAGIC   .option("sep", delimiter) \
// MAGIC   .load(file_location)
// MAGIC 
// MAGIC trade_type.write.format("delta").mode("overwrite").save('/delta/trade_type')

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## JSON Data
// MAGIC 
// MAGIC Our HR system is modern, and we are able to get a feed in JSON format.

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC # File location and type
// MAGIC file_location = "/FileStore/tables/brokerage_hr.json"
// MAGIC file_type = "json"
// MAGIC 
// MAGIC # CSV options
// MAGIC infer_schema = "true"
// MAGIC first_row_is_header = "false"
// MAGIC delimiter = ","
// MAGIC 
// MAGIC # The applied options are for CSV files. For other file types, these will be ignored.
// MAGIC employee = spark.read.format(file_type) \
// MAGIC   .option("inferSchema", infer_schema) \
// MAGIC   .option("header", first_row_is_header) \
// MAGIC   .option("sep", delimiter) \
// MAGIC   .load(file_location)
// MAGIC 
// MAGIC display(employee)
// MAGIC 
// MAGIC employee.write.format("delta").mode("overwrite").save('/delta/employee')

// COMMAND ----------

// MAGIC %sql
// MAGIC drop table if exists status_type;
// MAGIC drop table if exists trade_type;
// MAGIC drop table if exists employee;
// MAGIC 
// MAGIC CREATE TABLE status_type
// MAGIC USING DELTA
// MAGIC LOCATION '/delta/status_type';
// MAGIC 
// MAGIC CREATE TABLE trade_type
// MAGIC USING DELTA
// MAGIC LOCATION '/delta/trade_type';
// MAGIC 
// MAGIC CREATE TABLE employee
// MAGIC USING DELTA
// MAGIC LOCATION '/delta/employee';

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## Inline Visualizations
// MAGIC 
// MAGIC We have all of our source data in Delta Lake as queryable tables. We can write a series of select statements to ensure the data looks reasonable. This helps in data mapping explanations, documentation, and regression testing when we productionize the notebook.

// COMMAND ----------

// MAGIC %sql
// MAGIC select *, 
// MAGIC        month(t.trade_dt) as month
// MAGIC from trade t 
// MAGIC join trade_type tt  
// MAGIC   on t.trade_type = tt.trade_id
// MAGIC join status_type st
// MAGIC   on t.status_type = st.st_id;

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## Breadcrumbs
// MAGIC 
// MAGIC We can put full table queries, which eliminates the need to use a SQL editor to to look for column names while structuring our `CREATE TABLE as SELECT...` statement.

// COMMAND ----------

// MAGIC %sql
// MAGIC select 
// MAGIC   *
// MAGIC from trade t 
// MAGIC join trade_type tt  
// MAGIC   on t.trade_type = tt.trade_id
// MAGIC join status_type st
// MAGIC   on t.status_type = st.st_id;

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## Creating a Delta Lake Copy of Our Fact Table
// MAGIC 
// MAGIC We can now use basic SQL to load our `TRADE_FACT` table.

// COMMAND ----------

// MAGIC %sql
// MAGIC DROP table if exists trade_fact;
// MAGIC 
// MAGIC CREATE table trade_fact
// MAGIC USING DELTA
// MAGIC as
// MAGIC SELECT
// MAGIC   TRADE_DT,
// MAGIC   QUANTITY,
// MAGIC   TAX,
// MAGIC   SECURITY_SYMBOL,
// MAGIC   EXECUTOR_ID,
// MAGIC   IS_CASH,
// MAGIC   BID_PRICE,
// MAGIC   TRADE_TYPE,
// MAGIC   FEES,
// MAGIC   CUSTOMER_ACCOUNT_ID,
// MAGIC   COMMISSION,
// MAGIC   TRADE_PRICE,
// MAGIC   T.TRADE_ID,
// MAGIC   STATUS_TYPE,
// MAGIC   TRADE_NAME,
// MAGIC   IS_MRKT,
// MAGIC   IS_SELL,
// MAGIC   ST_ID,
// MAGIC   ST_NAME
// MAGIC from trade t 
// MAGIC join trade_type tt  
// MAGIC   on t.trade_type = tt.trade_id
// MAGIC join status_type st
// MAGIC   on t.status_type = st.st_id;
// MAGIC   
// MAGIC SELECT * from trade_fact;

// COMMAND ----------

// MAGIC %sql
// MAGIC select trade_type, status_type, count(*) as quantity from trade_fact group by trade_type, status_type;

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## Loading Snowflake
// MAGIC 
// MAGIC Databricks has native support for Snowflake as a source or a target database. Our Snowflake credentials are stored in a built-in, secur secrets store, and accessible from any notebook.

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC val user = dbutils.secrets.get("kscope", "snowflake_user")
// MAGIC val password = dbutils.secrets.get("kscope", "snowflake_password")
// MAGIC 
// MAGIC 
// MAGIC val target = Map( "sfUrl" -> "redpill.snowflakecomputing.com",
// MAGIC                   "sfUser" -> user,
// MAGIC                   "sfPassword" -> password,
// MAGIC                   "sfDatabase" -> "databricks",
// MAGIC                   "sfSchema" -> "edw",
// MAGIC                   "sfWarehouse" -> "dataload" )
// MAGIC 
// MAGIC val trade = spark.table("trade_fact")
// MAGIC 
// MAGIC trade
// MAGIC .write.format("snowflake")
// MAGIC .options(target)
// MAGIC .option("dbtable", "TRADE_FACT")
// MAGIC .mode("append")
// MAGIC .save()
// MAGIC 
// MAGIC val employee = spark.table("employee")
// MAGIC 
// MAGIC employee
// MAGIC .drop("_FIVETRAN_ID")
// MAGIC .drop("_FIVETRAN_DELETED")
// MAGIC .drop("_FIVETRAN_SYNCED")
// MAGIC .write.format("snowflake")
// MAGIC .options(target)
// MAGIC .option("dbtable", "EMPLOYEE")
// MAGIC .mode("append")
// MAGIC .save()

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ## Reading from Snowflake
// MAGIC 
// MAGIC Of course we can read from Snowflake as well. We'll demonstrate that while testing the read was successful.

// COMMAND ----------

// MAGIC %scala
// MAGIC 
// MAGIC import org.apache.spark.sql.DataFrame
// MAGIC 
// MAGIC val query: DataFrame = spark.read
// MAGIC   .format("snowflake")
// MAGIC   .options(target)
// MAGIC   .option("query", "select *, extract(month from trade_dt) as month from employee e join trade_fact t on e.employee_id=t.executor_id")
// MAGIC   .load()
// MAGIC 
// MAGIC display(query)