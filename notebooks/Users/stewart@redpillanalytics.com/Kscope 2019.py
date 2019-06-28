# Databricks notebook source
# MAGIC %md
# MAGIC #### We're storing our Snowflake username and password as *secrets* in a Databricks *scope* that I called "kscope". Notice when I execute, Databricks redacts these variables for security sake:

# COMMAND ----------

# MAGIC %scala
# MAGIC val user = dbutils.secrets.get("kscope", "snowflake_user")
# MAGIC val password = dbutils.secrets.get("kscope", "snowflake_password")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Let's configure our Snowflake connections. We're using the same Snowflake database for our SOURCE and TARGET, but different schemas. Notice again our secrets are redacted:

# COMMAND ----------

# MAGIC %scala
# MAGIC val source = Map( "sfUrl" -> "redpill.snowflakecomputing.com",
# MAGIC                   "sfUser" -> user,
# MAGIC                   "sfPassword" -> password,
# MAGIC                   "sfDatabase" -> "databricks",
# MAGIC                   "sfSchema" -> "trt",
# MAGIC                   "sfWarehouse" -> "dataload" )
# MAGIC 
# MAGIC val target = Map( "sfUrl" -> "redpill.snowflakecomputing.com",
# MAGIC                   "sfUser" -> user,
# MAGIC                   "sfPassword" -> password,
# MAGIC                   "sfDatabase" -> "databricks",
# MAGIC                   "sfSchema" -> "edw",
# MAGIC                   "sfWarehouse" -> "dataload" )

# COMMAND ----------

# MAGIC %md
# MAGIC #### We are replicated tables to Snowflake using **Fivetran**, a cloud-based replication service. So I'll pull my source table **CUSTOMER** from Snowflake into Databricks Delta Lake:

# COMMAND ----------

# MAGIC %scala
# MAGIC import org.apache.spark.sql.DataFrame
# MAGIC 
# MAGIC val customer: DataFrame = spark.read
# MAGIC   .format("snowflake")
# MAGIC   .options(source)
# MAGIC   .option("dbtable", "customer")
# MAGIC   .load()
# MAGIC 
# MAGIC customer.write.format("delta").mode("overwrite").save("/delta/customer/")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Once we've loaded our files into Delta Lake, we can use Spark SQL. We'll create a structured delta lake table to hold our intermediate customer table:

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS customer;
# MAGIC 
# MAGIC CREATE TABLE customer
# MAGIC USING DELTA
# MAGIC LOCATION '/delta/customer';

# COMMAND ----------

# MAGIC %md
# MAGIC #### Let's visualize the results. Notice that our customer source data has numerous checkpoint records that we don't much care about. So let's just get the first one:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT customer_id,
# MAGIC        rank() over (partition by customer_id order by action_ts) customer_rank,
# MAGIC        count(*) customer_count
# MAGIC from customer
# MAGIC group by customer_id, action_ts
# MAGIC order by customer_id, customer_rank;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Let's create a persistent staging table:

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP table if exists customer_stage;
# MAGIC 
# MAGIC CREATE table customer_stage 
# MAGIC using delta
# MAGIC AS
# MAGIC SELECT *,
# MAGIC        rank() over (partition by customer_id order by action_ts) CUSTOMER_RANK,
# MAGIC        CASE ACCOUNT_TAX_STATUS
# MAGIC           WHEN 2 then 'Charity'
# MAGIC           WHEN 0 then 'Exempt'
# MAGIC           ELSE 'Non-exempt'
# MAGIC        END TAX_STATUS
# MAGIC FROM customer;
# MAGIC 
# MAGIC DELETE FROM customer_stage where customer_rank <> 1;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Let's visualize results again:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT tax_status, customer_rank, count(*) customer_cnt
# MAGIC from customer_stage
# MAGIC group by tax_status, customer_rank;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Let's use Scala again to demonstrate it's interoperability. Let's remove unnecessary columns and then load the results to Snowflake:

# COMMAND ----------

# MAGIC %scala
# MAGIC val d_customer = spark.table("customer_stage")
# MAGIC 
# MAGIC d_customer
# MAGIC .drop("ACCOUNT_TAX_STATUS")
# MAGIC .drop("_FIVETRAN_ID")
# MAGIC .drop("_FIVETRAN_DELETED")
# MAGIC .drop("_FIVETRAN_SYNCED")
# MAGIC .write.format("snowflake")
# MAGIC .options(target)
# MAGIC .option("dbtable", "D_CUSTOMER")
# MAGIC .mode("append")
# MAGIC .save()