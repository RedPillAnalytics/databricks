// Databricks notebook source
// MAGIC %md
// MAGIC We'll start by grabbing credentials for access to Snowflake Data Warehouse

// COMMAND ----------

val user = dbutils.secrets.get("kscope", "snowflake_user")
val password = dbutils.secrets.get("kscope", "snowflake_password")

// COMMAND ----------

// MAGIC %md
// MAGIC Let's configure our Snowflake connections. We're using the same Snowflake database for our SOURCE and TARGET, but different schemas. We are also using the Databricks app **secret** 

// COMMAND ----------

val source = Map( "sfUrl" -> "redpill.snowflakecomputing.com",
                  "sfUser" -> user,
                  "sfPassword" -> password,
                  "sfDatabase" -> "databricks",
                  "sfSchema" -> "trt",
                  "sfWarehouse" -> "dataload" )

val target = Map( "sfUrl" -> "redpill.snowflakecomputing.com",
                  "sfUser" -> user,
                  "sfPassword" -> password,
                  "sfDatabase" -> "databricks",
                  "sfSchema" -> "edw",
                  "sfWarehouse" -> "dataload" )



// COMMAND ----------

// MAGIC %md
// MAGIC Let's create a few dataframes of tables to work with

// COMMAND ----------

import org.apache.spark.sql.DataFrame

val customer: DataFrame = spark.read
  .format("snowflake")
  .options(source)
  .option("dbtable", "customer")
  .load()

customer.write.format("delta").mode("overwrite").save("/delta/customer/")

// COMMAND ----------

// MAGIC %md
// MAGIC Let's create a Delta Lake table to hold this. A Delta lake table has all the 

// COMMAND ----------

// MAGIC %sql
// MAGIC DROP TABLE IF EXISTS customer;
// MAGIC 
// MAGIC CREATE TABLE IF NOT EXISTS customer
// MAGIC USING DELTA
// MAGIC LOCATION '/delta/customer';

// COMMAND ----------

// MAGIC %md
// MAGIC Let's visualize the results:

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT *,
// MAGIC        rank() over (partition by customer_id order by action_ts) customer_rank 
// MAGIC from customer
// MAGIC order by customer_id, customer_rank;

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE table customer_ranked AS
// MAGIC SELECT *,
// MAGIC        rank() over (partition by customer_id order by action_ts) customer_rank 
// MAGIC from customer
// MAGIC where customer_rank = 1;