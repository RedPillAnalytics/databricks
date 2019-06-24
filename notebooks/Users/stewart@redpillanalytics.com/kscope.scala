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
// MAGIC Let's now create a structured delta lake table to hold our intermediate customer table:

// COMMAND ----------

// MAGIC %sql
// MAGIC DROP TABLE IF EXISTS customer;
// MAGIC 
// MAGIC CREATE TABLE customer
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
// MAGIC DROP table if exists customer_transformed;
// MAGIC 
// MAGIC CREATE table customer_transformed 
// MAGIC using delta
// MAGIC location '/delta/customer_transformed'
// MAGIC AS
// MAGIC SELECT *,
// MAGIC        rank() over (partition by customer_id order by action_ts) CUSTOMER_RANK,
// MAGIC        CASE ACCOUNT_TAX_STATUS
// MAGIC           WHEN 2 then 'Charity'
// MAGIC           WHEN 0 then 'Exempt'
// MAGIC           ELSE 'Non-exempt'
// MAGIC           END TAX_STATUS
// MAGIC from customer;
// MAGIC 
// MAGIC DELETE FROM customer_transformed where customer_rank <> 1;

// COMMAND ----------

// MAGIC %md
// MAGIC Let's visualize results again:

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT * from customer_transformed;

// COMMAND ----------

// MAGIC %md
// MAGIC Let's use Scala again to demonstrate it's interoperability. Let's drop a few unneeded columns:

// COMMAND ----------

val customer_stage = spark.read.format("delta").load("/delta/customer_transformed")

customer_stage
.drop("ACCOUNT_TAX_STATUS")
.drop("_FIVETRAN_ID")
.drop("_FIVETRAN_DELETED")
.drop("_FIVETRAN_SYNCED")
.write.format("snowflake")
.options(target)
.option("dbtable", "D_CUSTOMER")
.mode("append")
.save()