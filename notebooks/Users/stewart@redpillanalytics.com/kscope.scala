// Databricks notebook source
// MAGIC %md
// MAGIC We'll start by grabbing credentials for access to Snowflake Data Warehouse

// COMMAND ----------

val user = dbutils.secrets.get("kscope", "snowflake_user")
val password = dbutils.secrets.get("kscope", "snowflake_password")

// COMMAND ----------

// MAGIC %md
// MAGIC Let's configure our Snowflake connections. We're using the same Snowflake database for our SOURCE and TARGET, but different schemas.

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

val trade: DataFrame = spark.read
  .format("snowflake")
  .options(source)
  .option("dbtable", "trade")
  .load()

// COMMAND ----------

// MAGIC %md
// MAGIC Let's visualize the results.

// COMMAND ----------

display(trade)