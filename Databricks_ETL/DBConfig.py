# Databricks notebook source
# Set authentication type to SAS for the specified ADLS Gen2 account
spark.conf.set("fs.azure.account.auth.type.dlsassign.dfs.core.windows.net", "SAS")

# Specify the SAS token provider type for accessing the ADLS account
spark.conf.set("fs.azure.sas.token.provider.type.dlsassign.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")

# Set the fixed SAS token value for authentication and authorization
spark.conf.set("fs.azure.sas.fixed.token.dlsassign.dfs.core.windows.net", "sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2023-08-31T03:26:51Z&st=2023-08-07T19:26:51Z&spr=https&sig=wU3ruQRItsr%2BNfdVcUerP4mqT2WHtA5QYOrPWZEGPwA%3D")

# COMMAND ----------

# Create the dropdown widget
dbutils.widgets.dropdown("dbname", defaultValue="cleansed", choices=["cleansed", "curated",
                                                                     "raw"])

# Get the selected value from the widget
db = dbutils.widgets.get("dbname")

# Print the selected database name
print("Selected database:", db)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS raw;
# MAGIC CREATE DATABASE IF NOT EXISTS cleansed;
# MAGIC CREATE DATABASE IF NOT EXISTS curated;

# COMMAND ----------

available_db = spark.sql(f"Show databases").collect()
display(available_db)

# COMMAND ----------

# MAGIC %sql
# MAGIC truncate table cleansed.customers

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE table IF NOT EXISTS cleansed.customers
# MAGIC (
# MAGIC customer_id STRING,
# MAGIC company_name STRING,
# MAGIC specialized_industries string
# MAGIC )
# MAGIC USING DELTA LOCATION 'abfs://cleansed@dlsassign.dfs.core.windows.net/customers'
# MAGIC

# COMMAND ----------

from delta.tables import DeltaTable
delta_tb = DeltaTable.forName(spark,"raw.customers")
display(delta_tb)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE table IF NOT EXISTS curated.orders
# MAGIC (
# MAGIC order_id STRING,
# MAGIC customer_id STRING,
# MAGIC order_lines_product_id string,
# MAGIC order_lines_volume int,
# MAGIC order_lines_price float,
# MAGIC amount float,
# MAGIC `timestamp` TIMESTAMP
# MAGIC )
# MAGIC USING DELTA LOCATION 'abfs://curated@dlsassign.dfs.core.windows.net/curated/orders'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS curated.live_orders(
# MAGIC     order_id STRING,
# MAGIC     order_customer_id STRING,
# MAGIC     order_line STRUCT<
# MAGIC         price: FLOAT,
# MAGIC         product_id: STRING,
# MAGIC         volume: INT
# MAGIC     >,
# MAGIC   `timestamp` TIMESTAMP
# MAGIC )
# MAGIC USING DELTA LOCATION 'abfs://curated@dlsassign.dfs.core.windows.net/curated/live_orders'