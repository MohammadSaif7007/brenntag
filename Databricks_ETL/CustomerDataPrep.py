# Databricks notebook source
spark.conf.set("fs.azure.account.auth.type.dlsassign.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.dlsassign.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.dlsassign.dfs.core.windows.net", "sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2023-08-31T03:26:51Z&st=2023-08-07T19:26:51Z&spr=https&sig=wU3ruQRItsr%2BNfdVcUerP4mqT2WHtA5QYOrPWZEGPwA%3D")

# COMMAND ----------

from pyspark.sql.functions import col, split, explode
from pyspark.sql.utils import AnalysisException

# COMMAND ----------

def process_data():
    source_data_df = spark.read.parquet('abfs://raw@dlsassign.dfs.core.windows.net/customers.parquet')
    total_rows_before = source_data_df.count()

    # Filter null values and count after
    source_data_df = source_data_df.filter(col("specialized_industries").isNotNull())
    total_rows_after = source_data_df.count()

    # Display and print results
    null_specialized_industries = source_data_df.filter(col("specialized_industries").isNull())
    null_specialized_industries.select("customer_id", "company_name").show()

    print(f"Total rows before removing null values: {total_rows_before}")
    print(f"Total rows after removing null values: {total_rows_after}")

    source_data_df = source_data_df.withColumn("specialized_industries", explode(split(col("specialized_industries"), ";")))

    try:
        existing_df = spark.sql(f"SELECT * FROM cleansed.customers")
    except AnalysisException:
        existing_df = None

    # Your data transfer logic
    if existing_df.count() == 0:
        incremental_df = source_data_df
    else:
        # Find the difference including duplicates (equivalent to EXCEPT ALL)
        incremental_df = existing_df.exceptAll(source_data_df)

    # Verify if the DataFrame is empty or not using count()
    if incremental_df.head(1):
        print(f"new rows are available!! Total new rows are {incremental_df.count()} added in cleansed.customers table")
        incremental_df.write.format("delta").mode("append").saveAsTable("cleansed.customers")
    else:
        print(f"No new rows are added as same data already exist in the bucket")


# COMMAND ----------

process_data()