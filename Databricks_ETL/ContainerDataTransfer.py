# Databricks notebook source
spark.conf.set("fs.azure.account.auth.type.dlsassign.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.dlsassign.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.dlsassign.dfs.core.windows.net", "sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2023-08-31T03:26:51Z&st=2023-08-07T19:26:51Z&spr=https&sig=wU3ruQRItsr%2BNfdVcUerP4mqT2WHtA5QYOrPWZEGPwA%3D")

# COMMAND ----------

from pyspark.sql.utils import AnalysisException
def data_transfer(source_data,existing_data):
        """
        Test the data transfer logic using Spark DataFrames.

        This method performs the following steps:
        1. Reads data from the source container 'demo-data' and displays it.
        2. Tries to read existing data from the destination container 'raw'. If it fails, assumes no existing data.
        3. Computes the incremental DataFrame based on whether existing data is available.
        4. Checks if the incremental DataFrame contains any rows and prints corresponding messages and load data if new data arrived.
        """
        # Reads data from container demo-data and display it
        source_data_df = spark.read.csv(source_data, header=True)
        
        # Read existing data from the destination container if available
        try:
            existing_df = spark.read.parquet(existing_data)
        except AnalysisException:
            existing_df = None
        
        # Your data transfer logic
        if existing_df is not None:
            incremental_df = existing_df.exceptAll(source_data_df)
        else:
            incremental_df = source_data_df
        
        # Verify if the DataFrame is empty or not using count()
        if incremental_df.head(1):
            print("Bucket is empty")
            incremental_df.write.mode("append").format("parquet").save(existing_data)
        else:
            print("Bucket is not empty")