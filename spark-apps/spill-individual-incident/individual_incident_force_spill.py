from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.storagelevel import StorageLevel

APP_NAME = f"(Spill) Showing spill {__file__}"
# app-20250304130422-0001

spark_conf = SparkConf().setAppName(APP_NAME)\
    .set("spark.executor.memory", "1g")\
    .set("spark.dynamicAllocation.enabled", "false")\
    .set("spark.executor.cores", 6)\
    .set("spark.sql.adaptive.enabled", "false")\
    .set("spark.sql.adaptive.coalescePartitions.enabled", "false")
    
SOURCE = "/opt/spark/data/landing/individual_incident_archive_csv"

def read_without_schema_inference(spark, source):
    spark.sparkContext.setJobGroup(APP_NAME, "Reading parquet files." )
    df = spark.read.option("header", "true").option("inferSchema", "false").csv(SOURCE)
    return df

def main():
    # To run this application, run the command below into this project root directory:
    # make run
    # make submit app=spill-individual-incident/individual_incident_force_spill.py
    #
    # Para executar esta aplicação, execute o comando abaixo no diretório root deste projeto:
    # make submit app=spill-individual-incident/individual_incident_no_aqe_cache_mem_only_big_storage.py
    #app-20250303153804-0011
    
    spark = SparkSession.builder.config(conf = spark_conf).getOrCreate()
    df_input = read_without_schema_inference(spark, SOURCE)
    
    spark.sparkContext.setJobGroup(APP_NAME, "Sorting and writing." )
    df_input.sort("ID", ascending = False).write.format("noop").mode("overwrite").save()

    
if __name__ == "__main__":
    main()
