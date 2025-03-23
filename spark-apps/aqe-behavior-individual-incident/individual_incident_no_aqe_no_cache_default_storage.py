from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

APP_NAME = f"(No Cache - No AQE - 450m - Default Storage Fraction.) {__file__}"

spark_conf = SparkConf().setAppName(APP_NAME)\
    .set("spark.executor.memory", "450m")\
    .set("spark.memory.fraction", 0.60)\
    .set("spark.memory.storageFraction", 0.50)\
    .set("spark.sql.shuffle.partitions", 1)\
    .set("spark.dynamicAllocation.enabled", "true")\
    .set("spark.dynamicAllocation.minExecutors", 1)\
    .set("spark.dynamicAllocation.maxExecutors", 1)\
    .set("spark.executor.cores", 1)\
    .set("spark.sql.adaptive.enabled", "false")\
    .set("spark.sql.adaptive.coalescePartitions.enabled", "false")
    
SOURCE = "/opt/spark/data/landing/individual_incident_archive_csv"

def read_without_schema_inference(spark, source):
    spark.sparkContext.setJobGroup(APP_NAME, "Reading without schema inference." )
    df = spark.read.option("header", "true").option("inferSchema", "false").csv(source)
    return df

def main():
    # https://stackoverflow.com/questions/74824056/what-is-spark-spill-disk-and-memory-both#:~:text=%22Shuffle%20spill%20(memory)%20is,much%20smaller%20than%20the%20former.
    # To run this application, run the command below into this project root directory:
    # make submit app=spill-individual-incident/individual_incident_no_aqe_no_cache_default_storage.py
    #
    # Para executar esta aplicação, execute o comando abaixo no diretório root deste projeto:
    # make submit app=spill-individual-incident/individual_incident_no_aqe_no_cache_default_storage.py
    spark = SparkSession.builder.config(conf = spark_conf).getOrCreate()
    df_input = read_without_schema_inference(spark, SOURCE)
       
    spark.sparkContext.setJobGroup(APP_NAME, "Sorting and writing dataframe - 5 Partitions.")
    spark.conf.set("spark.sql.shuffle.partitions", 5)
    df_input.sort("ID", ascending = False).write.format("noop").mode("overwrite").save()

    spark.sparkContext.setJobGroup(APP_NAME, "Sorting and writing dataframe - 1 Partitions.")
    spark.conf.set("spark.sql.shuffle.partitions", 1)
    df_input.sort("ID", ascending = False).write.format("noop").mode("overwrite").save()
    
if __name__ == "__main__":
    main()
    