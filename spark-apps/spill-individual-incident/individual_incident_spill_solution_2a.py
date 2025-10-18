from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

APP_NAME = f"(Spill) Solution 2: showing spill with 16MiB partitions - {__file__}"

spark_conf = SparkConf().setAppName(APP_NAME)\
    .set("spark.executor.memory", "1g")\
    .set("spark.dynamicAllocation.enabled", "false")\
    .set("spark.executor.cores", 6)\
    .set("spark.sql.adaptive.enabled", "false")\
    .set("spark.sql.adaptive.coalescePartitions.enabled", "false")

SOURCE = "/opt/spark/data/landing/individual_incident_archive_csv"

def read_without_schema_inference(spark):
    spark.sparkContext.setJobGroup(APP_NAME, "Reading parquet files." )
    df = spark.read.option("header", "true").option("inferSchema", "false").csv(SOURCE)
    return df

def main():
    # To run this application, run the command below into this project root directory:
    # make submit app=spill-individual-incident/individual_incident_no_aqe_cache_mem_only_big_storage.py
    #
    # Para executar esta aplicação, execute o comando abaixo no diretório root deste projeto:
    # make submit app=spill-individual-incident/individual_incident_no_aqe_cache_mem_only_big_storage.py
    spark = SparkSession.builder.config(conf = spark_conf).getOrCreate()
    df_input = read_without_schema_inference(spark)

    spark.sparkContext.setJobGroup(APP_NAME, "Sorting and writing.")
    # 16MB = 16777216, 32MB = 33554432, 24MB = 25165824
    spark.conf.set("spark.sql.files.maxPartitionBytes", 16777216)

    # 13207.02MB/8MB = 1100,58 -> 1650,87 -> 1675
    spark.conf.set("spark.sql.shuffle.partitions", 1675)
    df_input.sort("ID", ascending = False).write.format("noop").mode("overwrite").save()


if __name__ == "__main__":
    main()
