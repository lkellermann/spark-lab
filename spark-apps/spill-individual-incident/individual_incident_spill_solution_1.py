from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.storagelevel import StorageLevel

APP_NAME = f"(Spill) Solution 1: increase number of partitions = smaller partitions - {__file__}"

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
    # make submit app=spill-individual-incident/individual_incident_spill_solution_1.py
    #
    # Para executar esta aplicação, execute o comando abaixo no diretório root deste projeto:
    # make submit app=spill-individual-incident/individual_incident_spill_solution_1.py
    spark = SparkSession.builder.config(conf = spark_conf).getOrCreate()
    df_input = read_without_schema_inference(spark, SOURCE)

    # Shuffle Write -> 12.3GiB -> 13207.02 MiB
    # Shuffle Partition Size -> 4MiB

    # shuffle spark.sql.shuffle.partitions = 13207.02MiB/4MiB = 3301.75 -> 3350

    spark.sparkContext.setJobGroup(APP_NAME, "Sorting and writing.")
    spark.conf.set("spark.sql.files.maxPartitionBytes", 8388608) # 8MB
    spark.conf.set("spark.sql.shuffle.partitions", 3350)
    df_input.sort("ID", ascending = False).write.format("noop").mode("overwrite").save()


if __name__ == "__main__":
    main()
