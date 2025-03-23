from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

APP_NAME = f"(Count vs isEmpty - No AQE - 450m - 1 core - 1 executor) {__file__}"

spark_conf = SparkConf().setAppName(APP_NAME)\
    .set("spark.executor.memory", "450m")\
    .set("spark.dynamicAllocation.enabled", "true")\
    .set("spark.dynamicAllocation.minExecutors", 1)\
    .set("spark.dynamicAllocation.maxExecutors", 1)\
    .set("spark.executor.cores", "1")\
    .set("spark.sql.adaptive.enabled", "false")\
    .set("spark.sql.adaptive.coalescePartitions.enabled", "false")
       
SOURCE_NON_PARTITIONED = "/opt/spark/data/bronze/individual_incident_archive_distinct_non_partitioned_parquet"


def run_count(spark):
    spark.sparkContext.setJobGroup(APP_NAME, "Job using df.count().")
    df = spark.read.parquet(SOURCE_NON_PARTITIONED)
    result = df.count()
    print(result)

def run_isempty(spark):
    spark.sparkContext.setJobGroup(APP_NAME, "Job using df.isEmpty().")
    df = spark.read.parquet(SOURCE_NON_PARTITIONED)
    result = df.isEmpty()
    print(result)
    
def run_rdd_isempty(spark):
    spark.sparkContext.setJobGroup(APP_NAME, "Job using df.rdd.isEmpty().")
    df = spark.read.parquet(SOURCE_NON_PARTITIONED)
    result = df.rdd.isEmpty()
    print(result)
    
def main():
    # https://stackoverflow.com/questions/74824056/what-is-spark-spill-disk-and-memory-both#:~:text=%22Shuffle%20spill%20(memory)%20is,much%20smaller%20than%20the%20former.
    # To run this application, run the command below into this project root directory:
    # make submit app=spill-individual-incident/individual_incident_count_vs_isempty.py
    #
    # Para executar esta aplicação, execute o comando abaixo no diretório root deste projeto:
    # make submit app=spill-individual-incident/individual_incident_count_vs_isempty.py
    # make submit-bullseye app=spill-individual-incident/individual_incident_count_vs_isempty.py
    spark = SparkSession.builder.config(conf = spark_conf).getOrCreate()
    
    run_count(spark)
    run_isempty(spark)
    run_rdd_isempty(spark)
    
if __name__ == "__main__":
    main()
    