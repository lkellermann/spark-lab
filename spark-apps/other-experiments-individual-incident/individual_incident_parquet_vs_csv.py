from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

APP_NAME = f"(Max Date Partition Parquet - AQE - 8g - 1 core) {__file__}"

spark_conf = SparkConf().setAppName(APP_NAME)\
    .set("spark.executor.memory", "8g")\
    .set("spark.dynamicAllocation.enabled", "true")\
    .set("spark.dynamicAllocation.minExecutors", 1)\
    .set("spark.dynamicAllocation.maxExecutors", 1)\
    .set("spark.executor.cores", "1")\
    .set("spark.sql.adaptive.enabled", "true")\
    .set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    
SOURCE_PARQUET = "/opt/spark/data/bronze/individual_incident_distinct_partitioned_parquet"

SOURCE_CSV = "/opt/spark/data/bronze/individual_incident_distinct_partitioned_csv"

def run_maxdate_csv(spark):
    spark.sparkContext.setJobGroup(APP_NAME, "Max Date CSV.")
    df = spark.read.option("header", "true").option("inferSchema", "false").csv(SOURCE_CSV)
    df.selectExpr("max(date_HRF) as max_dateHRF").show()

def run_maxdate_parquet(spark):
    df = spark.read.parquet(SOURCE_PARQUET)
    spark.sparkContext.setJobGroup(APP_NAME, "Max Date Parquet.")
    df.selectExpr("max(date_HRF) as max_dateHRF").show()

def main():
    # https://stackoverflow.com/questions/74824056/what-is-spark-spill-disk-and-memory-both#:~:text=%22Shuffle%20spill%20(memory)%20is,much%20smaller%20than%20the%20former.
    # To run this application, run the command below into this project root directory:
    # make submit app=spill-individual-incident/individual_incident_parquet_vs_csv.py
    #
    # Para executar esta aplicação, execute o comando abaixo no diretório root deste projeto:
    # make submit app=spill-individual-incident/individual_incident_parquet_vs_csv.py
    spark = SparkSession.builder.config(conf = spark_conf).getOrCreate()
    
    #run_maxdate_csv(spark)
    run_maxdate_parquet(spark)    
    
if __name__ == "__main__":
    main()
    