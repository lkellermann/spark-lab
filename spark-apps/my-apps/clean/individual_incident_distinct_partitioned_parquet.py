from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

APP_NAME = f"(Distinct Partitioned Parquet - 3g) {__file__}"


spark_conf = SparkConf().setAppName(APP_NAME)\
    .set("spark.executor.memory", "3g")\
    .set("spark.dynamicAllocation.enabled", "true")\
    .set("spark.dynamicAllocation.minExecutors","2")\
    .set("spark.dynamicAllocation.maxExecutors","3")\
    .set("spark.default.parallelism", "3")\
    .set("spark.executor.cores", "2")\
    .set("spark.sql.adaptive.enabled", "true")\
    .set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    
SOURCE = "/opt/spark/data/bronze/individual_incident_archive_distinct_non_partitioned_parquet"

def read_without_schema_inference(spark, source):
    spark.sparkContext.setJobGroup(APP_NAME, "Reading without schema inference." )
    df = spark.read.parquet(source)
    return df
   
def write_distinct_partitioned_parquet(spark):
    output= "/opt/spark/data/bronze/individual_incident_distinct_partitioned_parquet"
    spark.sparkContext.setJobGroup(APP_NAME, "Creating partitioned CSV table with distinct values." )
    df = read_without_schema_inference(spark, SOURCE)
    df.write.format("parquet").partitionBy("date_HRF").mode("overwrite").save(output)

def main():
    # To run this application, run the command below into this project root directory:
    # make run-scaled spark-worker=5
    # make submit app=my-apps/clean/individual_incident_distinct_partitioned_parquet.py
    #
    # Para executar esta aplicação, execute o comando abaixo no diretório root deste projeto:
    # make run-scaled spark-worker=5
    # make submit app=my-apps/clean/individual_incident_distinct_partitioned_parquet.py
    
    spark = SparkSession.builder.config(conf = spark_conf).getOrCreate()
    
    write_distinct_partitioned_parquet(spark)

if __name__ == "__main__":
    main()