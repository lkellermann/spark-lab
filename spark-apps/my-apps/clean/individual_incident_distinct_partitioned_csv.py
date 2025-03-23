from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

APP_NAME = f"(Distinct Partitioned CSV - 5g) {__file__}"


spark_conf = SparkConf().setAppName(APP_NAME)\
    .set("spark.executor.memory", "5g")\
    .set("spark.dynamicAllocation.enabled", "true")\
    .set("spark.dynamicAllocation.minExecutors","1")\
    .set("spark.dynamicAllocation.maxExecutors","1")\
    .set("spark.default.parallelism", "1")\
    .set("spark.executor.cores", "4")\
    .set("spark.sql.adaptive.enabled", "true")\
    .set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    
SOURCE = "/opt/spark/data/bronze/individual_incident_distinct_partitioned_parquet"

def read_without_schema_inference(spark, source):
    spark.sparkContext.setJobGroup(APP_NAME, "Reading without schema inference." )
    df = spark.read.parquet(source)
    return df
   
def write_distinct_partitioned_csv(spark):
    output= "/opt/spark/data/bronze/individual_incident_distinct_partitioned_csv"
    spark.sparkContext.setJobGroup(APP_NAME, "Creating partitioned CSV table with distinct values." )
    df = read_without_schema_inference(spark, SOURCE)
    df.write.format("csv").partitionBy("date_HRF").mode("overwrite").save(output)

def main():
    # To run this application, run the command below into this project root directory:
    # make run-scaled spark-worker=5
    # make submit app=my-apps/clean/individual_incident_distinct_partitioned_csv.py
    #
    # Para executar esta aplicação, execute o comando abaixo no diretório root deste projeto:
    # make run-scaled spark-worker=5
    # make submit app=my-apps/clean/individual_incident_distinct_partitioned_csv.py
    
    spark = SparkSession.builder.config(conf = spark_conf).getOrCreate()
    
    write_distinct_partitioned_csv(spark)

if __name__ == "__main__":
    main()