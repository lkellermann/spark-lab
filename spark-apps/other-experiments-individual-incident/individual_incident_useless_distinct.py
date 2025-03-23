from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

APP_NAME = f"(Useless Distinct - AQE - 4g - 6 cores) {__file__}"

spark_conf = SparkConf().setAppName(APP_NAME)\
    .set("spark.executor.memory", "4g")\
    .set("spark.dynamicAllocation.enabled", "true")\
    .set("spark.dynamicAllocation.minExecutors", 1)\
    .set("spark.dynamicAllocation.maxExecutors", 1)\
    .set("spark.executor.cores", "6")\
    .set("spark.sql.adaptive.enabled", "true")\
    .set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    
SOURCE = "/opt/spark/data/bronze/individual_incident_archive_distinct_non_partitioned_parquet"

def read_parquet(spark, source):
    spark.sparkContext.setJobGroup(APP_NAME, "Reading parquet files." )
    df = spark.read.parquet(source)
    return df

def main():
    # https://stackoverflow.com/questions/74824056/what-is-spark-spill-disk-and-memory-both#:~:text=%22Shuffle%20spill%20(memory)%20is,much%20smaller%20than%20the%20former.
    # To run this application, run the command below into this project root directory:
    # make submit app=spill-individual-incident/individual_incident_useless_distinct.py
    #
    # Para executar esta aplicação, execute o comando abaixo no diretório root deste projeto:
    # make submit app=spill-individual-incident/individual_incident_useless_distinct.py
    spark = SparkSession.builder.config(conf = spark_conf).getOrCreate()
    df_input = read_parquet(spark, SOURCE)
    
    spark.sparkContext.setJobGroup(APP_NAME, "Emulating write after useless distinct.")
    df_input.distinct().write.format("noop").mode("overwrite").save()

    spark.sparkContext.setJobGroup(APP_NAME, "Emulating write without the useless distinct")
    df_input.write.format("noop").mode("overwrite").save()
    
if __name__ == "__main__":
    main()
    