from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

APP_NAME = f"(Predicate Pushdown - AQE - 5g - 2 cores - Max 3 executors) {__file__}"

spark_conf = SparkConf().setAppName(APP_NAME)\
    .set("spark.executor.memory", "5g")\
    .set("spark.dynamicAllocation.enabled", "true")\
    .set("spark.dynamicAllocation.minExecutors", 1)\
    .set("spark.dynamicAllocation.maxExecutors", 3)\
    .set("spark.executor.cores", "2")\
    .set("spark.sql.adaptive.enabled", "true")\
    .set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    
SOURCE_NON_PARTITIONED = "/opt/spark/data/bronze/individual_incident_archive_non_partitioned_parquet"

SOURCE_PARTITIONED = "/opt/spark/data/bronze/individual_incident_archive_partitioned_parquet"

def run_partitioned_filter_before(spark):
    spark.sparkContext.setJobGroup(APP_NAME, "Run aggregations AFTER filtering in partitioned data.")
    df_fb = spark.read.parquet(SOURCE_PARTITIONED).filter("cast(date_HRF as string) like ('202102%')")
    df_fb.groupBy("date_HRF").count().write.format("noop").mode("overwrite").save()

def run_partitioned_filter_before_one_line(spark):
    df_fb = spark.read.parquet(SOURCE_PARTITIONED)
    spark.sparkContext.setJobGroup(APP_NAME, "Run aggregations AFTER filtering in partitioned data - ONE LINE.")
    df_fb.filter("cast(date_HRF as string) like ('202102%')").groupBy("date_HRF").count().write.format("noop").mode("overwrite").save()

def run_partitioned_filter_after(spark):
    spark.sparkContext.setJobGroup(APP_NAME, "Run aggregations BEFORE filtering in partitioned data.")
    df_fa = spark.read.parquet(SOURCE_PARTITIONED)
    df_fa.groupBy("date_HRF").count().filter("date_HRF like ('202102%')").write.format("noop").mode("overwrite").save()

#def run_non_partitioned_filter_before(spark):
#    df_fbnp = read_parquet(spark, SOURCE_NON_PARTITIONED)
#    spark.sparkContext.setJobGroup(APP_NAME, "Run aggregations AFTER filtering in NON partitioned data.")
#    df_fbnp.where("date_HRF like ('202102%')").groupBy("date_HRF").count().write.format("noop").mode("overwrite").save()

def main():
    # https://stackoverflow.com/questions/74824056/what-is-spark-spill-disk-and-memory-both#:~:text=%22Shuffle%20spill%20(memory)%20is,much%20smaller%20than%20the%20former.
    # To run this application, run the command below into this project root directory:
    # make submit app=spill-individual-incident/individual_incident_predicate_pushdown.py
    #
    # Para executar esta aplicação, execute o comando abaixo no diretório root deste projeto:
    # make submit app=spill-individual-incident/individual_incident_predicate_pushdown.py
    spark = SparkSession.builder.config(conf = spark_conf).getOrCreate()
    
    run_partitioned_filter_before(spark)
    run_partitioned_filter_before_one_line(spark)
    run_partitioned_filter_after(spark)
    #run_non_partitioned_filter_before(spark)
    
    
    
if __name__ == "__main__":
    main()
    