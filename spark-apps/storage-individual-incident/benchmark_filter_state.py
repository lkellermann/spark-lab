from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

APP_NAME = "Benchmark Filter State"
spark_conf = SparkConf().setAppName(APP_NAME)\
    .set("spark.executor.memory", "5g")\
    .set("spark.dynamicAllocation.enabled", "false")\
    .set("spark.executor.instances", "1") \
    .set("spark.executor.cores", "3")\
    .set("spark.sql.adaptive.enabled", "true")\
    .set("spark.sql.adaptive.coalescePartitions.enabled", "true")\
    .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")\
    .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")


def main():
    # task submit APP=storage-individual-incident/benchmark_filter_state.py
    
    source_partition = "/opt/spark/data/bronze/individual_incident_delta_partitioned" 
    source_zorder = "/opt/spark/data/bronze/individual_incident_delta_zorder"
    source_lc = "/opt/spark/data/bronze/individual_incident_delta_liquid_clustering"

    spark = SparkSession.builder.config(conf = spark_conf).getOrCreate()

    filter_condition = "state = 'CA-California' OR state = 'California'"

    df_source_partitioned = spark.read.format('delta').load(source_partition).filter(filter_condition)
    df_source_zorder = spark.read.format('delta').load(source_zorder).filter(filter_condition)
    df_source_lc = spark.read.format('delta').load(source_zorder).filter(filter_condition)

    spark.sparkContext.setJobGroup(APP_NAME, "Emulating write - Source: Partitioned.")
    df_source_partitioned.write.format("noop").mode("overwrite").save()

    spark.sparkContext.setJobGroup(APP_NAME, "Emulating write - Source: ZOrder.")
    df_source_zorder.write.format("noop").mode("overwrite").save()

    spark.sparkContext.setJobGroup(APP_NAME, "Emulating write - Source: Liquid Clustering.")
    df_source_lc.write.format("noop").mode("overwrite").save()

if __name__ == '__main__':
    main()