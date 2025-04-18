from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

APP_NAME = f"(Spill) Solution 4: increases total executor memory - 'scaling out' executors  - {__file__}"

spark_conf = SparkConf().setAppName(APP_NAME)\
    .set("spark.executor.memory", "2g")\
    .set("spark.dynamicAllocation.enabled", "false")\
    .set("spark.executor.cores", 2)\
    .set("spark.sql.adaptive.enabled", "false")\
    .set("spark.sql.adaptive.coalescePartitions.enabled", "false")

SOURCE = "/opt/spark/data/landing/individual_incident_archive_csv"

def read_without_schema_inference(spark):
    spark.sparkContext.setJobGroup(APP_NAME, "Reading parquet files." )
    df = spark.read.option("header", "true").option("inferSchema", "false").csv(SOURCE)
    return df

def main():
    # To run this application, run the command below into this project root directory:
    # make submit app=spill-individual-incident/individual_incident_spill_solution_4.py
    #
    # Para executar esta aplicação, execute o comando abaixo no diretório root deste projeto:
    # make submit app=spill-individual-incident/individual_incident_spill_solution_4.py
    spark = SparkSession.builder.config(conf = spark_conf).getOrCreate()
    df_input = read_without_schema_inference(spark)

    spark.sparkContext.setJobGroup(APP_NAME, "Sorting and writing.")

    spark.conf.set("spark.sql.shuffle.partitions", 840)

    df_input.sort("ID", ascending = False).write.format("noop").mode("overwrite").save()


if __name__ == "__main__":
    main()
