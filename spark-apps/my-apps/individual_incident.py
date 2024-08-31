from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

APP_NAME = f"(Smallfile - 3000 - 1g) {__file__}"


spark_conf = SparkConf().setAppName(APP_NAME)\
    .set("spark.executor.memory", "1g")\
    .set("spark.memory.fraction", "0.80")\
    .set("spark.dynamicAllocation.enabled", "true")\
    .set("spark.dynamicAllocation.minExecutors","1")\
    .set("spark.dynamicAllocation.maxExecutors","3")\
    .set("spark.default.parallelism", "4")\
    .set("spark.executor.cores", "2")\
    .set("spark.sql.adaptive.enabled", "true")\
    .set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    
SOURCE = "/opt/spark/data/landing/individual_incident_archive_csv"
    
def read_with_schema_inference(spark, source):
    spark.sparkContext.setJobGroup(APP_NAME, "Reading with schema inference." )
    df = spark.read.option("header", "true").option("inferSchema", "true").csv(source)
    return df

def read_without_schema_inference(spark, source):
    spark.sparkContext.setJobGroup(APP_NAME, "Reading without schema inference." )
    df = spark.read.option("header", "true").csv(source)
    return df

def read_with_ddl(spark, source):
    spark.sparkContext.setJobGroup(APP_NAME, "Reading with DDL." )
    schema_ddl = """state STRING
                    ,ID STRING
                    ,ORI STRING
                    ,incident_number STRING
                    ,date_HRF STRING
                    ,date_SIF STRING
                    ,hour STRING
                    ,total_offense STRING
                    ,total_victim STRING
                    ,total_offender STRING
                    ,violence_offense STRING
                    ,theft_offense STRING
                    ,drug_offense STRING
                    ,sex_offense STRING
                    ,kidnapping_trafficking STRING
                    ,other_offense STRING
                    ,gun_involvement STRING
                    ,completed STRING
                    ,attempted STRING
                    ,drug_involvement STRING
                    ,property_value STRING
                    ,stolen_motor STRING
                    ,male_victim STRING
                    ,female_victim STRING
                    ,unknown_sex_victim STRING
                    ,w_victim STRING
                    ,b_victim STRING
                    ,i_victim STRING
                    ,a_victim STRING
                    ,p_victim STRING
                    ,unknown_race_victim STRING
                    ,minor_victim STRING
                    ,non_minor_victim STRING
                    ,unknown_age_victim STRING
                    ,offender_wi_family STRING
                    ,offender_outside_family STRING
                    ,offender_not_known STRING
                    ,male_offender STRING
                    ,female_offender STRING
                    ,unknown_sex_offender STRING
                    ,w_offender STRING
                    ,b_offender STRING
                    ,i_offender STRING
                    ,a_offender STRING
                    ,p_offender STRING
                    ,unknown_race_offender STRING
                    ,minor_offender STRING
                    ,non_minor_offender STRING
                    ,unknown_age_offender STRING""" 
    df = spark.read.option("header", "true").schema(schema_ddl).csv(source)
    return df

def return_ddl(df):
    schema_json = df.schema.json()
    ddl = df.sparkSession.sparkContext._jvm.org.apache.spark.sql.types.DataType.fromJson(schema_json).toDDL()
    return ddl

def benchmark_read_with_ddl_sort(spark):
    spark.sparkContext.setJobGroup(APP_NAME, "Write DF Sorted with DDL - Avoid Spill?" )
    df_ddl = read_with_ddl(spark, SOURCE)
    df_sorted = df_ddl.sort("ID", ascending=False)
    spark.sparkContext.setJobGroup(APP_NAME, "Write DF Sorted with DDL." )
    
    # Benchmark action:
    df_sorted.write.format("noop").mode("overwrite").save()

def create_small_parquet_files(spark):
    output = "/opt/spark/data/landing/individual_incident_archive_parquet_small"
    spark.sparkContext.setJobGroup(APP_NAME, "Creating Small Parquet Files." )
    num_partitions = 3000
    
    df = read_with_ddl(spark, SOURCE)
    df_small = df.repartition(num_partitions)
    df_small.write.format("parquet").mode("overwrite").save(output)

def main():
    # To run this application, run the command below into this project root directory:
    # make submit app=my-apps/individual_incident.py
    #
    # Para executar esta aplicação, execute o comando abaixo no diretório root deste projeto:
    # make submit app=my-apps/individual_incident.py
    
    spark = SparkSession.builder.config(conf = spark_conf).getOrCreate()
    #benchmark_write_df_sorted_with_ddl(spark)
    create_small_parquet_files(spark)

if __name__ == "__main__":
    main()