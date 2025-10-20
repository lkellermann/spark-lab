from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

APP_NAME = f"(Delta Partitioned) {__file__}"
SOURCE = "/opt/spark/data/landing/individual_incident_archive_csv"

spark_conf = SparkConf().setAppName(APP_NAME)\
    .set("spark.executor.memory", "8g")\
    .set("spark.executor.cores", "3")\
    .set("spark.dynamicAllocation.enabled", "true")\
    .set("spark.dynamicAllocation.minExecutors","1")\
    .set("spark.dynamicAllocation.maxExecutors","1")\
    .set("spark.sql.adaptive.enabled", "true")\
    .set("spark.sql.adaptive.coalescePartitions.enabled", "true")\
    .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")\
    .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")


def read_without_schema_inference(spark, source):
    spark.sparkContext.setJobGroup(APP_NAME, "Reading without schema inference." )
    df = spark.read.option("header", "true").option("inferSchema", "false").csv(source)
    return df

def read_with_ddl(spark, source):
    spark.sparkContext.setJobGroup(APP_NAME, "Reading with DDL." )
    schema_ddl = """state STRING
                    ,ID STRING
                    ,ORI STRING
                    ,incident_number STRING
                    ,date_HRF INT
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

def write_delta_partitioned(spark):

    output = "/opt/spark/data/bronze/individual_incident_delta_partitioned"
    spark.sparkContext.setJobGroup(APP_NAME, "Creating Delta table with Partitions.")

    df = read_with_ddl(spark, SOURCE)

    df.write.format("delta")\
        .mode("overwrite")\
        .partitionBy("date_HRF")\
        .save(output)

    print("Partitioned by by columns: date_HRF")

def main():
    # To run this application, run the command below:
    # task submit APP=my-apps/clean/individual_incident_delta_partitioned.py

    spark = SparkSession.builder.config(conf = spark_conf).getOrCreate()

    # Create Delta table with liquid clustering
    write_delta_partitioned(spark)


if __name__ == "__main__":
    main()