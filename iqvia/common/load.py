from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType


# import findspark


def load_df(spark: SparkSession, load_path: str, schema: StructType, file_delimiter='|', file_header=True,
            infer_schema=False, format_type='csv'):
    if format_type == 'csv':
        if not infer_schema:
            return spark.read \
                .schema(schema) \
                .options(inferSchema=False, delimiter=file_delimiter, header=file_header) \
                .csv(load_path)
        else:
            return spark.read \
                .options(inferSchema=True, delimiter=file_delimiter, header=file_header) \
                .csv(load_path)
    elif format_type == 'parquet':
        return spark.read.parquet(load_path)


def load_plan(spark: SparkSession, load_path: str, schema: StructType, format_type):
    return load_df(spark, load_path, schema, format_type=format_type) \
        .withColumn('org_type', lit('THIRD PARTY CLAIMS AGGREGATOR')) \
        .withColumn('plan_status', lit(True))


def load_patient(spark: SparkSession, load_path: str, schema: StructType, format_type):
    return load_df(spark, load_path, schema, format_type=format_type) \
        .withColumn('consumer_type', lit('MEMBER')) \
        .withColumn('consumer_status', lit(True))


def load_claim(spark: SparkSession, load_path: str, schema: StructType, format_type):
    return load_df(spark, load_path, schema, format_type=format_type) \
        .withColumn("PLAN_ID_CLAIM", col("PLAN_ID")) \
        .withColumn('source_org_oid', lit('IQVIA'))
        # .drop(col("PATIENT_ID"))
# .withColumn("PATIENT_ID_CLAIM", col("PATIENT_ID")) \


def load_procedure(spark: SparkSession, load_path: str, schema: StructType, format_type):
    return load_df(spark, load_path, schema, format_type=format_type)


# def load_procedure_modifier1(spark: SparkSession, load_path: str, schema: StructType, format_type):
#     return load_df(spark, load_path, schema) \
#         .select(col('PRC_MODR_CD').alias('PRC1_MODR_CD'), col('PRC_MODR_DESC').alias('PRC1_MODR_DESC'))
#
#
# def load_procedure_modifier2(spark: SparkSession, load_path: str, schema: StructType, format_type):
#     return load_df(spark, load_path, schema) \
#         .select(col('PRC_MODR_CD').alias('PRC2_MODR_CD'), col('PRC_MODR_DESC').alias('PRC2_MODR_DESC'))
#
#
# def load_procedure_modifier3(spark: SparkSession, load_path: str, schema: StructType):
#     return load_df(spark, load_path, schema) \
#         .select(col('PRC_MODR_CD').alias('PRC3_MODR_CD'), col('PRC_MODR_DESC').alias('PRC3_MODR_DESC'))
#
#
# def load_procedure_modifier4(spark: SparkSession, load_path: str, schema: StructType, format_type):
#     return load_df(spark, load_path, schema) \
#         .select(col('PRC_MODR_CD').alias('PRC4_MODR_CD'), col('PRC_MODR_DESC').alias('PRC4_MODR_DESC'))


def load_diagnosis(spark: SparkSession, load_path: str, schema: StructType, format_type):
    return load_df(spark, load_path, schema, format_type=format_type)


def load_drug(spark: SparkSession, load_path: str, schema: StructType, format_type):
    return load_df(spark, load_path, schema, format_type=format_type)


def load_provider(spark: SparkSession, load_path: str, schema: StructType, format_type):
    return load_df(spark, load_path, schema, format_type=format_type)


# def load_rendering_provider(provider_raw: DataFrame):
#     return provider_raw \
#         .select(col('PROVIDER_ID').alias('RENDERING_PROVIDER_ID'),
#                 col('PROVIDER_TYP_ID').alias('RENDERING_PROVIDER_TYP_ID'),
#                 col('ORG_NM').alias('RENDERING_ORG_NM'),
#                 col('IMS_RXER_ID').alias('RENDERING_IMS_RXER_ID'),
#                 col('LAST_NM').alias('RENDERING_LAST_NM'),
#                 col('FIRST_NM').alias('RENDERING_FIRST_NM'),
#                 col('ADDR_LINE1_TXT').alias('RENDERING_ADDR_LINE1_TXT'),
#                 col('ADDR_LINE2_TXT').alias('RENDERING_ADDR_LINE2_TXT'),
#                 col('CITY_NM').alias('RENDERING_CITY_NM'),
#                 col('ST_CD').alias('RENDERING_ST_CD'),
#                 col('ZIP').alias('RENDERING_ZIP'),
#                 col('PRI_SPCL_CD').alias('RENDERING_PRI_SPCL_CD'),
#                 col('PRI_SPCL_DESC').alias('RENDERING_PRI_SPCL_DESC'),
#                 col('ME_NBR').alias('RENDERING_ME_NBR'),
#                 col('NPI').alias('RENDERING_NPI'))
#
#
# def load_referring_provider(provider_raw: DataFrame):
#     return provider_raw \
#         .select(col('PROVIDER_ID').alias('REFERRING_PROVIDER_ID'),
#                 col('PROVIDER_TYP_ID').alias('REFERRING_PROVIDER_TYP_ID'),
#                 col('ORG_NM').alias('REFERRING_ORG_NM'),
#                 col('IMS_RXER_ID').alias('REFERRING_IMS_RXER_ID'),
#                 col('LAST_NM').alias('REFERRING_LAST_NM'),
#                 col('FIRST_NM').alias('REFERRING_FIRST_NM'),
#                 col('ADDR_LINE1_TXT').alias('REFERRING_ADDR_LINE1_TXT'),
#                 col('ADDR_LINE2_TXT').alias('REFERRING_ADDR_LINE2_TXT'),
#                 col('CITY_NM').alias('REFERRING_CITY_NM'),
#                 col('ST_CD').alias('REFERRING_ST_CD'),
#                 col('ZIP').alias('REFERRING_ZIP'),
#                 col('PRI_SPCL_CD').alias('REFERRING_PRI_SPCL_CD'),
#                 col('PRI_SPCL_DESC').alias('REFERRING_PRI_SPCL_DESC'),
#                 col('ME_NBR').alias('REFERRING_ME_NBR'),
#                 col('NPI').alias('REFERRING_NPI'))

# pro_provider_raw = spark.read.schema(raw_pro_provider_schema)\
#                     .options(inferSchema=False,delimiter=',', header=True)\
#                     .csv(pro_provider_path)\
#                     .persist(StorageLevel.MEMORY_AND_DISK)
