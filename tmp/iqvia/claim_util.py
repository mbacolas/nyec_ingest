from pyspark.sql import DataFrame
from pyspark import RDD
from pyspark.sql.functions import *
from pyspark import StorageLevel
from pyspark.sql import Row
from datetime import datetime, timedelta, date
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, BooleanType
from pymonad.either import Left, Right, Either
import traceback
import json



source_org_oid='IQVIA'

# @udf(returnType=StringType())
# def is_valid_code(code: str) -> str:
#     if code == 'a':
#         return 'BBBB'
#     else:
#         return ''        # return json.dumps({'code': 'this is an invalid code'})

#
# @udf(returnType=StringType())
# def convert_to_iso(code: str, return_error=False) -> str:
#     if return_error:
#         return json.dumps({'code': 'this is an invalid code'})
#     else:
#         return '2020-01-01'

#
# def to_date(source_column_name: str, target_column_name: str, date_str: str, procedure: dict, date_format="%Y-%m-%d"):
#     try:
#         date_time_obj = datetime.strptime(date_str, date_format).date()
#         procedure[target_column_name] = date_time_obj
#         procedure[target_column_name+'_raw'] = date_time_obj
#     except Exception as e:
#         e = traceback.format_exc()
#         procedure['error'].append(json.dumps({'source_column_name': source_column_name, 'error': e, 'raw_value': date_str}))
#         # return Left(json.dumps({'column': col_name, 'error': e, 'raw_value': date_str}))
#     # else:
#
#     # return Right(date_time_obj)


def is_code_valid(code: str, code_system: str) -> Either:
    # validation_errors.append(json.dumps({'column': 'PRC_VERS_TYP_ID:PRC_CD',
    #                                      'error': 'INVALID_CODE OR CODE_SYSTEM',
    #                                      'raw_value': claim_row.PRC_VERS_TYP_ID + ':' + claim_row.PRC_CD}))  # TODO: standardize message
    return Right(True)

# @udf(returnType=StringType())
# def to_dob(year: int) -> date:
#     dob = str(year)+'-01-01'
#     return datetime.strptime(dob, "%Y-%m-%d").date()

# udf_star_desc = udf(lambda year:to_dob(year),DateType())
# test = claims_raw.select(col('CLAIM_ID'), col('SVC_NBR'), col('CLAIM_TYP_CD'), col('SVC_FR_DT'), col('SVC_TO_DT'), col('HOSP_ADMT_DT'))
# test.withColumn("CLAIM_TYP_CD",udf_star_desc(col("CLAIM_TYP_CD")))  #.select(col('CLAIM_ID'), col('SVC_NBR'), col('CLAIM_TYP_CD'), col('SVC_FR_DT'), col('SVC_TO_DT')).first()
# test.withColumn("CLAIM_TYP_CD",func1("CLAIM_TYP_CD"))

def test(**kwargs):
    print(kwargs)

{'a':None}
test(**{'a':1})

def create_proc_row(claim_row: Row) -> Row:
    start_date_result = convert_to_date('SVC_FR_DT', claim_row.SVC_FR_DT)
    to_date_result = convert_to_date('SVC_TO_DT', claim_row.SVC_TO_DT)
    validation_errors = []
    validation_warnings = []
    valid = True
    warn = False
    start_date = None
    to_date = None

    row_dict = claim_row.asDict()
    new_row_dict = {'source_consumer_id': row_dict['PATIENT_ID']}
    new_row = Row(**new_row_dict)

    if start_date_result.is_left():
        validation_errors.append(start_date_result.either(lambda x: x, lambda x: x))
        valid = False
    else:
        start_date = start_date_result.value
    if to_date_result.is_left():
        validation_errors.append(to_date_result.either(lambda x: x, lambda x: x))
        valid = False
    else:
        to_date = to_date_result.value
    code = None
    code_system = None
    revenue_code = None
    is_proc_code_valid = is_code_valid(claim_row.PRC_CD, claim_row.PRC_VERS_TYP_ID)
    is_rev_code_valid = is_code_valid(claim_row.CLAIM_HOSP_REV_CD, 'REV')
    if is_proc_code_valid.is_left():
        validation_errors.append(is_proc_code_valid.either(lambda x: x, lambda x: x))
        valid = False
    else:
        code = claim_row.PRC_CD
        code_system = claim_row.PRC_VERS_TYP_ID
    if is_rev_code_valid.is_left():
        validation_errors.append(is_rev_code_valid.either(lambda x: x, lambda x: x))
        valid = False
    else:
        revenue_code = claim_row.CLAIM_HOSP_REV_CD
    proc_row = Row(source_consumer_id=claim_row.PATIENT_ID,
                   source_org_oid=claim_row.source_org_oid,
                   start_date_raw=claim_row.SVC_FR_DT,
                   start_date=start_date,
                   to_date_raw=claim_row.SVC_TO_DT,
                   to_date=to_date,
                   code_raw=claim_row.PRC_CD,
                   code=code,
                   code_system_raw=claim_row.PRC_VERS_TYP_ID,
                   code_system=code_system,
                   revenue_code_raw=claim_row.CLAIM_HOSP_REV_CD,
                   revenue_code=revenue_code,
                   mod_raw=[claim_row.PRC1_MODR_CD, claim_row.PRC2_MODR_CD, claim_row.PRC3_MODR_CD, claim_row.PRC4_MODR_CD],
                   mod=[claim_row.PRC1_MODR_CD, claim_row.PRC2_MODR_CD, claim_row.PRC3_MODR_CD, claim_row.PRC4_MODR_CD],
                   error=validation_errors,
                   warning=validation_warnings,
                   is_valid=valid,
                   has_warnings=warn,
                   TMP=None)
    return proc_row


from entity import *

def create_row(claim_row):
    r = {'source_consumer_id': claim_row.PATIENT_ID}
    Row()

# claim_raw.map(lambda r: create_proc_row(r))

def to_procedure(claim_raw: RDD, procedure_fields: list) -> RDD:

    for field in procedure_fields:
        f = procedure_meta[field]
        f()
        # claim_raw.withColumn('source_org_oid', lit('IQVIA'))
        pass


    return claim_raw.map(lambda r: Row(**get_procedure_dict(r.asDict())))

    # return claim_raw.map(lambda r: create_proc_row(r))


# def to_procedure(claim_raw: DataFrame) -> DataFrame:
#     udf_code_desc_lookup = udf(lambda code, code_system: code_desc_lookup(code, code_system), StringType())
#     from pyspark.sql import functions as F
#
#     # .withColumn('new_column', lit(10))
#     # .withColumn("PRC_VERS_TYP_ID", when(claim_df.PRC_VERS_TYP_ID == "10", "ICD10"))
#     #                                  .when(test.CLAIM_TYP_CD == "P","OUTPATIENT")
#     #                                  # .when(test.CLAIM_TYP_CD.isNull() ,"")
#     #                                  .otherwise(test.CLAIM_TYP_CD))
#
#     a = claim_raw.select('SVC_FR_DT', 'PRC_CD', 'PRC_VERS_TYP_ID'). \
#         withColumn('ERROR', lit('')).\
#         withColumn('ERROR', concat(col('ERROR'), lit("new_word")))
#
#     claim_raw.select('SVC_FR_DT', 'PRC_CD', 'PRC_VERS_TYP_ID'). \
#         withColumn('ERROR', lit('')). \
#         withColumn('IS_VALID', is_valid(col('ERROR'))). \
#         withColumn('SVC_FR_DT_NEW_A', lit("USA**")).\
#         withColumn('SVC_FR_DT_NEW', code_desc_lookup(col("PRC_CD"), col("PRC_VERS_TYP_ID"))).\
#         withColumn('ERROR', concat(col('ERROR'), convert_to_iso(lit('WTF')) )).\
#         withColumn('ERROR', concat(col('ERROR'), is_valid_code(lit('WTF')) )). \
#         withColumn('IS_VALID', is_valid(col('ERROR'))). \
#         withColumn('ERROR', concat(col('ERROR'), is_valid_code(lit('WTF')))). \
#         show(truncate=False)
#         # withColumn('ERROR', concat(col('ERROR'), is_valid_code(col('SVC_FR_DT_NEW'), col('SVC_FR_DT_NEW'))))
#
#
#     claim_raw.withColumn('SVC_FR_DT', udf_code_desc_lookup(col("PRC_CD"), col("PRC_VERS_TYP_ID")))
#
#     claim_raw.withColumn("PRC_VERS_TYP_ID", when(col('PRC_VERS_TYP_ID') == "10", "ICD10"))
#
#     claim_raw.select(claim_raw.PATIENT_ID.alias('source_consumer_id'),
#                      claim_raw.SVC_FR_DT.alias('start_date_raw'))\
#                     .withColumn('start_date', udf_code_desc_lookup(col("PRC_CD"), col("PRC_VERS_TYP_ID")))
#
#     return claim_raw.select(claim_raw.PATIENT_ID.alias('source_consumer_id'),
#                              claim_raw.SVC_FR_DT.alias('start_date_raw'),
#                              # claim_raw.SVC_FR_DT.alias('SVC_FR_DT').alias('start_date'),
#                              claim_raw.SVC_TO_DT.alias('end_date_raw'),
#                              # claim_raw.SVC_TO_DT.alias('SVC_TO_DT').alias('end_date'),
#                              claim_raw.PRC_CD.alias('code_raw'),
#                              # claim_raw.PRC_CD.alias('PRC_CD').alias('code'),
#                              claim_raw.PRC_VERS_TYP_ID.alias('code_system_raw'),
#                              # claim_raw.PRC_VERS_TYP_ID.alias('PRC_VERS_TYP_ID').alias('code_system'),
#                              claim_raw.CLAIM_HOSP_REV_CD.alias('revenue_code_raw'),
#                              # claim_raw.CLAIM_HOSP_REV_CD.alias('CLAIM_HOSP_REV_CD').alias('revenue_code'),
#                              # claim_raw.PAT_ZIP3.alias('???DESC').alias('desc'),
#                              claim_raw.PRC1_MODR_CD.alias('mod1_raw'),
#                              # claim_raw.PRC1_MODR_CD.alias('PRC1_MODR_CD').alias('mod1'),
#                              claim_raw.PRC2_MODR_CD.alias('mod2_raw'),
#                              # claim_raw.PRC2_MODR_CD.alias('PRC2_MODR_CD').alias('mod2'),
#                              claim_raw.PRC3_MODR_CD.alias('mod3_raw'),
#                              # claim_raw.PRC3_MODR_CD.alias('PRC3_MODR_CD').alias('mod3'),
#                              claim_raw.PRC4_MODR_CD.alias('mod4_raw'),
#                              # claim_raw.PRC4_MODR_CD.alias('PRC4_MODR_CD').alias('mod4'),
#                              lit('IQVIA').alias('source_org_oid'))\
#                              .withColumn('start_date', )
#                             # .withColumn("PRC_VERS_TYP_ID", when(claim_df.PRC_VERS_TYP_ID == "10", "ICD10"))
#
#
# def to_problem(claim_raw: DataFrame) -> DataFrame:
#     return claim_raw.select(claim_raw.PATIENT_ID.alias('source_consumer_id'),
#                             claim_raw.SVC_FR_DT.alias('SVC_FR_DT').alias('start_date'),
#                             claim_raw.SVC_TO_DT.alias('SVC_TO_DT').alias('end_date'),
#                             claim_raw.DIAG_CD.alias('DIAG_CD').alias('code'),
#                             claim_raw.DIAG_VERS_TYP_ID.alias('DIAG_VERS_TYP_ID').alias('code_system'),
#                             # claim_raw.PAT_ZIP3.alias('???DESC').alias('desc'),
#                             lit('IQVIA').alias('source_org_oid'))
#
#
# def to_medication(claim_raw: DataFrame) -> DataFrame:
#     return claim_raw.select(claim_raw.PATIENT_ID.alias('source_consumer_id'),
#                             claim_raw.SVC_FR_DT.alias('SVC_FR_DT').alias('start_date'),
#                             claim_raw.SVC_TO_DT.alias('SVC_TO_DT').alias('end_date'),
#                             claim_raw.NDC_CD.alias('NDC_CD').alias('code'),
#                             lit('NDC').alias('NDC'),
#                             lit('IQVIA').alias('source_org_oid'))


    # .withColumn('new_column', lit(10))
                    # .withColumn("PRC_VERS_TYP_ID", when(claim_df.PRC_VERS_TYP_ID == "10", "ICD10"))
#                                  .when(test.CLAIM_TYP_CD == "P","OUTPATIENT")
#                                  # .when(test.CLAIM_TYP_CD.isNull() ,"")
#                                  .otherwise(test.CLAIM_TYP_CD))

#
# def to_procedure(claim_df: DataFrame) -> DataFrame:
#     a = claim_df.rdd.map(lambda r: Row(source_consumer_id=r.PATIENT_ID,
#                                        source_org_oid='',
#                                        type=,
#                                        type_raw=,
#                                        active='m',
#                                        active_raw='m',
#                                        active='m',
#                                        active='m',
#                                        active='m',
#                                        active='m',
#                                        active='m',
#                                        active='m',
#                                        active='m',
#                                        active='m',
#                                        active='m',
#                                        TMP=None))

    # mpi                text,
    # source_org_oid     text                not null,
    # source_consumer_id text                not null,
    # type               patient_type        not null,
    # type_raw           text                not null,
    # active             boolean             not null,
    # active_raw         text                not null,
    # prefix             text,
    # prefix_raw         text,
    # first_name         text,
    # first_name_raw     text,
    # middle_name        text,
    # middle_name_raw    text,
    # last_name          text,
    # last_name_raw      text,
    # gender             gender_type         not null,
    # gender_raw         text                not null,
    # dob                date                not null,
    # dob_raw            text                not null,
    # dod                date,
    # dod_raw            text,
    # ssn                text,
    # ssn_raw            text,
    # ethnicity          text,
    # ethnicity_raw      text,
    # race               text,
    # race_raw           text,
    # deceased           boolean,
    # deceased_raw       boolean,
    # marital_status     marital_status_type not null,
    # marital_status_raw marital_status_type not null,
    # managing_org       text                not null,
    # created_at         timestamp with time zone default CURRENT_TIMESTAMP,
    # created_by         text                not null,
    # updated_at         timestamp with time zone default CURRENT_TIMESTAMP,
    # updated_by         text                not null,
    # id                 serial

