from pyspark.sql import DataFrame
from pyspark import RDD
from pyspark.sql.functions import *
from pyspark import StorageLevel
from pyspark.sql import Row
from datetime import datetime, timedelta, date
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, BooleanType
from pymonad.either import Left, Right, Either
import traceback
from common.functions import *


def extract_left(*result: Either):
    error = []

    for r in result:
        if r.is_left():
            error.append(result.either(lambda x: x, lambda x: x))

    return error




def _to_procedure_row(claim_row: Row) -> Row:
    start_date_result = str_to_date(claim_row.SVC_FR_DT, 'SVC_FR_DT')
    to_date_result = str_to_date(claim_row.SVC_TO_DT, 'SVC_TO_DT')
    proc_code_system = check_code_system(claim_row.PRC_VERS_TYP_ID)
    proc_code = check_code(claim_row.PRC_CD, claim_row.PRC_VERS_TYP_ID)
    rev_code = check_code(claim_row.CLAIM_HOSP_REV_CD, 'REV') #TODO: hardcoded value
    nyec_desc = get_code_desc(claim_row.PRC_CD, claim_row.PRC_VERS_TYP_ID)
    is_record_valid = lambda x: False if (len(x) > 0) else True

    validation_errors = extract_left(*[start_date_result,
                                         to_date_result,
                                         proc_code,
                                         proc_code_system,
                                         rev_code])

    validation_warnings = extract_left(*[nyec_desc])

    valid = is_record_valid(validation_errors)
    warn = is_record_valid(validation_warnings)

    proc_row = Row(source_consumer_id=claim_row.PATIENT_ID,
                   source_org_oid=claim_row.source_org_oid,
                   start_date_raw=claim_row.SVC_FR_DT,
                   start_date=start_date_result.value,
                   to_date_raw=claim_row.SVC_TO_DT,
                   to_date=to_date_result.value,
                   code_raw=claim_row.PRC_CD,
                   code=proc_code.value,
                   code_system_raw=claim_row.PRC_VERS_TYP_ID,
                   code_system=proc_code_system.value,
                   revenue_code_raw=claim_row.CLAIM_HOSP_REV_CD,
                   revenue_code=rev_code.value,
                   desc=nyec_desc,
                   source_desc=None,
                   mod_raw=[claim_row.PRC1_MODR_CD, claim_row.PRC2_MODR_CD, claim_row.PRC3_MODR_CD, claim_row.PRC4_MODR_CD],
                   mod=[claim_row.PRC1_MODR_CD, claim_row.PRC2_MODR_CD, claim_row.PRC3_MODR_CD, claim_row.PRC4_MODR_CD],
                   error=validation_errors,
                   warning=validation_warnings,
                   is_valid=valid,
                   has_warnings=warn)

    return proc_row


def to_procedure(claim_raw: RDD) -> RDD:
    return claim_raw.map(lambda r: Row(**_to_procedure_row(r)))


def get_code(result: dict):

    if result is not None:
        result.get('code', None)

def _to_problem_row(claim_row: Row) -> Row:
    start_date_result = str_to_date(claim_row.SVC_FR_DT, 'SVC_FR_DT')
    to_date_result = str_to_date(claim_row.SVC_TO_DT, 'SVC_TO_DT')
    # diag_code_system = check_code_system(claim_row.DIAG_VERS_TYP_ID, 'DIAG_VERS_TYP_ID')
    diag_code = get_code(claim_row.DIAG_CD, claim_row.DIAG_VERS_TYP_ID, 'PRC_VERS_TYP_ID:DIAG_CD')

    # nyec_desc = get_code_desc(claim_row.DIAG_CD, claim_row.DIAG_VERS_TYP_ID, 'PRC_VERS_TYP_ID:DIAG_CD')
    is_record_valid = lambda x: False if (len(x) > 0) else True

    validation_errors = extract_left(*[start_date_result,
                                       to_date_result,
                                       proc_code,
                                       proc_code_system,
                                       rev_code])

    validation_warnings = extract_left(*[nyec_desc])

    valid = is_record_valid(validation_errors)
    warn = is_record_valid(validation_warnings)

    proc_row = Row(source_consumer_id=claim_row.PATIENT_ID,
                   source_org_oid=claim_row.source_org_oid,
                   start_date_raw=claim_row.SVC_FR_DT,
                   start_date=start_date_result.value,
                   to_date_raw=claim_row.SVC_TO_DT,
                   to_date=to_date_result.value,
                   code_raw=claim_row.PRC_CD,
                   code=proc_code.value,
                   code_system_raw=claim_row.PRC_VERS_TYP_ID,
                   code_system=proc_code_system.value,
                   revenue_code_raw=claim_row.CLAIM_HOSP_REV_CD,
                   revenue_code=rev_code.value,
                   desc=nyec_desc,
                   source_desc=None,
                   mod_raw=[claim_row.PRC1_MODR_CD, claim_row.PRC2_MODR_CD, claim_row.PRC3_MODR_CD,
                            claim_row.PRC4_MODR_CD],
                   mod=[claim_row.PRC1_MODR_CD, claim_row.PRC2_MODR_CD, claim_row.PRC3_MODR_CD, claim_row.PRC4_MODR_CD],
                   error=validation_errors,
                   warning=validation_warnings,
                   is_valid=valid,
                   has_warnings=warn)

    return proc_row


def to_problem(claim_raw: RDD) -> RDD:
    return claim_raw.map(lambda r: Row(**_to_problem_row(r)))