from functools import *
from datetime import datetime, timedelta, date
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, BooleanType
from pymonad.either import Left, Right, Either
import traceback
import json
from entiry_functions import *


procedure = {'source_consumer_id': None,
             'source_org_oid': None,
             'start_date_raw': None,
             'start_date': None,
             'to_date_raw': None,
             'to_date': None,
             'code_raw': None,
             'code': None,
             'code_system_raw': None,
             'code_system': None,
             'revenue_code_raw': None,
             'revenue_code': None,
             'mod_raw': None,
             'mod': None,
             'error': [],
             'warning': [],
             'is_valid': False,
             'has_warnings': True}


# def to_date(date_str: str, date_format="%Y-%m-%d", return_error=False):
#     try:
#         date_time_obj = datetime.strptime(date_str, date_format).date()
#     except Exception as e:
#         e = traceback.format_exc()
#         return Left({'error': e, 'raw_value': date_str})
#     return Right(date_time_obj)


# def to_date(date_str: str, date_format="%Y-%m-%d"):
#     result = _to_date(date_str, date_format)
#     if result.is_right():
#         return result.value
#     else:
#         return None


# def to_date_error(date_str: str, date_format="%Y-%m-%d"):
#     result = _to_date(date_str, date_format)
#     if result.is_left():
#         return result.either(lambda x: x, lambda x: x)
#     else:
#         return None

from entiry_functions import *
source_to_cdm_map = {'PATIENT_ID': 'source_consumer_id', 'SVC_FR_DT': 'start_date'}

list = [(k, v) for k, v in source_to_cdm_map.items()]


def add_error(error: list, msg: dict, source_column: str):
    if msg is not None:
        msg['source_column'] = source_column
        error.append(msg)


def source_to_cdm(source_dict: dict, source_to_cdm_map: dict):
    error = []

    add_error(to_date(source_dict['SVC_FR_DT'], 'SVC_FR_DT')[1])

    cdm_row = {}

    for source_column_name in source_to_cdm_map:
        cdm_column_name = source_to_cdm_map[source_column_name]

        cdm_row[cdm_column_name] = source_dict[source_column_name]


    return {'source_consumer_id': source_dict['PATIENT_ID'],
            'source_org_oid': source_dict['source_org_oid'],
            'start_date_raw': source_dict['SVC_FR_DT'],
            'start_date': to_date(source_dict['SVC_FR_DT'], 'SVC_FR_DT')[0],
            'to_date_raw': source_dict['SVC_TO_DT'],
            'to_date': to_date(row_dict['SVC_TO_DT'], 'SVC_TO_DT'),
            'code_raw': row_dict['PRC_CD'],
            'code': code,
            'code_system_raw': row_dict['PRC_VERS_TYP_ID'],
            'code_system': code_system,
            'revenue_code_raw': row_dict['CLAIM_HOSP_REV_CD'],
            'revenue_code': revenue_code,
            'mod_raw': [row_dict['PRC1_MODR_CD'], row_dict['PRC2_MODR_CD'], row_dict['PRC3_MODR_CD'],
                        row_dict['PRC4_MODR_CD']],
            'mod': [row_dict['PRC1_MODR_CD'], row_dict['PRC2_MODR_CD'], row_dict['PRC3_MODR_CD'],
                    row_dict['PRC4_MODR_CD']],
            error: }


def str_to_date(source_column_name: str, target_column_name: str, date_value: str, procedure: dict,
                date_format="%Y-%m-%d"):
    try:
        date_time_obj = datetime.strptime(date_value, date_format).date()
        procedure[target_column_name] = date_time_obj
        procedure[target_column_name + '_raw'] = date_time_obj
    except Exception as e:
        e = traceback.format_exc()
        procedure['error'].append(
            json.dumps({'source_column_name': source_column_name, 'error': e, 'raw_value': date_value}))


start_date_function = partial(str_to_date, source_column_name='SVC_DT', target_column_name='start_date',
                              procedure=procedure, date_format='%Y-%m-%d')
start_date_function('')
svc_dt_meta = {'function': str_to_date, 'target_column_name': 'start_date', 'date_format': '%Y-%m-%d'}
procedure_meta = {'SVC_DT': start_date_function}
