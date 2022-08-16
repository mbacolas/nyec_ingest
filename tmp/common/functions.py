from functools import *
from datetime import datetime, timedelta, date
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, BooleanType
from pymonad.either import Left, Right, Either
import traceback
import json


all_codes = {}
all_code_systems = {'ICD10'}


def str_to_date(date_raw: str, source_column_name: str, date_format="%Y-%m-%d") -> Either:
    try:
        date_time_obj = datetime.strptime(date_raw, date_format).date()
    except Exception as e:
        e = traceback.format_exc()
        error = {'error': e, 'raw_value': date_raw, 'date_format': date_format, 'source_column': source_column_name}
        return Left(json.dumps(error))
    return Right(date_time_obj)


def get_code(code_raw: str, code_system_raw: str, source_column_name: str) -> Either:
    code_dict = all_codes.get(f'{code_system_raw}:{code_raw}', None)

    if code_dict is not None:
        return Right(code_dict)
    else:
        error = {'source_column_name': 'code_system:code',
                 'error': f'CODE_SYSTEM:CODE {code_system_raw}:{code_raw} NOT FOUND',
                 'source_column_value': f'{code_system_raw}:{code_raw}',
                 'source_column_name': source_column_name}
        return Left(json.dumps(error))


def check_code_system(code_system_raw: str, source_column_name: str) -> Either:
    code_system = all_code_systems.get(f'{code_system_raw}', None)

    if code_system is not None:
        return Right(code_system_raw)
    else:
        error = {'source_column_name': 'code_system', 'error': f'CODE_SYSTEM {code_system_raw} NOT FOUND',
                 'source_column_value': code_system_raw, 'source_column_name': source_column_name}
        return Left(json.dumps(error))


def get_code_desc(code_raw: str, code_system_raw: str, source_column_name: str) -> Either:
    code_dict = all_codes.get(f'{code_system_raw}:{code_raw}', None)
    desc = code_dict.get('desc', None)

    if code_dict is not None and desc is not None:
        return Right(desc)
    else:
        warn = {'source_column_name': 'code_system', 'warning': f'CODE_SYSTEM {code_system_raw} NOT FOUND',
                 'source_column_value': code_system_raw, 'source_column_name': source_column_name}
        return Left(warn)

#
# def extract_error(result: Either) -> str:
#     if result.is_left():
#         return result.either(lambda x: x, lambda x: x)
#     else:
#         return None


