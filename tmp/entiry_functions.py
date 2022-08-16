from functools import *
from datetime import datetime, timedelta, date
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, BooleanType
from pymonad.either import Left, Right, Either
import traceback
import json


all_codes = {}


def noop():
    pass


def to_date(date_source_column_value: str, source_column_name: str, date_format="%Y-%m-%d"):
    date_time_obj = None
    error = None
    try:
        date_time_obj = datetime.strptime(date_str, date_format).date()
    except Exception as e:
        e = traceback.format_exc()
        error = {'error': e, 'raw_value': date_str, 'date_format': date_format, 'source_column': source_column}

    return date_time_obj, error


def check_code(code_raw: str, code_system_raw: str, source_column: str, return_error=False):
    code = all_codes.get(f'{code_system_raw}:code_raw', None)

    if not return_error and code is not None:
        return code_raw
    elif return_error:
        None

        return {'source_column': source_column, 'error': f'CODE_SYSTEM:CODE {code_system_raw}:{code_raw} NOT FOUND'}


def code_desc(code_raw: str, code_system_raw: str, source_column: str, return_error=False):
    if not return_error:
        code = all_codes.get(f'{code_system_raw}:code_raw', None)

        if code is not None:
            return code['description']
        else:
            None
    else:
        return {'source_column': source_column, 'error': f'CODE_SYSTEM:CODE {code_system_raw}:{code_raw} NOT FOUND'}