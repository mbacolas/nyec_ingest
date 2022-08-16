from functools import *
from datetime import datetime, timedelta, date
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, BooleanType
from pymonad.either import Left, Right, Either
import traceback
import json


all_codes = {}
all_code_systems = {'ICD10'}
NDC = 'NDC'

def str_to_date(date_raw: str, source_column_name: str, date_format="%Y%m%d") -> Either:
    try:
        if date_raw is None:
            return Right(None)
        date_time_obj = datetime.strptime(date_raw, date_format).date()
    except Exception as e:
        e = traceback.format_exc()
        error = {'error': e, 'raw_value': date_raw, 'date_format': date_format, 'source_column': source_column_name}
        return Left(json.dumps(error))
    return Right(date_time_obj)


#TODO: REMOVE once ref data is loaded
def tmp_function(code_system_raw: str, code_raw:str):
    search_key = f'{code_system_raw}:{code_raw}'
    if code_system_raw is None or code_raw is None:
        return None
    else:
        return {'code': search_key.split(':')[0], 'code_system': search_key.split(':')[1], 'desc': 'UNKNOWN'}

# get_code(claim_row.PRC_CD, all_code_system_result.value, 'PRC_CD')
def get_code(code_raw: str, code_system_raw: str, source_column_name: str) -> Either:
    # code_dict = all_codes.get(f'{code_system_raw}:{code_raw}', None)
    code_dict = tmp_function(code_system_raw, code_raw)
    if code_dict is not None:
        return Right(code_dict)
    else:
        error = {'error': f'CODE_SYSTEM:CODE {code_system_raw}:{code_raw} NOT FOUND',
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


def is_valid(obj: str, source_column_name: str, enum: list) -> Either:
    if obj in enum:
        return Right(obj)
    else:
        error = {'source_column_name': source_column_name,
                 'error': f'{source_column_name} IS NOT IN THE INCLUDED ENUM {enum}',
                 'source_column_value': obj}
        return Left(json.dumps(error))


def is_null(obj: str, source_column_name: str) -> Either:
    if obj is not None:
        return Right(obj)
    else:
        error = {'source_column_name': source_column_name,
                 'error': f'{source_column_name} value is None/Null',
                 'source_column_value': obj}
        return Left(json.dumps(error))

def to_claim_type(source_type: str) -> Either:
    if source_type == 'I':
        return Right('INSTITUTIONAL')
    elif source_type == 'P':
        return Right('PROFESSIONAL')
    else:
        error = {'source_column_name': 'CLAIM_TYP_CD', 'error': f'exepected value (I or P) not found',
                 'source_column_value': source_type}
        return Left(json.dumps(error))


def validate_facility_type_cd(cd: str) -> Either:
    """1 - Hospital
        2 - Skilled Nursing
        3 - Home Health
        4 - Religious Nonmedical Health Care Facility (Hospital)
        5 - Religious Nonmedical Health Care Facility (Extended Care)
        7 - Clinic
        8 - Specialty Facility, Hospital ASC Surgery"""
    valid_codes = ["{:2}".format(x) for x in range(1,8, 1)]
    if cd in valid_codes:
        return Right(cd)
    else:
        error = {'source_column_name': 'FCLT_TYP_CD', 'error': f'invalid facility type cd', 'source_column_value': cd}
        return Left(json.dumps(error))


def validate_admission_source_cd(cd: str) -> Either:
    """
        Code	Code value
        0	ANOMALY: invalid value, if present, translate to '9'
        1	Non-Health Care Facility Point of Origin (Physician Referral) — The patient was admitted to this facility upon an order of a physician.
        2	Clinic referral — The patient was admitted upon the recommendation of this facility's clinic physician.
        3	HMO referral — Reserved for national Prior to 3/08, HMO referral — The patient was admitted upon the recommendation of a health maintenance organization (HMO) physician.
        4	Transfer from hospital (Different Facility) — The patient was admitted to this facility as a hospital transfer from an acute care facility where he or she was an inpatient.
        5	Transfer from a skilled nursing facility (SNF) or Intermediate Care Facility (ICF) — The patient was admitted to this facility as a transfer from a SNF or ICF where he or she was a resident.
        6	Transfer from another health care facility — The patient was admitted to this facility as a transfer from another type of health care facility not defined elsewhere in this code list where he or she was an inpatient.
        7	Emergency room — The patient was admitted to this facility after receiving services in this facility's emergency room department (CMS discontinued this code 07/2010, although a small number of claims with this code appear after that time).
        8	Court/law enforcement — The patient was admitted upon the direction of a court of law or upon the request of a law enforcement agency's representative.
        9	Information not available — The means by which the patient was admitted is not known.
        A	Reserved for National Assignment. (eff. 3/08) Prior to 3/08 defined as: Transfer from a Critical Access Hospital — patient was admitted/referred to this facility as a transfer from a Critical Access Hospital.
        B	Transfer from Another Home Health Agency — The patient was admitted to this home health agency as a transfer from another home health agency. (Discontinued July 1, 2010 — Reference Condition Code 47)
        C	Readmission to Same Home Health Agency — The patient was readmitted to this home health agency within the same home health episode period. (Discontinued July 1, 2010)
        D	Transfer from hospital inpatient in the same facility resulting in a separate claim to the payer — The patient was admitted to this facility as a transfer from hospital inpatient within this facility resulting in a separate claim to the payer.
        E	Transfer from Ambulatory Surgical Center
        F	Transfer from hospice and is under a hospice plan of care or enrolled in hospice program
        1	Normal delivery — A baby delivered without complications.
        2	Premature delivery — A baby delivered with time and/or weight factors qualifying it for premature status.
        3	Sick baby — A baby delivered with medical complications, other than those relating to premature status.
        4	Extramural birth — A baby delivered in a nonsterile environment.
        5	Reserved for national assignment.
        6	Reserved for national assignment.
        7	Reserved for national assignment.
        8	Reserved for national assignment.
        9	Information not available."""
    return Right(cd) #TODO: should this come from ref data


def validate_admission_type_cd(cd: str) -> Either:
    """
        Code	Code value
        0	Blank
        1	Emergency - The patient required immediate medical intervention as a result of severe, life threatening, or potentially disabling conditions. Generally, the patient was admitted through the emergency room.
        2	Urgent - The patient required immediate attention for the care and treatment of a physical or mental disorder. Generally, the patient was admitted to the first available and suitable accommodation.
        3	Elective - The patient's condition permitted adequate time to schedule the availability of suitable accommodations.
        4	Newborn - Necessitates the use of special source of admission codes.
        5	Trauma Center - visits to a trauma center/hospital as licensed or designated by the State or local government authority authorized to do so, or as verified by the American College of Surgeons and involving a trauma activation.
        6   THRU 8	Reserved
        9	Unknown - Information not available."""
    pass



