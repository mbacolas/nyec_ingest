import random
from random import randint
from datetime import datetime, timedelta, date
import uuid
import string


def claims_header() -> list:
    columns = 'MONTH_ID, SVC_DT, PATIENT_ID, PAT_ZIP3, CLAIM_ID, SVC_NBR, DIAG_CD_POSN_NBR, CLAIM_TYP_CD, RENDERING_PROVIDER_ID, REFERRING_PROVIDER_ID, PLACE_OF_SVC_NM, PLAN_ID, PAY_TYP_DESC, DIAG_CD, DIAG_VERS_TYP_ID, PRC_CD, PRC_VERS_TYP_ID, PRC1_MODR_CD, PRC2_MODR_CD , PRC3_MODR_CD , PRC4_MODR_CD , NDC_CD , SVC_CRGD_AMT , UNIT_OF_SVC_AMT , HOSP_ADMT_DT , HOSP_DISCHG_DT , SVC_FR_DT , SVC_TO_DT , CLAIM_HOSP_REV_CD , FCLT_TYP_CD , ADMS_SRC_CD , ADMS_TYP_CD , ADMS_DIAG_CD , ADMS_DIAG_VERS_TYP_ID'.split(',')
    stripped_columns = [s.strip() for s in columns]
    return stripped_columns


def patient_header() -> list:
    return ['PATIENT_ID', 'PAT_BRTH_YR_NBR', 'PAT_GENDER_CD']


def svc_dt() -> str:
    """returns a random date of service from the last 1000 days
    """
    random_date = date.today() - timedelta(days=random.randrange(1, 1000))

    return random_date.strftime("%Y-%m-%d")


def month_id(svc_dt_str:str) -> str:
    return svc_dt_str.split('-')[1]
    # return '%02d' % svc_dt().split('-')[1]


def patient_id() -> str:
    return uuid.uuid4().hex[:12]


def patient_zip() -> str:
    return str(random.randrange(100, 999))


def claim_id() -> str:
    return str(uuid.uuid4().int)[:12]


def svc_nbr() -> int:
    return random.randrange(1, 10)


def diag_code_position_num() -> int:
    return random.randrange(1, 3)


def claim_type() -> str:
    return random.choice(['P', 'I'])


def rendering_provider_id() -> int:
    return claim_id()


def referring_provider_id() -> int:
    return claim_id()


def place_of_svc_nm() -> str:
    return random.choice(['CVS', 'Duane Reade'])


def plan_id() -> int:
    return claim_id()


def pay_type_desc() -> str:
    return random.choice(['CASH', 'MEDICAID', 'THIRD PARTY', 'MEDICARE', 'MEDICARE PART D', 'UNSPECIFIED'])


def diag_cd() -> str:
    index = random.randrange(1, 26)
    code_section = string.ascii_uppercase[index]
    code = str(random.randrange(1, 99))
    return code_section + code


def diag_vers() -> int:
    return 10


def proc_cd() -> int:
    return random.choice(['CPT', 'HCPCS', 'ICD10'])


def proc_cd_vers() -> int:
    return 10


def proc_cd_mod(code_system: str) -> str:
    if not code_system.startswith('ICD') and random.randrange(0,2) > 0:
        mod_index = str(random.randrange(22, 100))
        return 'CPT Modifier ' + mod_index
    else:
        return None


def ndc_cd() -> str:
    ndc = f'{random.randrange(1, 10)}{random.randrange(1, 10)}{random.randrange(1, 10)}{random.randrange(1, 10)}{random.randrange(1, 10)}' \
          f'-{random.randrange(1, 10)}{random.randrange(1, 10)}' \
          f'-{random.randrange(1, 10)}{random.randrange(1, 10)}'

    return ndc


def svc_crgd_amt() -> float:
    randint(100000, 1000*1000) / 100.00


def unit_of_svc_amt() -> float:
    randint(100000, 1000*1000) / 100.00


def hosp_admt_dt(claim_type_str: str) -> str:
    if claim_type_str == 'I':
        None
    else:
        None


def hosp_dischg_dt(claim_type_str: str) -> str:
    if claim_type_str == 'I':
        None
    else:
        None


def svc_fr_dt(date_of_svc_str: str) -> str:
    date_of_svc = datetime.strptime(date_of_svc_str, '%Y-%m-%d')
    from_dt = date_of_svc - timedelta(days=random.randrange(0, 10))

    return from_dt.strftime("%Y-%m-%d")


def svc_to_dt(date_of_svc_str: str) -> str:
    date_of_svc = datetime.strptime(date_of_svc_str, '%Y-%m-%d')
    to_dt = date_of_svc + timedelta(days=random.randrange(0, 10))

    return to_dt.strftime("%Y-%m-%d")


def rev_cd() -> str:
    return str(randint(240, 999))


def fclt_typ_cd() -> str:
    return random.choice(['11', '12', '86', '89'])


def adms_src_cd() -> str:
    return random.choice(['Physician Referral', 'Clinic Referral', 'HMO Referral', 'Transfer from Hospital',
                          'Transfer from SNF', 'Transfer From Another Health Care Facility',
                          'Emergency Room', 'Court/Law Enforcement', 'Information Not Available'])


def adms_type_diag_cd() -> str:
    return random.choice(['Emergency', 'Urgent', 'Newborn', 'Trauma', 'Information Not Available'])


def adms_diag_cd() -> str:
    return diag_cd()


def adms_diag_vers() -> int:
    return 10


def patient_dob_year() -> int:
    random_date = date.today() - timedelta(days=random.randrange(18*365, 90*365))

    return random_date.year


def patient_gender() -> str:
    return random.choice(['M', 'F', 'U'])
