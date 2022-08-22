from pymonad.either import *


def to_standard_code_system(version_id: str, type_cd: str, source_column_name) -> Either:
    if version_id == '2':
        return Right('ICD10')
    elif version_id == '1':
        return Right('ICD9')
    elif version_id == '-1' and type_cd=='C':
        return Right('CPT')
    elif version_id == '-1' and type_cd=='H':
        return Right('HCPCS')
    else:
        error = {'error': f'Invalid version_id/type_cd combination',
                 'source_column_value': f'{version_id}:{type_cd}',
                 'source_column_name': source_column_name}
        return Left(error)


PATIENT = 'PATIENT'
PROCEDURE = 'PROCEDURE'
PROBLEM = 'PROBLEM'
DRUG = 'DRUG'
COST = 'COST'
CLAIM = 'CLAIM'
PRACTIONER = 'PRACTIONER'
PLAN = 'PLAN'

from iqvia.common.functions import *

def get_code_system(code_system_version: str, code_system_type: str):
    return to_standard_code_system(code_system_version, code_system_type, 'PRC_VERS_TYP_ID:PRC_TYP_CD').value


plan_list = None

def get_plan_list(pl_list: list):
    plan_list = pl_list

def get_plan_cache(plan_list: list):
    return dict([(row['IMS_PLN_ID'], row.asDict()) for row in plan_list])

# drug_cache = dict([(row['NDC_CD'], row.asDict()) for row in drug_list])
# provider_cache = dict([(row['PROVIDER_ID'], row.asDict()) for row in provider_list])
# problem_cache = dict([(row["DIAG_CD"] + ':' + get_code_system(row["DIAG_VERS_TYP_ID"], ''), dict(DIAG_SHORT_DESC=row['DIAG_SHORT_DESC'])) for row in problem_list])
# proc_cache = dict([(row["PRC_CD"] + ':' + get_code_system(row["PRC_VERS_TYP_ID"], row["PRC_TYP_CD"]), dict(PRC_SHORT_DESC=row['PRC_SHORT_DESC'])) for row in proc_list])

ref_cache = {PLAN: get_plan_cache(plan_list)}
# ref_cache = {PROCEDURE: proc_cache, PROBLEM: problem_cache, DRUG: drug_cache, PRACTIONER: provider_cache, PLAN: plan_cache}
print('------------------------>>>>>>> broadcasting reference data')
broadcast_cache = spark.sparkContext.broadcast(ref_cache)