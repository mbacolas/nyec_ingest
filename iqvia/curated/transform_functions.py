from pyspark import RDD
from pyspark.sql import Row
import uuid
from common.functions import *
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pymonad.either import *
# from staging_ingest_processor import ref_lookup

RENDERING = 'RENDERING'
REFERRING = 'REFERRING'

PATIENT = 'PATIENT'
PROCEDURE = 'PROCEDURE'
PROBLEM = 'PROBLEM'
DRUG = 'DRUG'
COST = 'COST'
CLAIM = 'CLAIM'
PRACTIONER = 'PRACTIONER'
PLAN = 'PLAN'

# def ref_lookup(event_type: str, key: str):
#     return broadcast_cache.value[event_type].get(key, {})

def is_record_valid(rec: list):
    if len(rec) > 0:
        return False
    else:
        return True


def extract_left(*result: Either):
    error = []
    for r in result:
        if r.is_left():
            error.append(r.either(lambda x: x, lambda x: x))
    return error


def extract_code(result: Either) -> dict:
    if result.is_right():
        return result.value
    else:
        return {}


# RENDERING_PROVIDER_TYP_ID
# 1-PROFESSIONAL
# 2 -ORGANIZATION
def validate_provider_type(provider_type: str) -> Either:
    if provider_type == '1':
        return Right('PROFESSIONAL')
    elif provider_type == '2':
        return Right('ORGANIZATION')
    else:
        error = {'source_column_name': 'PROVIDER_TYP_ID', 'error': f'invalid PROVIDER_TYP_ID', 'source_column_value': provider_type}
        return Left(json.dumps(error))


# from iqvia.common.functions import *
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


def _to_procedure_row(claim_row: Row, ref_lookup) -> Row:
    start_date_result = str_to_date(claim_row.SVC_FR_DT, 'SVC_FR_DT')
    to_date_result = str_to_date(claim_row.SVC_TO_DT, 'SVC_TO_DT', False)

    cached_proc = ref_lookup(PROCEDURE, claim_row.PRC_CD + ':' + claim_row.PRC_VERS_TYP_ID)
    code_system = cached_proc.get('PRC_TYP_CD', None)
    all_code_system_result = to_standard_code_system(claim_row.PRC_VERS_TYP_ID, code_system, 'PRC_VERS_TYP_ID:PRC_TYP_CD')

    proc_code_result = get_code(claim_row.PRC_CD, all_code_system_result.value, 'PRC_CD')
    rev_code_result = get_code(claim_row.CLAIM_HOSP_REV_CD, 'REV', 'CLAIM_HOSP_REV_CD')  # TODO: hardcoded value
    proc_code=extract_code(proc_code_result)
    rev_code=extract_code(rev_code_result)
    validation_errors = extract_left(*[all_code_system_result,
                                       start_date_result,
                                       to_date_result,
                                       proc_code_result,
                                       rev_code_result])
    valid = is_record_valid(validation_errors)
    validation_warnings = []
    warn = False
    modifiers = [claim_row.PRC1_MODR_CD, claim_row.PRC2_MODR_CD, claim_row.PRC3_MODR_CD, claim_row.PRC4_MODR_CD]
    # modifiers = [{claim_row.PRC1_MODR_CD, claim_row.PRC2_MODR_CD, claim_row.PRC3_MODR_CD, claim_row.PRC4_MODR_CD]
    modifiers_clean = [i for i in modifiers if i is not None]

    proc_row = Row(id=uuid.uuid4().hex[:12],
                   source_consumer_id=claim_row.PATIENT_ID,
                   source_org_oid=claim_row.source_org_oid,
                   start_date_raw=claim_row.SVC_FR_DT,
                   start_date=start_date_result.value,
                   to_date_raw=claim_row.SVC_TO_DT,
                   to_date=to_date_result.value,
                   code_raw=claim_row.PRC_CD,
                   code=proc_code.get('code', None),
                   code_system_raw=code_system,
                   # code_system_raw=claim_row.PRC_TYP_CD,
                   code_system=proc_code.get('code_system', None),
                   revenue_code_raw=claim_row.CLAIM_HOSP_REV_CD,
                   revenue_code=rev_code.get('code', None),
                   desc=proc_code.get('desc', None),
                   source_desc=cached_proc.get('PRC_SHORT_DESC', None),
                   # source_desc=claim_row.PRC_SHORT_DESC,
                   mod_raw=modifiers_clean,
                   mod=modifiers_clean,
                   error=validation_errors,
                   warning=validation_warnings,
                   is_valid=valid,
                   has_warnings=warn,
                   batch_id=claim_row.batch_id,
                   date_created=claim_row.date_created)
    return proc_row


def to_procedure(claim_rdd: RDD, ref_lookup) -> RDD:
    return claim_rdd.filter(lambda r: r.PRC_CD is not None)\
                    .map(lambda r: _to_procedure_row(r, ref_lookup))\
                    .keyBy(lambda r: (r.source_org_oid, r.source_consumer_id, r.start_date, r.code_raw, r.code_system_raw)) \
                    .reduceByKey((lambda a,b: a))\
                    .map(lambda r: r[1])


def _to_problem_row(claim_row: Row, ref_lookup) -> Row:
    start_date_result = str_to_date(claim_row.SVC_FR_DT, 'SVC_FR_DT')
    to_date_result = str_to_date(claim_row.SVC_TO_DT, 'SVC_TO_DT', False)
    all_code_system_result = to_standard_code_system(claim_row.DIAG_VERS_TYP_ID, claim_row.DIAG_CD,
                                                     'DIAG_VERS_TYP_ID:DIAG_CD')
    diag_code_result = get_code(claim_row.DIAG_CD, all_code_system_result.value, 'DIAG_VERS_TYP_ID:DIAG_CD')
    diag_code = extract_code(diag_code_result)
    validation_errors = extract_left(*[all_code_system_result,
                                       start_date_result,
                                       to_date_result,
                                       diag_code_result])
    valid = is_record_valid(validation_errors)
    validation_warnings = []
    warn = False
    source_desc = ref_lookup(PROBLEM, f'{diag_code.get("code", None)}:{diag_code.get("code_system", None)}').get('DIAG_SHORT_DESC', None)

    diag_row = Row(id=uuid.uuid4().hex[:12],
                   source_consumer_id=claim_row.PATIENT_ID,
                   source_org_oid=claim_row.source_org_oid,
                   start_date_raw=claim_row.SVC_FR_DT,
                   start_date=start_date_result.value,
                   to_date_raw=claim_row.SVC_TO_DT,
                   to_date=to_date_result.value,
                   code_raw=claim_row.DIAG_CD,
                   code=diag_code.get('code', None),
                   code_system_raw=claim_row.DIAG_VERS_TYP_ID,
                   code_system=diag_code.get('code_system', None),
                   desc=diag_code.get('desc', None),
                   source_desc=source_desc,
                   # source_desc=claim_row.DIAG_SHORT_DESC,
                   is_admitting=False,
                   error=validation_errors,
                   warning=validation_warnings,
                   is_valid=valid,
                   has_warnings=warn,
                   batch_id=claim_row.batch_id,
                   date_created=claim_row.date_created)

    return diag_row


def to_problem(claim_rdd: RDD, ref_lookup) -> RDD:
    return claim_rdd.filter(lambda r: r.DIAG_CD is not None)\
                    .map(lambda r: _to_problem_row(r, ref_lookup))\
                    .keyBy(lambda r: (r.source_org_oid, r.source_consumer_id, r.start_date, r.code_raw, r.code_system_raw)) \
                    .reduceByKey((lambda a, b: a))\
                    .map(lambda r: r[1])


def _to_admitting_diagnosis(claim_row: Row) -> Row:
    start_date_result = str_to_date(claim_row.HOSP_ADMT_DT, 'HOSP_ADMT_DT')
    to_date_result = str_to_date(claim_row.HOSP_DISCHG_DT, 'HOSP_DISCHG_DT')
    all_code_system_result = to_standard_code_system(claim_row.ADMS_DIAG_VERS_TYP_ID, None, 'ADMS_DIAG_VERS_TYP_ID')
    diag_code_result = get_code(claim_row.ADMS_DIAG_CD, all_code_system_result.value, 'ADMS_DIAG_CD')
    diag_code = extract_code(diag_code_result)
    validation_errors = extract_left(*[all_code_system_result,
                                       start_date_result,
                                       to_date_result,
                                       diag_code_result])
    valid = is_record_valid(validation_errors)
    validation_warnings = []
    warn = False

    diag_row = Row(id=uuid.uuid4().hex[:12],
                   source_consumer_id=claim_row.PATIENT_ID,
                   source_org_oid=claim_row.source_org_oid,
                   start_date_raw=claim_row.HOSP_ADMT_DT,
                   start_date=start_date_result.value,
                   to_date_raw=claim_row.HOSP_DISCHG_DT,
                   to_date=to_date_result.value,
                   code_raw=claim_row.ADMS_DIAG_CD,
                   code=diag_code.get('code', None),
                   code_system_raw=claim_row.ADMS_DIAG_VERS_TYP_ID,
                   code_system=diag_code.get('code_system', None),
                   desc=diag_code.get('desc', None),
                   source_desc=None,
                   is_admitting=True,
                   error=validation_errors,
                   warning=validation_warnings,
                   is_valid=valid,
                   has_warnings=warn,
                   batch_id=claim_row.batch_id,
                   date_created=claim_row.date_created)
    return diag_row


def to_admitting_diagnosis(claim_rdd: RDD) -> RDD:
    return claim_rdd.filter(lambda r: r.ADMS_DIAG_CD is not None)\
                    .map(lambda r: _to_admitting_diagnosis(r))\
                    .keyBy(lambda r: (r.source_consumer_id, r.start_date, r.code_raw, r.code_system_raw)) \
                    .reduceByKey((lambda a, b: a))\
                    .map(lambda r: r[1])


def _to_drug_row(claim_row: Row, ref_lookup) -> Row:
    start_date_result = str_to_date(claim_row.SVC_FR_DT, 'SVC_FR_DT')
    to_date_result = str_to_date(claim_row.SVC_TO_DT, 'SVC_TO_DT', False)
    drug_code_result = get_code(claim_row.NDC_CD, 'NDC_CD', 'NDC_CD')
    drug_code = extract_code(drug_code_result)
    validation_errors = extract_left(*[start_date_result,
                                       to_date_result,
                                       drug_code_result])
    valid = is_record_valid(validation_errors)
    validation_warnings = []
    warn = False
    cached_drug = ref_lookup(DRUG, drug_code.get("code", None))

    drug_row = Row(id=uuid.uuid4().hex[:12],
                   source_consumer_id=claim_row.PATIENT_ID,
                   source_org_oid=claim_row.source_org_oid,
                   start_date_raw=claim_row.SVC_FR_DT,
                   start_date=start_date_result.value,
                   to_date_raw=claim_row.SVC_TO_DT,
                   to_date=to_date_result.value,
                   code_raw=claim_row.NDC_CD,
                   code=drug_code.get('code', None),
                   code_system_raw=NDC,
                   code_system=drug_code.get('code_system', None),
                   desc=drug_code.get('desc', None),
                   source_desc=cached_drug.get('MKTED_PROD_NM', None),
                   # source_desc=claim_row.MKTED_PROD_NM,
                   strength_raw=cached_drug.get('STRNT_DESC', None),
                   # strength_raw=claim_row.STRNT_DESC,
                   strength=cached_drug.get('STRNT_DESC', None),
                   # strength=claim_row.STRNT_DESC,
                   form=cached_drug.get('DOSAGE_FORM_NM', None),
                   # form=claim_row.DOSAGE_FORM_NM,
                   form_raw=cached_drug.get('DOSAGE_FORM_NM', None),
                   # form_raw=claim_row.DOSAGE_FORM_NM,
                   classification=cached_drug.get('USC_DESC', None),
                   # classification=claim_row.USC_DESC,
                   classification_raw=cached_drug.get('USC_DESC', None),
                   # classification_raw=claim_row.USC_DESC,
                   error=validation_errors,
                   warning=validation_warnings,
                   is_valid=valid,
                   has_warnings=warn,
                   batch_id=claim_row.batch_id,
                   date_created=claim_row.date_created)
    return drug_row


def to_drug(claim_rdd: RDD, ref_lookup) -> RDD:
    return claim_rdd \
        .filter(lambda r: r.NDC_CD is not None) \
        .map(lambda r: _to_drug_row(r, ref_lookup))\
        .keyBy(lambda r: (r.source_org_oid, r.source_consumer_id, r.start_date, r.code_raw, r.code_system_raw)) \
        .reduceByKey((lambda a, b: a))\
        .map(lambda r: r[1])


def is_inpatient(hosp_admt_dt: str):
    if hosp_admt_dt is None:
        return 'INPATIENT'
    else:
        return 'OUTPATIENT'


def _to_patient_row(patient_plan: Row) -> Row:
    # org_oid = is_valid(patient_plan.IMS_PAYER_ID, 'IMS_PAYER_ID')
    dob = str_to_date(f'{patient_plan.PAT_BRTH_YR_NBR}-01-01', 'patient.dob', date_format='%Y-%m-%d')
    gender = is_included(patient_plan.PAT_GENDER_CD, 'PAT_GENDER_CD', ['M', 'F', 'U'])
    validation_errors = extract_left(*[dob, gender])
    validation_warnings = []
    valid = is_record_valid(validation_errors)
    warn = False
    patient_row = Row(id=uuid.uuid4().hex[:12],
                      source_consumer_id=patient_plan.PATIENT_ID,
                      source_org_oid=patient_plan.source_org_oid,
                      type=patient_plan.consumer_type,
                      active=patient_plan.consumer_status,
                      dob_raw=patient_plan.PAT_BRTH_YR_NBR,
                      dob=dob.value,
                      gender_raw=patient_plan.PAT_GENDER_CD,
                      gender=gender.value,
                      error=validation_errors,
                      warning=validation_warnings,
                      is_valid=valid,
                      has_warnings=warn,
                      batch_id=patient_plan.batch_id,
                      date_created=patient_plan.date_created)
    return patient_row


def to_patient(patient_plan_rdd: RDD) -> RDD:
    return patient_plan_rdd.map(lambda r: _to_patient_row(r))\
                        .keyBy(lambda r: (r.source_consumer_id)) \
                        .reduceByKey((lambda a, b: a))\
                        .map(lambda r: r[1])

# def _to_org_row(patient_plan: Row) -> Row:
#     row_id = uuid.uuid4().hex[:12]
#     org_row = Row(id=row_id,
#                    source_org_oid=patient_plan.source_org_oid,
#                    name=patient_plan.source_org_oid,
#                    type=patient_plan.org_type,
#                    active=True,
#                    error=[],
#                    warning=[],
#                    is_valid=True,
#                    has_warnings=False,
#                    batch_id=patient_plan.batch_id,
#                    date_created=patient_plan.date_created)
#     return org_row
#
#
# def to_org(patient_plan_rdd: RDD) -> RDD:
#     return patient_plan_rdd.map(lambda r: _to_org_row(r))


def _to_eligibility_row(patient_plan_rdd: Row) -> Row:
    pass #TODO: not data to implement


def to_eligibility(patient_plan_rdd: RDD) -> RDD:
    return patient_plan_rdd.map(lambda r: _to_eligibility_row(r))


def _to_cost_row(claim_row: Row) -> Row:
    paid_amount_result = is_number(claim_row.SVC_CRGD_AMT, 'SVC_CRGD_AMT')
    validation_errors = extract_left(*[paid_amount_result])
    valid = is_record_valid(validation_errors)
    validation_warnings = []
    warn = False
    cost_row = Row(id=uuid.uuid4().hex[:12],
                   source_consumer_id=claim_row.PATIENT_ID,
                    source_org_oid=claim_row.source_org_oid,
                    claim_identifier=claim_row.CLAIM_ID,
                    service_number=claim_row.SVC_NBR,
                    paid_amount_raw=claim_row.SVC_CRGD_AMT,
                    paid_amount=paid_amount_result.value,
                    error=validation_errors,
                    warning=validation_warnings,
                    is_valid=valid,
                    has_warnings=warn,
                    batch_id=claim_row.batch_id,
                    date_created=claim_row.date_created)
    return cost_row


def to_cost(claim_row_rdd: RDD) -> RDD:
    return claim_row_rdd.map(lambda r: _to_cost_row(r)) \
                    .keyBy(lambda r: (r.source_org_oid, r.source_consumer_id, r.claim_identifier, r.service_number, r.paid_amount)) \
                    .reduceByKey((lambda a, b: a))\
                    .map(lambda r: r[1])


def _to_claim_row(claim_row: Row, ref_lookup) -> Row:
    source_claim_type = to_claim_type(claim_row.CLAIM_TYP_CD)
    start_date_result = str_to_date(claim_row.SVC_FR_DT, 'SVC_FR_DT')
    to_date_result = str_to_date(claim_row.SVC_TO_DT, 'SVC_TO_DT', is_requied=False)
    admission_date_result = str_to_date(claim_row.HOSP_ADMT_DT, 'HOSP_ADMT_DT', is_requied=False)
    discharge_date_result = str_to_date(claim_row.HOSP_DISCHG_DT, 'HOSP_DISCHG_DT', is_requied=False) #should be True
    facility_type_cd_result = validate_facility_type_cd(claim_row.FCLT_TYP_CD)
    admission_source_cd_result = validate_admission_source_cd(claim_row.ADMS_SRC_CD)
    admission_type_cd_result = validate_admission_type_cd(claim_row.ADMS_TYP_CD)
    validation_errors = extract_left(*[start_date_result])

    if admission_date_result.value is not None:
        validation_warnings = extract_left(*[source_claim_type,
                                           facility_type_cd_result,
                                           admission_source_cd_result,
                                           admission_type_cd_result])
    else:
        validation_warnings = extract_left(*[source_claim_type])
    valid = is_record_valid(validation_errors)
    warn = is_record_valid(validation_warnings)

    cached_plan = ref_lookup(PLAN, claim_row.PLAN_ID)
    # {'PLAN_ID': '20947', 'IMS_PLN_ID': '735', 'IMS_PLN_NM': 'UHC MED ADV GENERAL (HI)', 'IMS_PAYER_ID': '2429',
    #  'IMS_PAYER_NM': 'UHC/PACIFICARE/AARP MED D', 'PLANTRACK_ID': '0024290735', 'MODEL_TYP_CD': 'MED ADVG',
    #  'MODEL_TYP_NM': 'GENERAL MEDICARE D ADVANTAGE', 'IMS_PBM_ADJUDICATING_ID': '65000',
    #  'IMS_PBM_ADJUDICATING_NM': 'OPTUMRX (PROC UNSPEC)', 'org_type': 'THIRD PARTY CLAIMS AGGREGATOR',
    #  'plan_status': True}
    claim_stage_row = Row(id=uuid.uuid4().hex[:12],
                           source_consumer_id=claim_row.PATIENT_ID,
                            source_org_oid=claim_row.source_org_oid,
                            payer_name=cached_plan.get('IMS_PAYER_NM', None),
                            # payer_name=claim_row.IMS_PAYER_NM,
                            payer_id=cached_plan.get('IMS_PLN_ID', None),
                            # payer_id=claim_row.IMS_PLN_ID,
                            plan_name=cached_plan.get('IMS_PLN_NM', None),
                            # plan_name=claim_row.IMS_PLN_NM,
                            plan_id=claim_row.PLAN_ID,
                            claim_identifier=claim_row.CLAIM_ID,
                            service_number=claim_row.SVC_NBR,
                            type_raw=claim_row.CLAIM_TYP_CD,
                            type=source_claim_type.value,
                            sub_type_raw=claim_row.HOSP_ADMT_DT,
                            sub_type=is_inpatient(claim_row.HOSP_ADMT_DT),
                            start_date_raw=claim_row.SVC_FR_DT,
                            start_date=start_date_result.value,
                            end_date_raw=claim_row.SVC_TO_DT,
                            end_date=to_date_result.value,
                            admission_date_raw=claim_row.HOSP_ADMT_DT,
                            admission_date=admission_date_result.value,
                            discharge_date_raw=claim_row.HOSP_DISCHG_DT,
                            discharge_date=discharge_date_result.value,
                            units_of_service_raw=claim_row.UNIT_OF_SVC_AMT,
                            units_of_service=claim_row.UNIT_OF_SVC_AMT,
                            facility_type_cd_raw=claim_row.FCLT_TYP_CD,
                            facility_type_cd=claim_row.FCLT_TYP_CD,
                            admission_source_cd_raw=claim_row.ADMS_SRC_CD,
                            admission_source_cd=claim_row.ADMS_SRC_CD,
                            admission_type_cd_raw=claim_row.ADMS_TYP_CD,
                            admission_type_cd=claim_row.ADMS_TYP_CD,
                            place_of_service_raw=claim_row.PLACE_OF_SVC_NM,
                            place_of_service=claim_row.PLACE_OF_SVC_NM,
                            error=validation_errors,
                            warning=validation_warnings,
                            is_valid=valid,
                            has_warnings=warn,
                            batch_id=claim_row.batch_id,
                            date_created=claim_row.date_created)
    return claim_stage_row


def to_claim(claim_raw: RDD, ref_lookup) -> RDD:
    return claim_raw.map(lambda r: _to_claim_row(r, ref_lookup))\
                    .keyBy(lambda r: (r.source_org_oid, r.source_consumer_id, r.claim_identifier)) \
                    .reduceByKey((lambda a, b: a))\
                    .map(lambda r: r[1])


def _to_practitioner_row(claim_row: Row, ref_lookup) -> Row:
    providers = []

    if claim_row.RENDERING_PROVIDER_ID is not None:
        cached_provider = ref_lookup(PRACTIONER, claim_row.RENDERING_PROVIDER_ID)
        source_provider_type_result = validate_provider_type(cached_provider.get('PROVIDER_TYP_ID', None))
        # source_provider_type_result = validate_provider_type(claim_row.RENDERING_PROVIDER_TYP_ID)
        validation_errors = extract_left(*[source_provider_type_result])
        valid = is_record_valid(validation_errors)
        validation_warnings = []
        warn = False

        # {'PROVIDER_ID': '10151134',
        #  'PROVIDER_TYP_ID': '1',
        #  'ORG_NM': None,
        #  'IMS_RXER_ID': '4596454',
        #  'LAST_NM': 'NA', 'FIRST_NM': 'CHANGRIM', 'ADDR_LINE1_TXT': '3501 STOCKDALE HWY',
        #              'ADDR_LINE2_TXT': None, 'CITY_NM': 'BAKERSFIELD', 'ST_CD': 'CA', 'ZIP': '93309',
        # 'PRI_SPCL_CD': 'GPM',
        #  'PRI_SPCL_DESC': 'GENERAL PREVENTIVE MEDICINE', 'ME_NBR': '0051808117',
        # 'NPI': '1427290543'}
        rendering_provider_row = Row(id=uuid.uuid4().hex[:12],
                                     npi=cached_provider.get('NPI', None),
                                     # npi=claim_row.RENDERING_NPI,
                                    source_org_oid=claim_row.source_org_oid,
                                    first_name=cached_provider.get('FIRST_NM', None),
                                    # first_name=claim_row.RENDERING_FIRST_NM,
                                    last_name=cached_provider.get('FIRST_NM', None),
                                    # last_name=claim_row.RENDERING_LAST_NM,
                                    source_provider_id=claim_row.RENDERING_PROVIDER_ID,
                                    provider_type_raw=cached_provider.get('PROVIDER_TYP_ID', None),
                                    # provider_type_raw=claim_row.REFERRING_PROVIDER_TYP_ID,
                                    provider_type=source_provider_type_result.value,
                                    role=RENDERING,
                                    claim_identifier=claim_row.CLAIM_ID,
                                    service_number=claim_row.SVC_NBR,
                                    active=True,
                                    error=validation_errors,
                                    warning=validation_warnings,
                                    is_valid=valid,
                                    has_warnings=warn,
                                    batch_id=claim_row.batch_id,
                                    date_created=claim_row.date_created)
        providers.append(rendering_provider_row)

    if claim_row.REFERRING_PROVIDER_ID is not None:
        cached_provider = ref_lookup(PRACTIONER, claim_row.REFERRING_PROVIDER_ID)
        source_provider_type_result = validate_provider_type(cached_provider.get('PROVIDER_TYP_ID', None))
        # source_provider_type_result = validate_provider_type(claim_row.REFERRING_PROVIDER_TYP_ID)
        validation_errors = extract_left(*[source_provider_type_result])
        valid = is_record_valid(validation_errors)
        validation_warnings = []
        warn = False
        ref_provider_row = Row(id=uuid.uuid4().hex[:12],
                                     npi=cached_provider.get('NPI', None),
                                     # npi=claim_row.RENDERING_NPI,
                                    source_org_oid=claim_row.source_org_oid,
                                    first_name=cached_provider.get('FIRST_NM', None),
                                    # first_name=claim_row.RENDERING_FIRST_NM,
                                    last_name=cached_provider.get('FIRST_NM', None),
                                    # last_name=claim_row.RENDERING_LAST_NM,
                                    source_provider_id=claim_row.RENDERING_PROVIDER_ID,
                                    provider_type_raw=cached_provider.get('PROVIDER_TYP_ID', None),
                                    # provider_type_raw=claim_row.REFERRING_PROVIDER_TYP_ID,
                                    provider_type=source_provider_type_result.value,
                                    role=RENDERING,
                                    claim_identifier=claim_row.CLAIM_ID,
                                    service_number=claim_row.SVC_NBR,
                                    active=True,
                                    error=validation_errors,
                                    warning=validation_warnings,
                                    is_valid=valid,
                                    has_warnings=warn,
                                    batch_id=claim_row.batch_id,
                                    date_created=claim_row.date_created)
        providers.append(ref_provider_row)

    return providers


def to_practitioner(claim_rdd: RDD, ref_lookup) -> RDD:
    return claim_rdd.flatMap(lambda r: _to_practitioner_row(r, ref_lookup))
                    # .filter(lambda r: r.source_provider_id is not None and r.provider_type=='1')


def to_practitioner_role(practitioner_df: DataFrame) -> DataFrame:
    return practitioner_df.select(col('npi'),
                                   col('source_provider_id'),
                                   col('claim_identifier'),
                                   col('service_number'),
                                   col('role'),
                                   col('is_valid'),
                                   col('batch_id'),
                                   col('date_created'))
