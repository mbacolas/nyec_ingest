from pyspark import RDD
from pyspark.sql import Row
import uuid
from common.functions import *
from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pymonad.either import *

RENDERING = 'RENDERING'
REFERRING = 'REFERRING'
# is_record_valid = lambda x: False if (len(x) > 0) else True


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


def _to_procedure_row(claim_row: Row) -> Row:
    start_date_result = str_to_date(claim_row.SVC_FR_DT, 'SVC_FR_DT')
    to_date_result = str_to_date(claim_row.SVC_TO_DT, 'SVC_TO_DT', False)
    all_code_system_result = to_standard_code_system(claim_row.PRC_VERS_TYP_ID, claim_row.PRC_TYP_CD, 'PRC_VERS_TYP_ID:PRC_TYP_CD')
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
    row_id = uuid.uuid4().hex[:12]
    proc_row = Row(id=row_id,
                   source_consumer_id=claim_row.PATIENT_ID,
                   source_org_oid=claim_row.source_org_oid,
                   start_date_raw=claim_row.SVC_FR_DT,
                   start_date=start_date_result.value,
                   to_date_raw=claim_row.SVC_TO_DT,
                   to_date=to_date_result.value,
                   code_raw=claim_row.PRC_CD,
                   code=proc_code.get('code', None),
                   code_system_raw=claim_row.PRC_TYP_CD,
                   code_system=proc_code.get('code_system', None),
                   revenue_code_raw=claim_row.CLAIM_HOSP_REV_CD,
                   revenue_code=rev_code.get('code', None),
                   desc=proc_code.get('desc', None),
                   source_desc=claim_row.PRC_SHORT_DESC,
                   mod_raw=modifiers_clean,
                   mod=modifiers_clean,
                   error=validation_errors,
                   warning=validation_warnings,
                   is_valid=valid,
                   has_warnings=warn,
                   batch_id=claim_row.batch_id)
    return proc_row


def to_procedure(claim_rdd: RDD) -> RDD:
    return claim_rdd.filter(lambda r: r.PRC_CD is not None)\
                    .map(lambda r: _to_procedure_row(r))\
                    .keyBy(lambda r: (r.source_org_oid, r.source_consumer_id, r.start_date, r.code_raw, r.code_system_raw)) \
                    .reduceByKey((lambda a,b: a))\
                    .map(lambda r: r[1])


def _to_problem_row(claim_row: Row) -> Row:
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
    row_id = uuid.uuid4().hex[:12]
    diag_row = Row(id=row_id,
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
                   source_desc=claim_row.DIAG_SHORT_DESC,
                   is_admitting=False,
                   error=validation_errors,
                   warning=validation_warnings,
                   is_valid=valid,
                   has_warnings=warn,
                   batch_id=claim_row.batch_id)
    return diag_row


def to_problem(claim_rdd: RDD) -> RDD:
    return claim_rdd.filter(lambda r: r.DIAG_CD is not None)\
                    .map(lambda r: _to_problem_row(r))\
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
    row_id = uuid.uuid4().hex[:12]
    diag_row = Row(id=row_id,
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
                   batch_id=claim_row.batch_id)
    return diag_row


def to_admitting_diagnosis(claim_rdd: RDD) -> RDD:
    return claim_rdd.filter(lambda r: r.ADMS_DIAG_CD is not None)\
                    .map(lambda r: _to_admitting_diagnosis(r))\
                    .keyBy(lambda r: (r.source_consumer_id, r.start_date, r.code_raw, r.code_system_raw)) \
                    .reduceByKey((lambda a, b: a))\
                    .map(lambda r: r[1])


def _to_drug_row(claim_row: Row) -> Row:
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
    row_id = uuid.uuid4().hex[:12]
    drug_row = Row(id=row_id,
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
                   source_desc=claim_row.MKTED_PROD_NM,
                   strength_raw=claim_row.STRNT_DESC,
                   strength=claim_row.STRNT_DESC,
                   form=claim_row.DOSAGE_FORM_NM,
                   form_raw=claim_row.DOSAGE_FORM_NM,
                   classification=claim_row.USC_DESC,
                   classification_raw=claim_row.USC_DESC,
                   error=validation_errors,
                   warning=validation_warnings,
                   is_valid=valid,
                   has_warnings=warn,
                   batch_id=claim_row.batch_id)
    return drug_row


def to_drug(claim_rdd: RDD) -> RDD:
    return claim_rdd \
        .filter(lambda r: r.NDC_CD is not None) \
        .map(lambda r: _to_drug_row(r))\
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
    dob = str_to_date(f'{patient_plan.PAT_BRTH_YR_NBR}-01-01', 'patient.dob', '%Y-%m-%d')
    gender = is_included(patient_plan.PAT_GENDER_CD, 'PAT_GENDER_CD', ['M', 'F', 'U'])
    validation_errors = extract_left(*[dob, gender])
    validation_warnings = []
    valid = is_record_valid(validation_errors)
    warn = False
    patient_row = Row(source_org_oid=patient_plan.source_org_oid,
                      source_consumer_id=patient_plan.PATIENT_ID,
                      type=patient_plan.consumer_type,
                      active=patient_plan.consumer_status,
                      dob=dob.value,
                      gender=gender.value,
                      error=validation_errors,
                      warning=validation_warnings,
                      is_valid=valid,
                      has_warnings=warn,
                      batch_id=patient_plan.batch_id)
    return patient_row


def to_patient(patient_plan_rdd: RDD) -> RDD:
    return patient_plan_rdd.map(lambda r: _to_patient_row(r))\
                        .keyBy(lambda r: (r.source_consumer_id)) \
                        .reduceByKey((lambda a, b: a))\
                        .map(lambda r: r[1])


def _to_org_row(patient_plan: Row) -> Row:
    org_row = Row(source_org_oid=patient_plan.source_org_oid,
                   name=patient_plan.source_org_oid,
                   # name=patient_plan.IMS_PAYER_NM,
                   type=patient_plan.org_type,
                   active=True,
                   error=[],
                   warning=[],
                   is_valid=True,
                   has_warnings=False)
    return org_row


def to_org(patient_plan_rdd: RDD) -> RDD:
    return patient_plan_rdd.map(lambda r: _to_org_row(r))


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
    row_id = uuid.uuid4().hex[:12]
    cost_row = Row(id=row_id,
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
                    batch_id=claim_row.batch_id)
    return cost_row


def to_cost(claim_row_rdd: RDD) -> RDD:
    return claim_row_rdd.map(lambda r: _to_cost_row(r)) \
                    .keyBy(lambda r: (r.source_org_oid, r.source_consumer_id, r.claim_identifier, r.service_number, r.paid_amount)) \
                    .reduceByKey((lambda a, b: a))\
                    .map(lambda r: r[1])


def _to_claim_row(claim_row: Row) -> Row:
    source_claim_type = to_claim_type(claim_row.CLAIM_TYP_CD)

    start_date_result = str_to_date(claim_row.SVC_FR_DT, 'SVC_FR_DT')
    to_date_result = str_to_date(claim_row.SVC_TO_DT, 'SVC_TO_DT', False)
    admission_date_result = str_to_date(claim_row.HOSP_ADMT_DT, 'HOSP_ADMT_DT', False)
    discharge_date_result = str_to_date(claim_row.HOSP_DISCHG_DT, 'HOSP_DISCHG_DT', False) #should be True
    facility_type_cd_result = validate_facility_type_cd(claim_row.FCLT_TYP_CD)
    admission_source_cd_result = validate_admission_source_cd(claim_row.ADMS_SRC_CD)
    admission_type_cd_result = validate_admission_type_cd(claim_row.ADMS_TYP_CD)
    row_id = uuid.uuid4().hex[:12]
    validation_errors = extract_left(*[source_claim_type,
                                       start_date_result,
                                       facility_type_cd_result,
                                       admission_source_cd_result,
                                       admission_type_cd_result])
    valid = is_record_valid(validation_errors)
    validation_warnings = []
    warn = False

    claim_stage_row = Row(id=row_id,
                           source_consumer_id=claim_row.PATIENT_ID,
                            source_org_oid=claim_row.source_org_oid,
                            payer_name=claim_row.IMS_PAYER_NM,
                            payer_id=claim_row.IMS_PLN_ID,
                            plan_name=claim_row.IMS_PLN_NM,
                            plan_id=claim_row.PLAN_ID,
                            claim_identifier=claim_row.CLAIM_ID,
                            service_number=claim_row.SVC_NBR,
                            type=source_claim_type.value,
                            sub_type=is_inpatient(claim_row.HOSP_ADMT_DT),
                            start_date=start_date_result.value,
                            end_date=to_date_result.value,
                            admission_date=admission_date_result.value,
                            discharge_date=discharge_date_result.value,
                            units_of_service=claim_row.UNIT_OF_SVC_AMT,
                            facility_type_cd=claim_row.FCLT_TYP_CD,
                            admission_source_cd=claim_row.ADMS_SRC_CD,
                            admission_type_cd=claim_row.ADMS_TYP_CD,
                            place_of_service=claim_row.PLACE_OF_SVC_NM,
                            error=validation_errors,
                            warning=validation_warnings,
                            is_valid=valid,
                            has_warnings=warn)
    return claim_stage_row


def to_claim(claim_raw: RDD) -> RDD:
    return claim_raw.map(lambda r: _to_claim_row(r))\
                    .keyBy(lambda r: (r.source_org_oid, r.source_consumer_id, r.claim_identifier)) \
                    .reduceByKey((lambda a, b: a))\
                    .map(lambda r: r[1])


def _to_practitioner_row(claim_row: Row) -> Row:
    providers = []

    if claim_row.RENDERING_PROVIDER_ID is not None:
        source_provider_type_result = validate_provider_type(claim_row.RENDERING_PROVIDER_TYP_ID)
        validation_errors = extract_left(*[source_provider_type_result])
        valid = is_record_valid(validation_errors)
        validation_warnings = []
        warn = False

        rendering_provider_row = Row(npi=claim_row.RENDERING_NPI,
                                    source_org_oid=claim_row.source_org_oid,
                                    first_name=claim_row.RENDERING_FIRST_NM,
                                    last_name=claim_row.RENDERING_LAST_NM,
                                    source_provider_id=claim_row.RENDERING_PROVIDER_ID,
                                    provider_type=source_provider_type_result.value,
                                    role=RENDERING,
                                    claim_identifier=claim_row.CLAIM_ID,
                                    service_number=claim_row.SVC_NBR,
                                    active=True,
                                    error=validation_errors,
                                    warning=validation_warnings,
                                    is_valid=valid,
                                    has_warnings=warn)
        providers.append(rendering_provider_row)

    if claim_row.REFERRING_PROVIDER_ID is not None:
        source_provider_type_result = validate_provider_type(claim_row.REFERRING_PROVIDER_TYP_ID)
        validation_errors = extract_left(*[source_provider_type_result])
        valid = is_record_valid(validation_errors)
        validation_warnings = []
        warn = False
        ref_provider_row = Row(npi=claim_row.REFERRING_NPI,
                                source_org_oid=claim_row.source_org_oid,
                                first_name=claim_row.REFERRING_FIRST_NM,
                                last_name=claim_row.REFERRING_LAST_NM,
                                source_provider_id=claim_row.REFERRING_PROVIDER_ID,
                                provider_type=source_provider_type_result.value,
                                role=REFERRING,
                                claim_identifier=claim_row.CLAIM_ID,
                                service_number=claim_row.SVC_NBR,
                                active=True,
                                error=validation_errors,
                                warning=validation_warnings,
                                is_valid=valid,
                                has_warnings=warn)
        providers.append(ref_provider_row)

    return providers


def to_practitioner(claim_rdd: RDD) -> RDD:
    return claim_rdd.flatMap(lambda r: _to_practitioner_row(r))
                    # .filter(lambda r: r.source_provider_id is not None and r.provider_type=='1')


def to_practitioner_role_row(practitioner_df: DataFrame) -> DataFrame:
    return practitioner_df.select(col('npi'),
                                   col('source_provider_id'),
                                   col('claim_identifier'),
                                   col('role'))
