from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, ArrayType, \
    MapType, BooleanType, DecimalType

raw_plan_schema = StructType([ \
    StructField("PLAN_ID", StringType()), \
    StructField("IMS_PLN_ID", StringType()), \
    StructField("IMS_PLN_NM", StringType()), \
    StructField("IMS_PAYER_ID", StringType()), \
    StructField("IMS_PAYER_NM", StringType()), \
    StructField("PLANTRACK_ID", StringType()), \
    StructField("MODEL_TYP_CD", StringType()), \
    StructField("MODEL_TYP_NM", StringType()), \
    StructField("IMS_PBM_ADJUDICATING_ID", StringType()), \
    StructField("IMS_PBM_ADJUDICATING_NM", StringType())
])

raw_patient_schema = StructType([ \
    StructField("PATIENT_ID",StringType(), False),
    StructField("PAT_BRTH_YR_NBR",StringType(),False),
    StructField("PAT_GENDER_CD",StringType(), False)
])

raw_claim_schema = StructType([ \
    StructField("MONTH_ID",StringType(),False), \
    StructField("SVC_DT",StringType(),False), \
    StructField("PATIENT_ID",StringType(),False), \
    StructField("PAT_ZIP3",StringType(),False), \
    StructField("CLAIM_ID",StringType(),False), \
    StructField("SVC_NBR",StringType(),False), \
    StructField("DIAG_CD_POSN_NBR",StringType(),True), \
    StructField("CLAIM_TYP_CD",StringType(),False), \
    StructField("RENDERING_PROVIDER_ID",StringType(),True), \
    StructField("REFERRING_PROVIDER_ID",StringType(),True), \
    StructField("PLACE_OF_SVC_NM",StringType(),True), \
    StructField("PLAN_ID",StringType(),True), \
    StructField("PAY_TYP_DESC",StringType(),True), \
    StructField("DIAG_CD",StringType(),True), \
    StructField("DIAG_VERS_TYP_ID",StringType(),True), \
    StructField("PRC_CD",StringType(),True), \
    StructField("PRC_VERS_TYP_ID",StringType(),True), \
    StructField("PRC1_MODR_CD",StringType(),True), \
    StructField("PRC2_MODR_CD",StringType(),True), \
    StructField("PRC3_MODR_CD",StringType(),True), \
    StructField("PRC4_MODR_CD",StringType(),True), \
    StructField("NDC_CD",StringType(),True), \
    StructField("SVC_CRGD_AMT",StringType(),True), \
    StructField("UNIT_OF_SVC_AMT",StringType(),True), \
    StructField("HOSP_ADMT_DT",StringType(),True), \
    StructField("HOSP_DISCHG_DT",StringType(),True), \
    StructField("SVC_FR_DT",StringType(),False), \
    StructField("SVC_TO_DT",StringType(),True), \
    StructField("CLAIM_HOSP_REV_CD",StringType(),True), \
    StructField("FCLT_TYP_CD",StringType(),True), \
    StructField("ADMS_SRC_CD",StringType(),True), \
    StructField("ADMS_TYP_CD",StringType(),True), \
    StructField("ADMS_DIAG_CD",StringType(),True), \
    StructField("ADMS_DIAG_VERS_TYP_ID",StringType(),True)
])

raw_procedure_schema = StructType([ \
    StructField("PRC_CD", StringType()), \
    StructField("PRC_VERS_TYP_ID", StringType()), \
    StructField("PRC_SHORT_DESC", StringType()), \
    StructField("PRC_TYP_CD", StringType())
])

raw_procedure_modifier_schema = StructType([ \
    StructField("PRC_MODR_CD", StringType()), \
    StructField("PRC_MODR_DESC", StringType())
])

raw_diag_schema = StructType([ \
    StructField("DIAG_CD", StringType()), \
    StructField("DIAG_VERS_TYP_ID", StringType()), \
    StructField("DIAG_SHORT_DESC", StringType())
])

raw_drug_schema = StructType([ \
    StructField("NDC_CD", StringType()), \
    StructField("MKTED_PROD_NM", StringType()), \
    StructField("STRNT_DESC", StringType()), \
    StructField("DOSAGE_FORM_NM", StringType()), \
    StructField("USC_CD", StringType()), \
    StructField("USC_DESC", StringType())
])

raw_provider_schema = StructType([ \
    StructField("PROVIDER_ID", StringType()), \
    StructField("PROVIDER_TYP_ID", StringType()), \
    StructField("ORG_NM", StringType()), \
    StructField("IMS_RXER_ID", StringType()), \
    StructField("LAST_NM", StringType()), \
    StructField("FIRST_NM", StringType()), \
    StructField("ADDR_LINE1_TXT", StringType()), \
    StructField("ADDR_LINE2_TXT", StringType()), \
    StructField("CITY_NM", StringType()), \
    StructField("ST_CD", StringType()), \
    StructField("ZIP", StringType()), \
    StructField("PRI_SPCL_CD", StringType()), \
    StructField("PRI_SPCL_DESC", StringType()), \
    StructField("ME_NBR", StringType()), \
    StructField("NPI", StringType())
])

raw_pro_provider_schema = StructType([
    StructField("MONTH_ID", StringType()),
    StructField("RENDERING_PROVIDER_ID", StringType()),
    StructField("TIER_ID", StringType())
])


stage_procedure_schema = StructType([
    StructField("row_id", StringType()),
    StructField("source_consumer_id", StringType()),
    StructField("source_org_oid", StringType()),
    StructField("start_date_raw", StringType()),
    StructField("start_date", DateType()),
    StructField("to_date_raw", StringType()),
    StructField("to_date", DateType()),
    StructField("code_raw", StringType()),
    StructField("code", StringType()),
    StructField("code_system_raw", StringType()),
    StructField("code_system", StringType()),
    StructField("revenue_code_raw", StringType()),
    StructField("revenue_code", StringType()),
    StructField("desc", StringType()),
    StructField("source_desc", StringType()),
    StructField("mod_raw", ArrayType(StringType())),
    StructField("mod", ArrayType(StringType())),
    StructField("error", ArrayType(StringType())),
    StructField("warning", ArrayType(StringType())),
    StructField("is_valid", BooleanType()),
    StructField("has_warnings", BooleanType()),
    StructField("batch_id", StringType())
])

stage_problem_schema = StructType([
    StructField("id", StringType()),
    StructField("source_consumer_id", StringType()),
    StructField("source_org_oid", StringType()),
    StructField("start_date_raw", StringType()),
    StructField("start_date", DateType()),
    StructField("to_date_raw", StringType()),
    StructField("to_date", DateType()),
    StructField("code_raw", StringType()),
    StructField("code", StringType()),
    StructField("code_system_raw", StringType()),
    StructField("code_system", StringType()),
    StructField("desc", StringType()),
    StructField("source_desc", StringType()),
    StructField("is_admitting", BooleanType()),
    StructField("error", ArrayType(StringType())),
    StructField("warning", ArrayType(StringType())),
    StructField("is_valid", BooleanType()),
    StructField("has_warnings", BooleanType()),
    StructField("batch_id", StringType())
])

error_schema = StructType([
    StructField("batch_id", StringType()),
    StructField("type", StringType()),
    StructField("row_errors", StringType()),
    StructField("row_value", StringType()),
    StructField("date_created", DateType())
])

stage_drug_schema = StructType([
    StructField("id", StringType()),
    StructField("source_consumer_id", StringType()),
    StructField("source_org_oid", StringType()),
    StructField("start_date_raw", StringType()),
    StructField("start_date", DateType()),
    StructField("to_date_raw", StringType()),
    StructField("to_date", DateType()),
    StructField("code_raw", StringType()),
    StructField("code", StringType()),
    StructField("code_system_raw", StringType()),
    StructField("code_system", StringType()),
    StructField("desc", StringType()),
    StructField("source_desc", StringType()),
    StructField("strength_raw", StringType()),
    StructField("strength", StringType()),
    StructField("form", StringType()),
    StructField("form_raw", StringType()),
    StructField("classification", StringType()),
    StructField("classification_raw", StringType()),
    StructField("error", StringType()),
    StructField("warning", StringType()),
    StructField("is_valid", BooleanType()),
    StructField("has_warnings", BooleanType()),
    StructField("batch_id", StringType())
])

raw_org_schema = StructType([
    StructField("source_org_oid", StringType()),
    StructField("name", StringType()),
    StructField("type", StringType()),
    StructField("active", StringType())
])

stage_org_schema = StructType([
    StructField("id", StringType()),
    StructField("source_org_oid", StringType()),
    StructField("name", StringType()),
    StructField("type", StringType()),
    StructField("active", StringType()),
    StructField("error", ArrayType(StringType())),
    StructField("warning", ArrayType(StringType())),
    StructField("is_valid", BooleanType()),
    StructField("has_warnings", BooleanType()),
    StructField("batch_id", StringType())
])

stage_patient_schema = StructType([
    StructField("id", StringType()),
    StructField("source_consumer_id", StringType()),
    StructField("source_org_oid", StringType()),
    StructField("type", StringType()),
    StructField("active", BooleanType()),
    StructField("dob", DateType()),
    StructField("gender", StringType()),
    StructField("error", ArrayType(StringType())),
    StructField("warning", ArrayType(StringType())),
    StructField("is_valid", BooleanType()),
    StructField("has_warnings", BooleanType()),
    StructField("batch_id", StringType())
])


stage_cost_schema = StructType([
    StructField("id", StringType()),
    StructField("source_consumer_id", StringType()),
    StructField("source_org_oid", StringType()),
    StructField("claim_identifier", StringType()),
    StructField("service_number", StringType()),
    StructField("paid_amount_raw", StringType()),
    StructField("paid_amount", DecimalType()),
    StructField("error", ArrayType(StringType())),
    StructField("warning", ArrayType(StringType())),
    StructField("is_valid", BooleanType()),
    StructField("has_warnings", BooleanType()),
    StructField("batch_id", StringType())
])

diag_row = Row(source_consumer_id=claim_row.PATIENT_ID,
               source_org_oid=claim_row.source_org_oid,
               claim_identifier=claim_row.CLAIM_ID,
               service_number=claim_row.SVC_NBR,
               type=source_claim_type.value,
               sub_type=is_inpatient(claim_row.HOSP_ADMT_DT),
               admission_date=admission_date_result.value,
               discharge_date=discharge_date_result.value,
               units_of_service=claim_row.SVC_CRGD_AMT,
               facility_type_cd=facility_type_cd_result.value,
               admission_source_cd=admission_source_cd_result.value,
               admission_type_cd=admission_type_cd_result.value,
               place_of_service_raw=claim_row.PLACE_OF_SVC_NM,
               place_of_service=claim_row.PLACE_OF_SVC_NM,
               error=validation_errors,
               warning=validation_warnings,
               is_valid=valid,
               has_warnings=warn)

stage_claim_schema = StructType([
    StructField("id", StringType()),
    StructField("source_consumer_id", StringType()),
    StructField("source_org_oid", StringType()),
    StructField("claim_identifier", StringType()),
    StructField("service_number", StringType()),
    StructField("type", StringType()),
    StructField("admission_date", DateType()),

    StructField("discharge_date", DateType()),
    StructField("admission_date", DateType()),
    StructField("admission_date", DateType()),
    StructField("admission_date", DateType()),
    StructField("admission_date", StringType()),
    StructField("admission_date", StringType()),

    StructField("error", ArrayType(StringType())),
    StructField("warning", ArrayType(StringType())),
    StructField("is_valid", BooleanType()),
    StructField("has_warnings", BooleanType()),
    StructField("batch_id", StringType())
])