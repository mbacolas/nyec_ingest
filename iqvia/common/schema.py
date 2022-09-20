from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, ArrayType, \
    MapType, BooleanType, DecimalType, TimestampType

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

stage_procedure__modifier_schema = StructType([
    StructField("id", StringType()),
    StructField("source_org_oid", StringType()),
    StructField("source_consumer_id", StringType()),
    StructField("start_date", DateType()),
    StructField("to_date", DateType()),
    StructField("code", StringType()),
    StructField("code_system", StringType()),
    StructField("mod", StringType()),
    StructField("batch_id", StringType()),
    StructField("date_created", DateType())
])

stage_procedure_schema = StructType([
    StructField("id", StringType()),
    StructField("body_site", StringType()),
    StructField("outcome", StringType()),
    StructField("complication", StringType()),
    StructField("note", StringType()),
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
    StructField("error", ArrayType(MapType(StringType(), StringType()))),
    StructField("warning", ArrayType(MapType(StringType(), StringType()))),
    StructField("is_valid", BooleanType()),
    StructField("has_warnings", BooleanType()),
    StructField("batch_id", StringType()),
    StructField("date_created", DateType())
])

stage_problem_schema = StructType([
    StructField("id", StringType()),
    StructField("primary", BooleanType()),
    StructField("clinical_status", StringType()),
    StructField("severity", StringType()),
    StructField("onset_date", DateType()),
    StructField("onset_age", IntegerType()),
    StructField("abatement_date", DateType()),
    StructField("abatement_age", IntegerType()),
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
    StructField("error", ArrayType(MapType(StringType(), StringType()))),
    StructField("warning", ArrayType(MapType(StringType(), StringType()))),
    StructField("is_valid", BooleanType()),
    StructField("has_warnings", BooleanType()),
    StructField("batch_id", StringType()),
    StructField("date_created", DateType())
])

error_schema = StructType([
    StructField("id", StringType()),
    StructField("batch_id", StringType()),
    StructField("type", StringType()),
    StructField("is_valid", BooleanType()),
    StructField("has_warnings", BooleanType()),
    StructField("row_errors", StringType()),
    StructField("row_warnings", StringType()),
    StructField("row_value", StringType()),
    StructField("date_created", DateType())
])

stage_drug_schema = StructType([
    StructField("id", StringType()),
    StructField("status", StringType()),
    StructField("discontinued_date", DateType()),
    StructField("days_supply", DecimalType()),
    StructField("dispense_qty", DecimalType()),
    StructField("dosage", StringType()),
    StructField("dosage_unit", StringType()),
    StructField("refills", DecimalType()),
    StructField("dosage_instructions", StringType()),
    StructField("dosage_indication", StringType()),
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
    StructField("error", ArrayType(MapType(StringType(), StringType()))),
    StructField("warning", ArrayType(MapType(StringType(), StringType()))),
    StructField("is_valid", BooleanType()),
    StructField("has_warnings", BooleanType()),
    StructField("batch_id", StringType()),
    StructField("date_created", DateType())
])

raw_org_schema = StructType([
    StructField("id", StringType()),
    StructField("source_org_oid", StringType()),
    StructField("name", StringType()),
    StructField("type", StringType()),
    StructField("active", BooleanType()),
    StructField("batch_id", StringType()),
    StructField("date_created", DateType())
])

# stage_org_schema = StructType([
#     StructField("id", StringType()),
#     StructField("source_org_oid", StringType()),
#     StructField("name", StringType()),
#     StructField("type", StringType()),
#     StructField("active", StringType()),
#     StructField("error", ArrayType(StringType())),
#     StructField("warning", ArrayType(StringType())),
#     StructField("is_valid", BooleanType()),
#     StructField("has_warnings", BooleanType()),
#     StructField("batch_id", StringType()),
#     StructField("date_created", DateType())
# ])

stage_patient_schema = StructType([
    StructField("id", StringType()),
    StructField("mpi", StringType()),
    StructField("prefix", StringType()),
    StructField("suffix", StringType()),
    StructField("first_name", StringType()),
    StructField("middle_name", StringType()),
    StructField("last_name", StringType()),
    StructField("dod", DateType()),
    StructField("ssn", StringType()),
    StructField("ethnicity", StringType()),
    StructField("race", StringType()),
    StructField("deceased", BooleanType()),
    StructField("marital_status", StringType()),
    # StructField("phone", ArrayType(StringType())),
    # StructField("email", ArrayType(StringType())),
    # StructField("address", ArrayType(StringType())),
    StructField("source_consumer_id", StringType()),
    StructField("source_org_oid", StringType()),
    StructField("type", StringType()),
    StructField("active", BooleanType()),
    StructField("dob_raw", StringType()),
    StructField("dob", DateType()),
    StructField("gender_raw", StringType()),
    StructField("gender", StringType()),
    StructField("error", ArrayType(MapType(StringType(), StringType()))),
    StructField("warning", ArrayType(MapType(StringType(), StringType()))),
    StructField("is_valid", BooleanType()),
    StructField("has_warnings", BooleanType()),
    StructField("batch_id", StringType()),
    StructField("date_created", DateType())
])

stage_telcom_schema = StructType([
    StructField("id", StringType()),
    StructField("source_consumer_id", StringType()),
    StructField("source_org_oid", StringType()),
    StructField("claim_identifier", StringType()),
    StructField("service_number", StringType()),
    StructField("paid_amount_raw", StringType()),
    StructField("paid_amount", DecimalType()),
    StructField("error", ArrayType(MapType(StringType(), StringType()))),
    StructField("warning", ArrayType(MapType(StringType(), StringType()))),
    StructField("is_valid", BooleanType()),
    StructField("has_warnings", BooleanType()),
    StructField("batch_id", StringType()),
    StructField("date_created", DateType())
])

stage_address_schema = StructType([
    StructField("id", StringType()),
    StructField("source_consumer_id", StringType()),
    StructField("source_org_oid", StringType()),
    StructField("primary", BooleanType()),
    StructField("use", StringType()),
    StructField("type", StringType()),
    StructField("error", ArrayType(MapType(StringType(), StringType()))),
    StructField("warning", ArrayType(MapType(StringType(), StringType()))),
    StructField("city", BooleanType()),
    StructField("state", BooleanType()),
    StructField("zip", BooleanType()),
    StructField("start_date", BooleanType()),
    StructField("end_date", BooleanType()),
    StructField("batch_id", StringType()),
    StructField("date_created", DateType())
])

stage_cost_schema = StructType([
    StructField("id", StringType()),
    StructField("co_payment", DecimalType()),
    StructField("deductible_amount", DecimalType()),
    StructField("coinsurance", DecimalType()),
    StructField("covered_amount", DecimalType()),
    StructField("allowed_amount", DecimalType()),
    StructField("not_covered_amount", DecimalType()),
    StructField("source_consumer_id", StringType()),
    StructField("source_org_oid", StringType()),
    StructField("claim_identifier", StringType()),
    StructField("service_number", StringType()),
    StructField("paid_amount_raw", StringType()),
    StructField("paid_amount", DecimalType()),
    StructField("error", ArrayType(MapType(StringType(), StringType()))),
    StructField("warning", ArrayType(MapType(StringType(), StringType()))),
    StructField("is_valid", BooleanType()),
    StructField("has_warnings", BooleanType()),
    StructField("batch_id", StringType()),
    StructField("date_created", DateType())
])


stage_claim_schema = StructType([
    StructField("id", StringType()),
    StructField("source_consumer_id", StringType()),
    StructField("source_org_oid", StringType()),
    StructField("payer_name", StringType()),
    StructField("payer_id", StringType()),
    StructField("plan_name", StringType()),
    StructField("plan_id", StringType()),
    StructField("claim_identifier", StringType()),
    StructField("service_number", StringType()),
    StructField("type_raw", StringType()),
    StructField("type", StringType()),
    StructField("sub_type_raw", StringType()),
    StructField("sub_type", StringType()),
    StructField("start_date_raw", StringType()),
    StructField("start_date", DateType()),
    StructField("end_date_raw", StringType()),
    StructField("end_date", DateType()),
    StructField("admission_date_raw", StringType()),
    StructField("admission_date", DateType()),
    StructField("discharge_date_raw", StringType()),
    StructField("discharge_date", DateType()),
    StructField("units_of_service_raw", StringType()),
    StructField("units_of_service", StringType()),
    StructField("facility_type_cd_raw", StringType()),
    StructField("facility_type_cd", StringType()),
    StructField("admission_source_cd_raw", StringType()),
    StructField("admission_source_cd", StringType()),
    StructField("admission_type_cd_raw", StringType()),
    StructField("admission_type_cd", StringType()),
    StructField("place_of_service_raw", StringType()),
    StructField("place_of_service", StringType()),
    StructField("error", ArrayType(MapType(StringType(), StringType()))),
    StructField("warning", ArrayType(MapType(StringType(), StringType()))),
    StructField("is_valid", BooleanType()),
    StructField("has_warnings", BooleanType()),
    StructField("batch_id", StringType()),
    StructField("date_created", DateType())
])

stage_provider_schema = StructType([
    StructField("id", StringType()),
    StructField("npi", StringType()),
    StructField("source_org_oid", StringType()),
    StructField("first_name", StringType()),
    StructField("last_name", StringType()),
    StructField("source_provider_id", StringType()),
    StructField("provider_type_raw", StringType()),
    StructField("provider_type", StringType()),
    StructField("role", StringType()),
    StructField("claim_identifier", StringType()),
    StructField("service_number", StringType()),
    StructField("active", BooleanType()),
    StructField("error", ArrayType(MapType(StringType(), StringType()))),
    StructField("warning", ArrayType(MapType(StringType(), StringType()))),
    StructField("is_valid", BooleanType()),
    StructField("has_warnings", BooleanType()),
    StructField("batch_id", StringType()),
    StructField("date_created", DateType())
])

stage_provider_role_schema = StructType([
    StructField("npi", StringType()),
    StructField("claim_identifier", StringType()),
    StructField("service_number", StringType()),
    StructField("role", StringType()),
    StructField("date_created", DateType())
])

curated_ingest_run_schema = StructType([
    StructField("data_source", StringType()),
    StructField("run_date", DateType()),
    StructField("batch_id", StringType()),
    StructField("file_paths", StringType()),
    StructField("duration", StringType()),
    StructField("start_time", DateType()),
    StructField("end_time", DateType())
])
