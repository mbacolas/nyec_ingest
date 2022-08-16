from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, ArrayType, MapType, BooleanType


raw_plan_schema = StructType([ \
    StructField("source_org_oid", StringType(),False), \
    StructField("source_patient_id", StringType(),False), \
    StructField("is_valid", BooleanType())
])


raw_patient_schema = StructType([ \
    StructField("PATIENT_ID",StringType(),False), \
    StructField("DOB_YEAR",IntegerType(),True), \
    StructField("GENDER",StringType(),True),\
    StructField("TMP",StringType(),True)
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
    StructField("source_org_oid", StringType(),False), \
    StructField("source_patient_id", StringType(),False), \
    StructField("is_valid", BooleanType())
])


raw_procedure_modifier_schema = StructType([ \
    StructField("source_org_oid", StringType(),False), \
    StructField("source_patient_id", StringType(),False), \
    StructField("is_valid", BooleanType())
])


raw_diag_schema = StructType([ \
    StructField("source_org_oid", StringType(),False), \
    StructField("source_patient_id", StringType(),False), \
    StructField("is_valid", BooleanType())
])


raw_drug_schema = StructType([ \
    StructField("source_org_oid", StringType(),False), \
    StructField("source_patient_id", StringType(),False), \
    StructField("is_valid", BooleanType())
])


raw_provider_schema = StructType([ \
    StructField("source_org_oid", StringType(),False), \
    StructField("source_patient_id", StringType(),False), \
    StructField("is_valid", BooleanType())
])


raw_pro_provider_schema = StructType([ \
    StructField("source_org_oid", StringType(),False), \
    StructField("source_patient_id", StringType(),False), \
    StructField("is_valid", BooleanType())
])


# stage_procedure_schema = StructType([ \
#     StructField("source_org_oid", StringType(),False), \
#     StructField("source_patient_id", StringType(),False), \
#     StructField("start_date", StringType(),False), \
#     StructField("start_date_raw", DateType()), \
#     StructField("end_date", StringType()), \
#     StructField("end_date_raw", DateType()), \
#     StructField("code", StringType()), \
#     StructField("code_raw", StringType()), \
#     StructField("code_system", StringType()), \
#     StructField("code_system_raw", StringType()), \
#     StructField("desc", StringType()), \
#     StructField("desc_raw", StringType()), \
#     StructField("warning", MapType()),\
#     StructField("error", MapType()), \
#     StructField("is_valid", BooleanType())
# ])


# stage_problem_schema = StructType([ \
#     StructField("source_org_oid", StringType(),False), \
#     StructField("source_patient_id", StringType(),False), \
#     StructField("start_date", StringType(),False), \
#     StructField("start_date_raw", DateType()), \
#     StructField("end_date", StringType()), \
#     StructField("end_date_raw", DateType()), \
#     StructField("code", StringType()), \
#     StructField("code_raw", StringType()), \
#     StructField("code_system", StringType()), \
#     StructField("code_system_raw", StringType()), \
#     StructField("desc", StringType()), \
#     StructField("desc_raw", StringType()), \
#     StructField("warning", MapType()),\
#     StructField("error", MapType()), \
#     StructField("is_valid", BooleanType())
# ])