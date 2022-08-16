from iqvia.claim_service import *
import numpy
import pandas as pd

# --conf "spark.nyec.iqvia.plans_ingest_path=/Users/emmanuel.bacolas/tmp/iqvia_data/plans.csv" --conf "spark.nyec.iqvia.diags_ingest_path=/Users/emmanuel.bacolas/tmp/iqvia_data/diagnosis.csv" --conf "spark.nyec.iqvia.procs_ingest_path=/Users/emmanuel.bacolas/tmp/iqvia_data/procedure.csv" --conf "spark.nyec.iqvia.drugs_ingest_path=/Users/emmanuel.bacolas/tmp/iqvia_data/drug.csv" --conf "spark.nyec.iqvia.proc_mod_ingest_path=/Users/emmanuel.bacolas/tmp/iqvia_data/proc_modfier.csv" --conf "spark.nyec.iqvia.providers_ingest_path=/Users/emmanuel.bacolas/tmp/iqvia_data/provider.csv" --conf "spark.nyec.iqvia.claims_ingest_path=/Users/emmanuel.bacolas/tmp/iqvia_data/facts_dx.csv" --conf "spark.nyec.iqvia.patients_ingest_path=/Users/emmanuel.bacolas/tmp/iqvia_data/patients.csv"

# source ~/projects/nyec/nyec_ingest/venv/bin/activate
# export JAVA_HOME=/Users/emmanuel.bacolas/.sdkman/candidates/java/current/
# ./pyspark –-conf "spark.app.name=test_app" --driver-class-path /Users/emmanuel.bacolas/Downloads/postgresql-42.4.0.jar --jars /Users/emmanuel.bacolas/Downloads/postgresql-42.4.0.jar
# ./pyspark --conf "spark.nyec.iqvia.claims_ingest_path=/tmp/claim.csv" --conf "spark.nyec.iqvia.patient_ingest_path=/tmp/patient.csv"  --driver-class-path /Users/emmanuel.bacolas/Downloads/postgresql-42.4.0.jar --jars /Users/emmanuel.bacolas/Downloads/postgresql-42.4.0.jar

# ./pyspark  --conf spark.driver.cores=5 --conf spark.executor.cores=5  --conf spark.driver.memory=8g --conf spark.executor.memory=8g --conf "spark.nyec.iqvia.plans_ingest_path=/Users/emmanuel.bacolas/tmp/iqvia_data/plans.csv" --conf "spark.nyec.iqvia.diags_ingest_path=/Users/emmanuel.bacolas/tmp/iqvia_data/diagnosis.csv" --conf "spark.nyec.iqvia.procs_ingest_path=/Users/emmanuel.bacolas/tmp/iqvia_data/procedure.csv" --conf "spark.nyec.iqvia.drugs_ingest_path=/Users/emmanuel.bacolas/tmp/iqvia_data/drug.csv" --conf "spark.nyec.iqvia.proc_mod_ingest_path=/Users/emmanuel.bacolas/tmp/iqvia_data/proc_modfier.csv" --conf "spark.nyec.iqvia.providers_ingest_path=/Users/emmanuel.bacolas/tmp/iqvia_data/provider.csv" --conf "spark.nyec.iqvia.claims_ingest_path=/Users/emmanuel.bacolas/tmp/iqvia_data/facts_dx.csv" --conf "spark.nyec.iqvia.patients_ingest_path=/Users/emmanuel.bacolas/tmp/iqvia_data/patients.csv"  --driver-class-path /Users/emmanuel.bacolas/Downloads/postgresql-42.4.0.jar --jars /Users/emmanuel.bacolas/Downloads/postgresql-42.4.0.jar
# ./pyspark --conf spark.driver.cores=5 --conf spark.executor.cores=5 --conf spark.driver.memory=8g --conf spark.executor.memory=8g --conf "spark.nyec.iqvia.claims_ingest_path=/tmp/claim.csv"  --conf "spark.nyec.iqvia.patient_ingest_path=/tmp/patient.csv"  --driver-class-path /Users/emmanuel.bacolas/Downloads/postgresql-42.4.0.jar --jars /Users/emmanuel.bacolas/Downloads/postgresql-42.4.0.jar
# --conf "spark.nyec.dynamic_config = '[{"ingest_path" : "/tmp/claim.csv", "from" : "raw_claim_schema", "import" : "iqvia.common.schema"}, {"ingest_path" : "/tmp/patient.csv", "from" : "raw_patient_schema", "import" : "iqvia.common.schema"}]'
if __name__ == '__main__':

    p_header = patient_header()
    c_header = claims_header()

    patient = []
    claim = []

    for p in range(10):
        new_patient = []
        mrn = patient_id()
        dob = patient_dob_year()
        gender = patient_gender()

        new_patient.append(mrn)
        new_patient.append(dob)
        new_patient.append(gender)

        patient.append(new_patient)

        number_of_claims = random.randrange(10, 100)

        for c in range(number_of_claims):
            svcDt = svc_dt()
            monthId = month_id(svcDt)
            zip = patient_zip()
            claimId = claim_id()
            svcNbr = svc_nbr()
            diagCodeNbr = diag_code_position_num()
            claimType = claim_type()
            rendProvider = rendering_provider_id()
            refProvider = referring_provider_id()
            plcOfSvc = place_of_svc_nm()
            planId = plan_id()
            payType = pay_type_desc()
            diagCode = diag_cd()
            diagVer = diag_vers()
            procCd = proc_cd()
            procCdVer = proc_cd_vers()
            mod1 = proc_cd_mod(procCd)
            mod2 = proc_cd_mod(procCd)
            mod3 = proc_cd_mod(procCd)
            mod4 = proc_cd_mod(procCd)
            drugCode = ndc_cd()
            svcCrgdAmt = svc_crgd_amt()
            unitOfSvcAmt = unit_of_svc_amt()
            hospAdmtDt = hosp_admt_dt(claimType)
            hospDischgDt = hosp_dischg_dt(claimType)
            svcFrDt = svc_fr_dt(svcDt)
            svcToDt = svc_to_dt(svcDt)
            revCd = rev_cd()
            fcltTypCd = fclt_typ_cd()
            admsSrcCd = adms_src_cd()
            admsTypeDiagCd = adms_type_diag_cd()
            admsDiagCd = adms_diag_cd()
            admsDiagVers = adms_diag_vers()

            new_claim = []
            new_claim.append(monthId)
            new_claim.append(svcDt)
            new_claim.append(mrn)
            new_claim.append(zip)
            new_claim.append(claimId)
            new_claim.append(svcNbr)
            new_claim.append(diagCodeNbr)
            new_claim.append(claimType)
            new_claim.append(rendProvider)
            new_claim.append(refProvider)
            new_claim.append(plcOfSvc)
            new_claim.append(planId)
            new_claim.append(payType)
            new_claim.append(diagCode)
            new_claim.append(diagVer)
            new_claim.append(procCd)
            new_claim.append(procCdVer)
            new_claim.append(mod1)
            new_claim.append(mod2)
            new_claim.append(mod3)
            new_claim.append(mod4)
            new_claim.append(drugCode)
            new_claim.append(svcCrgdAmt)
            new_claim.append(unitOfSvcAmt)
            new_claim.append(hospAdmtDt)
            new_claim.append(hospDischgDt)
            new_claim.append(svcFrDt)
            new_claim.append(svcToDt)
            new_claim.append(revCd)
            new_claim.append(fcltTypCd)
            new_claim.append(admsSrcCd)
            new_claim.append(admsTypeDiagCd)
            new_claim.append(admsDiagCd)
            new_claim.append(admsDiagVers)

            claim.append(new_claim)

    a = numpy.asarray(patient)
    b = numpy.asarray(claim)

    df1 = pd.DataFrame(a, columns=[patient_header()])
    df2 = pd.DataFrame(b, columns=[claims_header()])

    df1.to_csv("/tmp/patient.csv", index=False)
    df2.to_csv("/tmp/claim.csv", index=False)
    # df1.to_parquet('/tmp/patient.parquet')
    # df2.to_parquet('/tmp/claim.parquet')
