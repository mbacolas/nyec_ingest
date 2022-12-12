USE ROLE SYSADMIN;
USE WAREHOUSE wh_nyec;
USE DATABASE DEV;
USE SCHEMA reporting;

select * from DEV.PROCESSED.MPI_NYEC_BRONX_RHIO;

--- BRONX_RHIO
merge into dev.reporting.patient_index using DEV.PROCESSED.MPI_NYEC_BRONX_RHIO
ON patient_index.smpi = MPI_NYEC_BRONX_RHIO.state_eid
    and
    patient_index.qe_source_org_oid = MPI_NYEC_BRONX_RHIO.QE_SOURCEID
    and
    patient_index.qe_mpi = MPI_NYEC_BRONX_RHIO.QE_MPI
    and
    patient_index.source_org_oid = MPI_NYEC_BRONX_RHIO.provider_sourceid
    and
    patient_index.source_facility_consumer_id = MPI_NYEC_BRONX_RHIO.MRN
when matched then
    update set patient_index.qe_name = MPI_NYEC_BRONX_RHIO.QE_SOURCENAME,
                patient_index.source_org_name = MPI_NYEC_BRONX_RHIO.provider_sourcename
WHEN NOT MATCHED THEN
    INSERT (smpi,
              qe_source_org_oid,
              qe_name,
              qe_mpi,
              source_org_oid,
              source_org_name,
              source_facility_consumer_id)
          VALUES (MPI_NYEC_BRONX_RHIO.state_eid,
           MPI_NYEC_BRONX_RHIO.qe_sourceid,
          MPI_NYEC_BRONX_RHIO.qe_sourcename,
           MPI_NYEC_BRONX_RHIO.qe_mpi,
           MPI_NYEC_BRONX_RHIO.provider_sourceid,
           MPI_NYEC_BRONX_RHIO.provider_sourcename,
           MPI_NYEC_BRONX_RHIO.mrn)


--- MPI_NYEC_HEALTHECONNECTIONS
merge into dev.reporting.patient_index using DEV.PROCESSED.MPI_NYEC_HEALTHECONNECTIONS
ON patient_index.smpi = MPI_NYEC_HEALTHECONNECTIONS.state_eid
    and
    patient_index.qe_source_org_oid = MPI_NYEC_HEALTHECONNECTIONS.QE_SOURCEID
    and
    patient_index.qe_mpi = MPI_NYEC_HEALTHECONNECTIONS.QE_MPI
    and
    patient_index.source_org_oid = MPI_NYEC_HEALTHECONNECTIONS.provider_sourceid
    and
    patient_index.source_facility_consumer_id = MPI_NYEC_HEALTHECONNECTIONS.MRN
when matched then
    update set patient_index.qe_name = MPI_NYEC_HEALTHECONNECTIONS.QE_SOURCENAME,
                patient_index.source_org_name = MPI_NYEC_HEALTHECONNECTIONS.provider_sourcename
WHEN NOT MATCHED THEN
    INSERT (smpi,
              qe_source_org_oid,
              qe_name,
              qe_mpi,
              source_org_oid,
              source_org_name,
              source_facility_consumer_id)
          VALUES (MPI_NYEC_HEALTHECONNECTIONS.state_eid,
           MPI_NYEC_HEALTHECONNECTIONS.qe_sourceid,
          MPI_NYEC_HEALTHECONNECTIONS.qe_sourcename,
           MPI_NYEC_HEALTHECONNECTIONS.qe_mpi,
           MPI_NYEC_HEALTHECONNECTIONS.provider_sourceid,
           MPI_NYEC_HEALTHECONNECTIONS.provider_sourcename,
           MPI_NYEC_HEALTHECONNECTIONS.mrn)


--- MPI_NYEC_HEALTHELINK
merge into dev.reporting.patient_index using DEV.PROCESSED.MPI_NYEC_HEALTHELINK
ON patient_index.smpi = MPI_NYEC_HEALTHELINK.state_eid
    and
    patient_index.qe_source_org_oid = MPI_NYEC_HEALTHELINK.QE_SOURCEID
    and
    patient_index.qe_mpi = MPI_NYEC_HEALTHELINK.QE_MPI
    and
    patient_index.source_org_oid = provider_sourceid
    and
    patient_index.source_facility_consumer_id = MRN
when matched then
    update set patient_index.qe_name = QE_SOURCENAME,
                patient_index.source_org_name = provider_sourcename
WHEN NOT MATCHED THEN
    INSERT (smpi,
              qe_source_org_oid,
              qe_name,
              qe_mpi,
              source_org_oid,
              source_org_name,
              source_facility_consumer_id)
          VALUES (state_eid,
           qe_sourceid,
           qe_sourcename,
           qe_mpi,
           provider_sourceid,
           provider_sourcename,
           MPI_NYEC_HEALTHELINK.mrn)


--- MPI_NYEC_HEALTHIX
merge into dev.reporting.patient_index using DEV.PROCESSED.MPI_NYEC_HEALTHIX
ON patient_index.smpi = state_eid
    and
    patient_index.qe_source_org_oid = QE_SOURCEID
    and
    patient_index.qe_mpi = QE_MPI
    and
    patient_index.source_org_oid = provider_sourceid
    and
    patient_index.source_facility_consumer_id = MRN
when matched then
    update set patient_index.qe_name = QE_SOURCENAME,
                patient_index.source_org_name = provider_sourcename
WHEN NOT MATCHED THEN
    INSERT (smpi,
              qe_source_org_oid,
              qe_name,
              qe_mpi,
              source_org_oid,
              source_org_name,
              source_facility_consumer_id)
          VALUES (state_eid,
           qe_sourceid,
           qe_sourcename,
           qe_mpi,
           provider_sourceid,
           provider_sourcename,
           mrn)

--- MPI_NYEC_HIXNY
merge into dev.reporting.patient_index using DEV.PROCESSED.MPI_NYEC_HIXNY
ON patient_index.smpi = state_eid
    and
    patient_index.qe_source_org_oid = QE_SOURCEID
    and
    patient_index.qe_mpi = MPI_NYEC_HIXNY.QE_MPI
    and
    patient_index.source_org_oid = provider_sourceid
    and
    patient_index.source_facility_consumer_id = MRN
when matched then
    update set patient_index.qe_name = QE_SOURCENAME,
                patient_index.source_org_name = provider_sourcename
WHEN NOT MATCHED THEN
    INSERT (smpi,
              qe_source_org_oid,
              qe_name,
              qe_mpi,
              source_org_oid,
              source_org_name,
              source_facility_consumer_id)
          VALUES (state_eid,
           qe_sourceid,
           qe_sourcename,
           qe_mpi,
           provider_sourceid,
           provider_sourcename,
           mrn)

--- MPI_NYEC_ROCHESTER_RHIO
merge into dev.reporting.patient_index using DEV.PROCESSED.MPI_NYEC_ROCHESTER_RHIO
ON patient_index.smpi = state_eid
    and
    patient_index.qe_source_org_oid = QE_SOURCEID
    and
    patient_index.qe_mpi = MPI_NYEC_ROCHESTER_RHIO.QE_MPI
    and
    patient_index.source_org_oid = provider_sourceid
    and
    patient_index.source_facility_consumer_id = MRN
when matched then
    update set patient_index.qe_name = QE_SOURCENAME,
                patient_index.source_org_name = provider_sourcename
WHEN NOT MATCHED THEN
    INSERT (smpi,
              qe_source_org_oid,
              qe_name,
              qe_mpi,
              source_org_oid,
              source_org_name,
              source_facility_consumer_id)
          VALUES (state_eid,
           qe_sourceid,
           qe_sourcename,
           qe_mpi,
           provider_sourceid,
           provider_sourcename,
           mrn)