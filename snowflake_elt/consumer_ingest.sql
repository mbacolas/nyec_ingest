

--- BRONX_RHIO
merge into dev.reporting.consumer using DEV.BRONX_RHIO.MPI
ON consumer.source_consumer_id = MPI.mrn and consumer.source_org_oid=MPI.provider_sourceid
when matched then
    update set consumer.first_name = MPI.first_name,
                consumer.last_name = MPI.last_name,
                consumer.gender = MPI.sex,
                consumer.dob = MPI.date_of_birth,
                consumer.ssn = MPI.ssn
WHEN NOT MATCHED THEN
    INSERT (source_org_oid,
                      source_consumer_id,
                      first_name,
                      last_name,
                      dob,
                      ssn,
                      gender,
                      active)
          VALUES (MPI.provider_sourceid,
           MPI.mrn,
          MPI.first_name,
           MPI.last_name,
           MPI.date_of_birth,
           MPI.ssn,
           MPI.sex,
           True)

--- HEALTHECONNECTIONS
merge into dev.reporting.consumer using DEV.HEALTHECONNECTIONS.MPI
ON consumer.source_consumer_id = MPI.mrn and consumer.source_org_oid=MPI.provider_sourceid
when matched then
    update set consumer.first_name = MPI.first_name,
                consumer.last_name = MPI.last_name,
                consumer.gender = MPI.sex,
                consumer.dob = MPI.date_of_birth,
                consumer.ssn = MPI.ssn
WHEN NOT MATCHED THEN
    INSERT (source_org_oid,
                      source_consumer_id,
                      first_name,
                      last_name,
                      dob,
                      ssn,
                      gender,
                      active)
          VALUES (MPI.provider_sourceid,
           MPI.mrn,
          MPI.first_name,
           MPI.last_name,
           MPI.date_of_birth,
           MPI.ssn,
           MPI.sex,
           True)

--- HEALTHELINK
merge into dev.reporting.consumer using DEV.HEALTHELINK.MPI
ON consumer.source_consumer_id = MPI.mrn and consumer.source_org_oid=MPI.provider_sourceid
when matched then
    update set consumer.first_name = MPI.first_name,
                consumer.last_name = MPI.last_name,
                consumer.gender = MPI.sex,
                consumer.dob = MPI.date_of_birth,
                consumer.ssn = MPI.ssn
WHEN NOT MATCHED THEN
    INSERT (source_org_oid,
                      source_consumer_id,
                      first_name,
                      last_name,
                      dob,
                      ssn,
                      gender,
                      active)
          VALUES (MPI.provider_sourceid,
           MPI.mrn,
          MPI.first_name,
           MPI.last_name,
           MPI.date_of_birth,
           MPI.ssn,
           MPI.sex,
           True)

--- HEALTHIX
merge into dev.reporting.consumer using DEV.HEALTHIX.MPI
ON consumer.source_consumer_id = MPI.mrn and consumer.source_org_oid=MPI.provider_sourceid
when matched then
    update set consumer.first_name = MPI.first_name,
                consumer.last_name = MPI.last_name,
                consumer.gender = MPI.sex,
                consumer.dob = MPI.date_of_birth,
                consumer.ssn = MPI.ssn
WHEN NOT MATCHED THEN
    INSERT (source_org_oid,
                      source_consumer_id,
                      first_name,
                      last_name,
                      dob,
                      ssn,
                      gender,
                      active)
          VALUES (MPI.provider_sourceid,
           MPI.mrn,
          MPI.first_name,
           MPI.last_name,
           MPI.date_of_birth,
           MPI.ssn,
           MPI.sex,
           True)


--- HIXNY
merge into dev.reporting.consumer using DEV.HIXNY.MPI
ON consumer.source_consumer_id = MPI.mrn and consumer.source_org_oid=MPI.provider_sourceid
when matched then
    update set consumer.first_name = MPI.first_name,
                consumer.last_name = MPI.last_name,
                consumer.gender = MPI.sex,
                consumer.dob = MPI.date_of_birth,
                consumer.ssn = MPI.ssn
WHEN NOT MATCHED THEN
    INSERT (source_org_oid,
                      source_consumer_id,
                      first_name,
                      last_name,
                      dob,
                      ssn,
                      gender,
                      active)
          VALUES (MPI.provider_sourceid,
           MPI.mrn,
          MPI.first_name,
           MPI.last_name,
           MPI.date_of_birth,
           MPI.ssn,
           MPI.sex,
           True)


--- ROCHESTER_RHIO
merge into dev.reporting.consumer using DEV.ROCHESTER_RHIO.MPI
ON consumer.source_consumer_id = MPI.mrn and consumer.source_org_oid=MPI.provider_sourceid
when matched then
    update set consumer.first_name = MPI.first_name,
                consumer.last_name = MPI.last_name,
                consumer.gender = MPI.sex,
                consumer.dob = MPI.date_of_birth,
                consumer.ssn = MPI.ssn
WHEN NOT MATCHED THEN
    INSERT (source_org_oid,
                      source_consumer_id,
                      first_name,
                      last_name,
                      dob,
                      ssn,
                      gender,
                      active)
          VALUES (MPI.provider_sourceid,
           MPI.mrn,
          MPI.first_name,
           MPI.last_name,
           MPI.date_of_birth,
           MPI.ssn,
           MPI.sex,
           True)

--- QE MPI
merge into dev.reporting.consumer using DEV.nyec.mpi_hixny
ON consumer.source_consumer_id = mpi_hixny.QE_MPI and consumer.source_org_oid=mpi_hixny.QE_SOURCEID
when matched then
    update set consumer.first_name = mpi_hixny.first_name,
                consumer.last_name = mpi_hixny.last_name,
                consumer.gender = mpi_hixny.sex,
                consumer.dob = mpi_hixny.date_of_birth,
                consumer.ssn = mpi_hixny.ssn
WHEN NOT MATCHED THEN
    INSERT (source_org_oid,
                      source_consumer_id,
                      first_name,
                      last_name,
                      dob,
                      ssn,
                      gender,
                      active)
          VALUES (mpi_hixny.QE_SOURCEID,
           mpi_hixny.QE_MPI,
          mpi_hixny.first_name,
           mpi_hixny.last_name,
           mpi_hixny.date_of_birth,
           mpi_hixny.ssn,
           mpi_hixny.sex,
           True)