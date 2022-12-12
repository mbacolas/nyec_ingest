set search_path to test;

CREATE EXTERNAL TABLE encounter_coverage ()

CREATE TABLE claim
(
  id int identity(0,1),

  source_consumer_id string,
  source_org_oid string,
  payer_name string,
  `payer_id` string,
  `plan_name` string,
  `plan_id` string,
  `claim_identifier` string,
  `service_number` string,
  `type` string,
  `sub_type` string,
  `start_date` date,
  `end_date` date,
  `admission_date` date,
  `discharge_date` date,
  `units_of_service` string,
  `facility_type_cd` string,
  `admission_source_cd` string,
  `admission_type_cd` string,
  `place_of_service` string,
  `batch_id` string,
  `date_created` date
)

CREATE EXTERNAL TABLE status_history ()


CREATE EXTERNAL TABLE `cost`(
  `id` string,
  `co_payment` decimal(10,0),
  `deductible_amount` decimal(10,0),
  `coinsurance` decimal(10,0),
  `covered_amount` decimal(10,0),
  `allowed_amount` decimal(10,0),
  `not_covered_amount` decimal(10,0),
  `source_consumer_id` string,
  `source_org_oid` string,
  `claim_identifier` string,
  `service_number` string,
  `paid_amount` decimal(10,0),
  `batch_id` string,
  `date_created` date)


CREATE EXTERNAL TABLE problem
(
  id int identity(0,1),
  is_primary boolean,
  clinical_status string,
  severity string,
  onset_date date,
  onset_age int,
  abatement_date date,
  abatement_age int,
  source_consumer_id string,
  source_org_oid string,
  start_date date,
  to_date date,
  code string,
  code_system string,
  desc string,
  source_desc string,
  is_admitting boolean,
  batch_id string,
  date_created date
)

CREATE EXTERNAL TABLE `procedure`(
  `id` string,
  `body_site` string,
  `outcome` string,
  `complication` string,
  `note` string,
  `source_consumer_id` string,
  `source_org_oid` string,
  `start_date` date,
  `to_date` date,
  `code` string,
  `code_system` string,
  `revenue_code` string,
  `desc` string,
  `source_desc` string,
  `batch_id` string,
  `date_created` date)

CREATE EXTERNAL TABLE `procedure_modifier`(
  `id` string,
  `source_org_oid` string,
  `source_consumer_id` string,
  `start_date` date,
  `to_date` date,
  `code` string,
  `code_system` string,
  `mod` string,
  `batch_id` string,
  `date_created` date)


CREATE EXTERNAL TABLE drug(
  `id` string,
  `status` string,
  `discontinued_date` date,
  `days_supply` decimal(10,0),
  `dispense_qty` decimal(10,0),
  `dosage` string,
  `dosage_unit` string,
  `refills` decimal(10,0),
  `dosage_instructions` string,
  `dosage_indication` string,
  `source_consumer_id` string,
  `source_org_oid` string,
  `start_date` date,
  `to_date` date,
  `code` string,
  `code_system` string,
  `desc` string,
  `source_desc` string,
  `strength` string,
  `form` string,
  `classification` string,
  `batch_id` string,
  `date_created` date)


CREATE EXTERNAL TABLE lab (
    id INT identity(1,1) NOT NULL,
    source_consumer_id TEXT  NOT NULL,
    source_org_oid TEXT  NOT NULL,
    encounter_id TEXT,
    start_date DATETIME  NOT NULL,
    end_date DATETIME,
    code TEXT  NOT NULL,
    code_system TEXT  NOT NULL,
    desc TEXT,
    source_desc TEXT,
    unit TEXT,
    result_string TEXT,
    result_boolean BOOLEAN,
    result_numeric NUMBER,
    interpretation TEXT,
    note TEXT,
    body_site TEXT,
    method TEXT
)

CREATE EXTERNAL TABLE ref_range (
)

CREATE EXTERNAL TABLE allergy (
)

CREATE EXTERNAL TABLE reaction (
)

CREATE EXTERNAL TABLE drg (
)

CREATE EXTERNAL TABLE immunzation (
)

CREATE EXTERNAL TABLE vital (
)

CREATE EXTERNAL TABLE family_history (
)

CREATE EXTERNAL TABLE reason (
)

CREATE EXTERNAL TABLE complication_detail (
)


CREATE EXTERNAL TABLE report (
)

CREATE EXTERNAL TABLE report (
)

CREATE EXTERNAL TABLE report (
)

CREATE EXTERNAL TABLE provider (
  `id` string,
  `npi` string,
  `source_org_oid` string,
  `source_provider_id` string,
  `provider_type` string,
  `active` string,
  `batch_id` string,
  `date_created` date)

CREATE EXTERNAL TABLE performer (
)


CREATE EXTERNAL TABLE `provider_role`(
  `npi` string,
  `source_provider_id` string,
  `claim_identifier` string,
  `service_number` string,
  `role` string,
  `batch_id` string,
  `date_created` date)

CREATE EXTERNAL TABLE encounter ()
CREATE EXTERNAL TABLE encounter_coverage ()

CREATE EXTERNAL TABLE status_history ()

CREATE EXTERNAL TABLE servicing_location ()
CREATE EXTERNAL TABLE servicing_location_to_provider ()

    CREATE TABLE test.contact (
    id varchar(256) not null,
    source_org_oid varchar(256) not null,
    source_consumer_id varchar(256) not null,
    type varchar(256) not null,
CONSTRAINT contact_pkey PRIMARY KEY(id),
CONSTRAINT contact_source_consumer_id_fkey FOREIGN KEY (source_consumer_id) REFERENCES test.patient(source_consumer_id)
        or
CONSTRAINT contact_source_org_oid_fkey FOREIGN KEY (source_org_oid) REFERENCES test.organization(source_org_oid));