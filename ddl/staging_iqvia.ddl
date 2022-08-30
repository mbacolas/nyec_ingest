CREATE TABLE "test"."organization" (
    id varchar(256) not null,
    source_org_oid varchar(256) not null,
    active boolean,
    type varchar(256) not null,
    name varchar(256),
    alias varchar(256),
    primary key (source_org_oid),
)
distkey(source_org_oid);

id
org_id (FK)
patient_id (FK)
type (Org, Patient or Contact)
role
first_name
last_name
phone
address
gender
start_date
end_date

CREATE TABLE contact (
    id varchar(256) not null,
    source_org_oid varchar(256) not null,
    type varchar(256) not null,
    primary key (source_org_oid)
)
distkey(source_org_oid);

CREATE TABLE "coverage"()
CREATE TABLE "communication"()
CREATE TABLE "address"()
CREATE TABLE "contact"()


CREATE TABLE "test"."consumer"(
    id varchar(256) not null,
    mpi varchar(256),
    prefix varchar(256),
    suffix varchar(256),
    first_name varchar(256),
    middle_name varchar(256),
    last_name varchar(256),
    dod DATE,
    dob DATE,
    ssn varchar(256),
    ethnicity varchar(256),
    race varchar(256),
    deceased boolean,
    marital_status varchar(256),
    source_consumer_id varchar(256) not null,
    source_org_oid varchar(256) not null,
    type varchar(256),
    active boolean,
    gender varchar(2),
    batch_id varchar(256),
    date_created date,
    primary key(id),
    foreign key(source_org_oid) references test.organization(source_org_oid)
    )
distkey(source_consumer_id)
compound sortkey(dob, gender);

CREATE EXTERNAL TABLE eligibility ()
CREATE EXTERNAL TABLE coverage ()
    CREATE TABLE "coverage_gaps"()
CREATE EXTERNAL TABLE encounter_coverage ()

CREATE EXTERNAL TABLE "claim" (
  `id` string,
  `source_consumer_id` string,
  `source_org_oid` string,
  `payer_name` string,
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
  `date_created` date)

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

CREATE EXTERNAL TABLE `diagnosis`(
  `id` string,
  `primary` boolean,
  `clinical_status` string,
  `severity` string,
  `onset_date` date,
  `onset_age` int,
  `abatement_date` date,
  `abatement_age` int,
  `source_consumer_id` string,
  `source_org_oid` string,
  `start_date` date,
  `to_date` date,
  `code` string,
  `code_system` string,
  `desc` string,
  `source_desc` string,
  `is_admitting` boolean,
  `batch_id` string,
  `date_created` date)

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

CREATE EXTERNAL TABLE status_history ()

CREATE EXTERNAL TABLE servicing_location ()
