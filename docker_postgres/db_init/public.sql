SET search_path TO public;
CREATE TYPE patient_type AS ENUM ('PATIENT', 'MEMBER');
CREATE TYPE gender_type AS ENUM ('M', 'F', 'U');
CREATE TYPE marital_status_type AS ENUM ('SINGLE', 'MARRIED', 'WIDOWED_NOT_REMARRIED', 'DIVORCED_NOT_REMARRIED', 'MARRIED_BUT_SEPARATED');

create table consumer
(
    mpi text,
    source_org_oid text not null,
    source_consumer_id text not null,
    type patient_type not null,
    active boolean not null,
    prefix text null,
    first_name text null,
    middle_name text null,
    last_name text null,
    gender gender_type not null,
    dob date not null,
    dod date null,
    ssn text null,
    ethnicity text null,
    race text null,
    deceased boolean null,
    marital_status marital_status_type not null,
    managing_org text not null,

    created_at timestamp with time zone default CURRENT_TIMESTAMP,
    created_by text not null,
    updated_at timestamp with time zone default CURRENT_TIMESTAMP,
    updated_by text not null,
    id serial constraint patient_pk primary key
);
