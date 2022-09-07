set search_path to test;

-- TODO ref table for org type
-- TODO ref table for contact role
-- TODO ref table for contact type
-- TODO ref table for consumer type
-- TODO ref table for consumer marital_status

-- CREATE TABLE account
-- (
--     id int identity(0,1),
--     name varchar(256),
--     alias varchar(256),
--     PRIMARY KEY(id),
--     unique (name)
-- )

CREATE TABLE organization
(
    id int identity(0,1),
    source_org_oid varchar(256) not null,
    ein varchar(64) not null,
    active boolean,
    type varchar(256) not null,
    name varchar(256),
    alias varchar(256),
    batch_id varchar(256),
    PRIMARY KEY(id),
    unique (source_org_oid)
)
distkey(source_org_oid);

CREATE TABLE consumer
(
    id int identity(0,1),
    dist_key varchar(32) not null, --md5(source_org_oid:source_consumer_id)
    mpi varchar(256),
    prefix varchar(32),
    suffix varchar(32),
    first_name varchar(256),
    middle_name varchar(256),
    last_name varchar(256),
    dod date,
    dob date,
    ssn varchar(32),
    ethnicity varchar(32),
    race varchar(32),
    deceased boolean,
    marital_status varchar(32),
    source_consumer_id varchar(256) not null,
    source_org_oid varchar(256) not null,
    type varchar(256),
    active boolean,
    gender varchar(2),
    batch_id varchar(256),
    date_created date,
    primary key(id),
    foreign key(source_org_oid) references organization(source_org_oid),
	unique (source_org_oid, source_consumer_id)
)
distkey(dist_key)
-- compound sortkey(dob, gender);

CREATE TABLE contact
(
    id int identity(0,1),
    dist_key varchar(32) not null, --md5(source_org_oid:source_consumer_id)/md5(source_org_oid)
    source_org_oid varchar(256),
    source_consumer_id varchar(256) ,
    type varchar(256) not null,
    role varchar(256) not null,
    first_name  varchar(64),
    last_name  varchar(64),
    gender varchar(2),
    start_date date,
    end_date date,
    batch_id varchar(256),
    PRIMARY KEY(id),
    FOREIGN KEY (source_org_oid, source_consumer_id) REFERENCES consumer(source_org_oid, source_consumer_id),
    FOREIGN KEY (source_org_oid) REFERENCES organization(source_org_oid),
    UNIQUE (source_org_oid, source_consumer_id),
    UNIQUE (source_org_oid, type, start_date)
)
distkey(dist_key);


CREATE TABLE telecom
(
    id int identity(0,1),
    dist_key varchar(32) not null, --md5(source_org_oid:source_consumer_id)/md5(source_org_oid)
    source_org_oid varchar(256),
    source_consumer_id varchar(256) ,
    type varchar(256) not null,
    value varchar(50) not null,
    is_primary boolean,
    PRIMARY KEY(id),
    FOREIGN KEY (source_org_oid, source_consumer_id) REFERENCES consumer(source_org_oid, source_consumer_id),
    FOREIGN KEY (source_org_oid) REFERENCES organization(source_org_oid)
)
distkey(dist_key);


CREATE TABLE address
(
    id int identity(0,1),
    dist_key varchar(32) not null, --md5(source_org_oid:source_consumer_id)/md5(source_org_oid)
    source_org_oid varchar(256),
    source_consumer_id varchar(256),
    type varchar(12) not null,
    is_primary boolean,
    use varchar(12),
    address_line_1 varchar(64),
    address_line_2 varchar(64),
    city varchar(64),
    state varchar(2),
    zip varchar(12),
    start_date date,
    end_date date,
    PRIMARY KEY(id),
    FOREIGN KEY (source_org_oid, source_consumer_id) REFERENCES consumer(source_org_oid, source_consumer_id),
    FOREIGN KEY (source_org_oid) REFERENCES organization(source_org_oid)
)
distkey(dist_key);


CREATE TABLE coverage
(
    id int identity(0,1),
    dist_key varchar(32) not null, --md5(source_org_oid:source_consumer_id)/md5(source_org_oid)
    source_org_oid varchar(256) not null,
    source_consumer_id varchar(256) not null,
    subscriber_id varchar(32) not null,
    relationship varchar(32) not null,
    group_num varchar(256) not null,
    start_date date not null,
    end_date date not null,
    payor_name varchar(32) not null,
    type varchar(32) not null,
    PRIMARY KEY(id),
    FOREIGN KEY (source_org_oid, source_consumer_id) REFERENCES consumer(source_org_oid, source_consumer_id),
    FOREIGN KEY (source_org_oid) REFERENCES organization(source_org_oid)
)
distkey(dist_key);


CREATE TABLE communication
(
    id int identity(0,1),
    dist_key varchar(32) not null, --md5(source_org_oid:source_consumer_id)
    source_org_oid varchar(256),
    source_consumer_id varchar(256),
    language_cd varchar(32),
    is_preferred boolean,
    PRIMARY KEY(id),
    FOREIGN KEY (source_org_oid, source_consumer_id) REFERENCES consumer(source_org_oid, source_consumer_id)
)
distkey(dist_key);


CREATE TABLE eligibility
(
    id int identity(0,1),
    source_org_oid varchar(256) not null,
    source_consumer_id varchar(256) not null,
    dist_key varchar(32) not null, --md5(source_org_oid:source_consumer_id)
    policy_holder varchar(256),
    relationship varchar(32),
    pcp_id varchar(256),
    start_date date,
    end_date date,
    employer_group_id varchar(256),
    employer_group_modifier varchar(256),
    benefit_plan_id varchar(256),
    benefit_sequence_number varchar(256),
    preferred_srv_cd varchar(256),
    health_plan_cd varchar(256),
    lob varchar(256),
    bic varchar(256),
    PRIMARY KEY(id),
    FOREIGN KEY (source_org_oid, source_consumer_id) REFERENCES consumer(source_org_oid, source_consumer_id)
)
distkey(dist_key);


CREATE TABLE coverage_gaps
(
    id int identity(0,1),
    eligibility_id bigint,
    from_date date,
    to_date date,
    PRIMARY KEY(id),
    FOREIGN KEY (eligibility_id) REFERENCES eligibility(id),
    UNIQUE (eligibility_id, from_date, to_date)
)
