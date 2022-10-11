
with denom_inclusion as (
select
    source_org_oid,
    source_consumer_id
from
    curated.patient p
INNER JOIN
    curated.procedure as proc
ON p.source_org_oid = proc.source_org_oid and p.source_consumer_id = proc.source_consumer_id
INNER JOIN
    colorectal_cancer_screening ccc
ON proc.code=ccc.code and proc.code_system=ccc.code_system
where
    datediff(year, p.dob, current_date) between 50 and 75
    and
    ccc.test_name = 'Denom Inclusion'),
denom_exclusion as (
select
    source_org_oid,
    source_consumer_id
from
    curated.procedure as proc
INNER JOIN
    colorectal_cancer_screening ccc
ON proc.code=ccc.code and proc.code_system=ccc.code_system
where
    ccc.test_name = 'Denom Exclusion' and code_system in ('CPT', 'HCPCS')
UNION
select
    source_org_oid,
    source_consumer_id
from
    curated.diagnosis as diag
INNER JOIN
    colorectal_cancer_screening ccc
ON diag.code=ccc.code and diag.code_system=ccc.code_system
where
    ccc.test_name = 'Denom Exclusion' and code_system = 'ICD10'
UNION
select
    source_org_oid,
    source_consumer_id
from
    curated.patient p
INNER JOIN
    curated.claim c
ON p.source_org_oid=c.source_org_oid and p.source_consumer_id=c.source_consumer_id
where
    datediff(year, p.dob, current_date) >= 65
    and
    UPPER(c.place_of_service) = 'HOSPICE'
)
select
    in.*
from
    denom_inclusion d_in
LEFT OUTER JOIN
    denom_exclusion d_out
ON d_in.source_org_oid=d_out.source_org_oid and d_in.source_consumer_id=d_out.source_consumer_id
where
    d_out.source_consumer_id is null;




CREATE TEMP TABLE denominator as (
    with denom_inclusion as (
select
    source_org_oid,
    source_consumer_id
from
    curated.patient p
INNER JOIN
    curated.procedure as proc
ON p.source_org_oid = proc.source_org_oid and p.source_consumer_id = proc.source_consumer_id
INNER JOIN
    colorectal_cancer_screening ccc
ON proc.code=ccc.code and proc.code_system=ccc.code_system
where
    datediff(year, p.dob, current_date) between 50 and 75
    and
    ccc.test_name = 'Denom Inclusion'),
denom_exclusion as (
select
    source_org_oid,
    source_consumer_id
from
    curated.procedure as proc
INNER JOIN
    colorectal_cancer_screening ccc
ON proc.code=ccc.code and proc.code_system=ccc.code_system
where
    ccc.test_name = 'Denom Exclusion' and code_system in ('CPT', 'HCPCS')
UNION
select
    source_org_oid,
    source_consumer_id
from
    curated.diagnosis as diag
INNER JOIN
    colorectal_cancer_screening ccc
ON diag.code=ccc.code and diag.code_system=ccc.code_system
where
    ccc.test_name = 'Denom Exclusion' and code_system = 'ICD10'
UNION
select
    source_org_oid,
    source_consumer_id
from
    curated.patient p
INNER JOIN
    curated.claim c
ON p.source_org_oid=c.source_org_oid and p.source_consumer_id=c.source_consumer_id
where
    datediff(year, p.dob, current_date) >= 65
    and
    UPPER(c.place_of_service) = 'HOSPICE'
)
select
    in.*
from
    denom_inclusion d_in
LEFT OUTER JOIN
    denom_exclusion d_out
ON d_in.source_org_oid=d_out.source_org_oid and d_in.source_consumer_id=d_out.source_consumer_id
where
    d_out.source_consumer_id is null;
)