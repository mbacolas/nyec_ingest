-- Numerator Criteria
create table numerator_criteria as (
    select source_org_oid, source_consumer_id from (
        select
            source_org_oid,
            source_consumer_id
        from
            curated.procedure as proc
        INNER JOIN
            colorectal_cancer_screening as ccc
        ON
            proc.code=ccc.code and proc.code_system=ccc.code_system
        where
            ccc.test_name = 'FOBT'
            and
            datediff(year, proc.start_date, current_date) <= 1
        UNION
        select
            source_org_oid,
            source_consumer_id
        from
            curated.procedure as proc
        INNER JOIN
            colorectal_cancer_screening as ccc
        ON
            proc.code=ccc.code and proc.code_system=ccc.code_system
        where
            ccc.test_name = 'Colonoscopy'
            and
            datediff(year, proc.start_date, current_date) <= 10
        UNION
        select
            source_org_oid,
            source_consumer_id
        from
            curated.procedure as proc
        INNER JOIN
            colorectal_cancer_screening as ccc
        ON
            proc.code=ccc.code and proc.code_system=ccc.code_system
        where
            ccc.test_name = 'Flexible Sigmoidocopy'
            and
            datediff(year, proc.start_date, current_date) <= 5
        UNION
        select
            source_org_oid,
            source_consumer_id
        from
            curated.procedure as proc
        INNER JOIN
            colorectal_cancer_screening as ccc
        ON
            proc.code=ccc.code and proc.code_system=ccc.code_system
        where
            ccc.test_name = 'FIT-DNA'
            and
            datediff(year, proc.start_date, current_date) <= 3
        UNION
        select
            source_org_oid,
            source_consumer_id
        from
            curated.procedure as proc
        INNER JOIN
            colorectal_cancer_screening as ccc
        ON
            proc.code=ccc.code and proc.code_system=ccc.code_system
        where
            ccc.test_name = 'Tomo'
            and
            datediff(year, proc.start_date, current_date) <= 5
    )
)