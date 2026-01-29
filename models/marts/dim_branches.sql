{{
    config(
        materialized='external',
        format='csv',
        location="data/output/dim_branches.csv",
        options={
            "overwrite": True
        }
    )
}}

with branches as (
    select * from {{ ref('stg_branches') }}
),

final as (
    select
        -- Claves
        branch_id,

        -- Atributos
        branch_name,
        city,
        address,
        manager_employee_id,
        opened_date,
        is_active

    from branches
)

select * from final
