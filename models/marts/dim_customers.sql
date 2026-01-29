{{
    config(
        materialized='external',
        format='csv',
        location="data/output/dim_customers.csv",
        options={
            "overwrite": True
        }
    )
}}

with customers_snapshot as (
    select * from {{ ref('customers_snapshot') }}
),

final as (
    select
        -- Claves
        dbt_scd_id as customer_key,
        customer_id,

        -- Atributos
        full_name,
        email,
        phone,
        address,
        city,
        credit_score,
        is_vip,

        -- Metadatos de SCD2
        dbt_valid_from as effective_from,
        dbt_valid_to as effective_to,
        case when dbt_valid_to is null then true else false end as is_current
    from customers_snapshot
)

select * from final
