{{
    config(
        materialized='external',
        format='csv',
        location="data/output/dim_accounts.csv",
        options={
            "overwrite": True
        }
    )
}}

with accounts as (
    select * from {{ ref('stg_accounts') }}
),

account_types as (
    select * from {{ ref('stg_account_types') }}
),

final as (
    select
        -- Claves
        a.account_id,
        a.customer_id,

        -- Atributos
        a.account_number,
        a.cbu,
        act.type_name as account_type,
        act.currency as account_currency,
        a.opened_date,
        a.status as account_status

    from accounts as a
    left join account_types as act on a.account_type_id = act.account_type_id
)

select * from final
