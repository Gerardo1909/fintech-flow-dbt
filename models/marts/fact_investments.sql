{{
    config(
        materialized='external',
        format='csv',
        location="data/output/fact_investments.csv",
        options={
            "overwrite": True
        }
    )
}}

with investments as (
    select * from {{ ref('stg_investments') }}
)

select
    -- Claves
    investment_id,
    customer_id,
    investment_type,

    -- Métricas y atributos
    quantity,
    purchase_price,
    (quantity * purchase_price) as total_cost,
    current_price,
    (quantity * current_price) as current_market_value,
    (quantity * (current_price - purchase_price)) as unrealized_pnl,
    status as investment_status,
    purchase_date

from investments
