{{
    config(
        materialized='external',
        format='csv',
        location="data/output/fact_transactions.csv",
        options={
            "overwrite": True
        }
    )
}}

with transactions_enriched as (
    select * from {{ ref('int_transactions_enriched') }}
)

select
    -- Claves
    transaction_id,
    account_id,
    customer_id,

    -- Dimensiones de tiempo
    transaction_at,
    transaction_year,
    transaction_month,
    transaction_day,

    -- Métricas y atributos
    transaction_type,
    transaction_type_description,
    transaction_amount as amount,
    balance_after_transaction as account_balance_snapshot,
    transaction_channel,
    transaction_status,
    transaction_failed

from transactions_enriched
