{{
    config(
        materialized='external',
        format='csv',
        location="data/output/fact_transfers.csv",
        options={
            "overwrite": True
        }
    )
}}

with transfers_enriched as (
    select * from {{ ref('int_transfers_enriched') }}
)

select
    -- Claves
    transfer_id,
    sender as sender_account_id,
    receiver as receiver_account_id,

    -- Métricas y atributos
    case
        when transfer_cancelled then 0
        else transfer_amount
    end as transfer_effective_amount,
    amount_category,
    transfer_year,
    transfer_month,
    transfer_day,
    transfer_hour,
    transfer_minute,
    transfer_second
    transfer_concept,
    transfer_cancelled,
    sender_account_currency

from transfers_enriched
