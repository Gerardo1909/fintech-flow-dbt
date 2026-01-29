with transfers as (
    select * from {{ ref('stg_transfers') }}
),
accounts as (
    select * from {{ ref('stg_accounts') }}
),
account_type as (
    select * from {{ ref('stg_account_types') }}
),

transfers_enriched as (
    select
        -- Claves
        t.transfer_id,
        t.from_account_id as sender,
        t.to_account_id as receiver,
        fa.account_type_id as sender_account_type_id,
        ta.account_type_id as receiver_account_type_id,

        -- Información de la cuenta origen
        fa.account_number as sender_account_number,
        fa.cbu as sender_cbu,
        fa.account_number as sender_account_number,
        fa_at.currency as sender_account_currency,
        fa_at.type_name as sender_account_type,

        -- Información de la cuenta destino
        ta.account_number as receiver_account_number,
        ta.cbu as receiver_cbu,
        ta.account_number as receiver_account_number,
        ta_at.currency as receiver_account_currency,
        ta_at.type_name as receiver_account_type,

        -- Información de la transferencia (agregar métricas derivadas de transfer_at, status y amount)
        date_part('year', t.transfer_at) as transfer_year,
        date_part('month', t.transfer_at) as transfer_month,
        date_part('day', t.transfer_at) as transfer_day,
        date_part('hour', t.transfer_at) as transfer_hour,
        date_part('minute', t.transfer_at) as transfer_minute,
        date_part('second', t.transfer_at) as transfer_second,
        case
            when t.amount > 500 then 'high_amount'
            when t.amount > 100 then 'medium_amount'
            else 'low_amount'
        end as amount_category,
        t.concept as transfer_concept,
        case
            when t.status = 'cancelled' then True
            else False
        end as transfer_cancelled,
        t.amount as transfer_amount

    from transfers as t
    join accounts as fa on t.from_account_id = fa.account_id
    join accounts as ta on t.to_account_id = ta.account_id
    join account_type as fa_at on fa.account_type_id = fa_at.account_type_id
    join account_type as ta_at on ta.account_type_id = ta_at.account_type_id
)

select * from transfers_enriched
