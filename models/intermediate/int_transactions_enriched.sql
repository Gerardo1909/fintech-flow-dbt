with accounts as (
    select * from {{ ref('stg_accounts') }}
),
transactions as (
    select * from {{ ref('stg_transactions') }}
),
customers as (
    select * from {{ ref('stg_customers') }}
),
account_type as (
    select * from {{ ref('stg_account_types') }}
),

transactions_enriched as (
    select
        -- Claves
        t.transaction_id,
        a.account_id,
        c.customer_id,
        act.account_type_id,

        -- Información de cliente
        c.first_name as customer_first_name,
        c.last_name as customer_last_name,
        c.full_name as customer_full_name,
        c.email as customer_email,
        c.phone as customer_phone,
        c.address as customer_address,
        c.birth_date as customer_birth_date,
        c.city as customer_city,
        c.credit_score as customer_credit_score,
        c.is_vip as customer_is_vip,

        -- Información de cuenta
        a.account_number,
        a.cbu as account_cbu,
        a.opened_date as account_opened_date,
        act.currency as account_currency,
        act.type_name as account_type,

        -- Información de transacción
        t.transaction_type,
        t.amount as transaction_amount,
        t.balance_after as balance_after_transaction,
        t.transaction_at,
        t.description as transaction_description,
        t.reference_number as transaction_reference_number,
        t.channel as transaction_channel,
        t.status as transaction_status,
        date_part('year', t.transaction_at) as transaction_year,
        date_part('month', t.transaction_at) as transaction_month,
        date_part('day', t.transaction_at) as transaction_day,
        date_part('hour', t.transaction_at) as transaction_hour,
        date_part('minute', t.transaction_at) as transaction_minute,
        date_part('second', t.transaction_at) as transaction_second,
        case
            when t.amount > 0 then 'income'
            else 'expense'
        end as transaction_type_description,
        case
            when t.status = 'failed' then True
            else False
        end as transaction_failed

    from transactions as t
    join accounts as a on t.account_id = a.account_id
    join customers as c on a.customer_id = c.customer_id
    join account_type as act on a.account_type_id = act.account_type_id
)
select * from transactions_enriched
