with source as (

    select * from {{ source('raw_fintech', 'finanzas_exchange_rates') }}

),

renamed as (

    select
        rate_id,
        cast(date as date) as date,
        upper(trim(currency_from)) as currency_from,
        upper(trim(currency_to)) as currency_to,
        cast(buy_rate as decimal(10,2)) as buy_rate,
        cast(sell_rate as decimal(10,2)) as sell_rate

    from source

)

select * from renamed
