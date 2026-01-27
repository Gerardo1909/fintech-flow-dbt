with source as (

    select * from {{ source('raw_fintech', 'finanzas_investments') }}

),

renamed as (

    select
        investment_id,
        customer_id,
        lower(trim(investment_type)) as investment_type,
        upper(trim(symbol)) as symbol,
        cast(quantity as integer) as quantity,
        cast(purchase_price as decimal(10,2)) as purchase_price,
        cast(current_price as decimal(10,2)) as current_price,
        cast(purchase_date as date) as purchase_date,
        lower(trim(status)) as status

    from source

)

select * from renamed
