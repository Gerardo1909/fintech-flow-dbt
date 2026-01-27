with source as (

    select * from {{ source('raw_fintech', 'finanzas_cards') }}

),

renamed as (

    select
        card_id,
        account_id,
        lower(trim(card_type)) as card_type,
        lower(trim(card_brand)) as card_brand,
        cast(card_number_last4 as varchar(4)) as card_last4,
        cast(expiry_date as date) as expiry_date,
        cast(credit_limit as decimal(10,2)) as credit_limit,
        cast(is_active as boolean) as is_active,
        cast(issued_date as date) as issued_date

    from source

)

select * from renamed
