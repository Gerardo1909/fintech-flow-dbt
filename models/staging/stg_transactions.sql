with source as (

    select * from {{ source('raw_fintech', 'finanzas_transactions') }}

),

renamed as (

    select
        transaction_id,
        account_id,
        trim(transaction_type) as transaction_type,
        cast(amount as decimal(10,2)) as amount,
        cast(balance_after as decimal(10,2)) as balance_after,
        cast(transaction_date as timestamp) as transaction_at,
        lower(trim(description)) as description,
        trim(reference_number) as reference_number,
        lower(trim(channel)) as channel,
        lower(trim(status)) as status

    from source

)

select * from renamed
