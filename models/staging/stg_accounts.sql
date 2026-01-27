with source as (

    select * from {{ source('raw_fintech', 'finanzas_accounts') }}

),

renamed as (

    select
        account_id,
        customer_id,
        account_type_id,
        trim(account_number) as account_number,
        trim(cbu) as cbu,
        cast(balance as decimal(10,2)) as balance,
        cast(opened_date as date) as opened_date,
        lower(trim(status)) as status,
        cast(last_activity_date as date) as last_activity_date

    from source

)

select * from renamed
