with source as (

    select * from {{ source('raw_fintech', 'finanzas_account_types') }}

),

renamed as (

    select
        account_type_id,
        lower(trim(type_name)) as type_name,
        upper(trim(currency)) as currency,
        cast(min_balance as decimal(10,2)) as min_balance,
        cast(monthly_fee as decimal(10,2)) as monthly_fee,
        cast(interest_rate as decimal(10,2)) as interest_rate,
        cast(allows_overdraft as boolean) as allows_overdraft

    from source

)

select * from renamed
