with source as (

    select * from {{ source('raw_fintech', 'finanzas_loans') }}

),

renamed as (

    select
        loan_id,
        customer_id,
        lower(trim(loan_type)) as loan_type,
        cast(principal_amount as decimal(10,2)) as principal_amount,
        cast(interest_rate as decimal(10,2)) as interest_rate,
        cast(term_months as integer) as term_months,
        cast(monthly_payment as decimal(10,2)) as monthly_payment,
        cast(start_date as date) as start_date,
        cast(end_date as date) as end_date,
        lower(trim(status)) as status,
        lower(trim(collateral)) as collateral

    from source

)

select * from renamed
