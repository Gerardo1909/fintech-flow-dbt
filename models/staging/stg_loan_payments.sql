with source as (

    select * from {{ source('raw_fintech', 'finanzas_loan_payments') }}

),

renamed as (

    select
        payment_id,
        loan_id,
        cast(payment_date as date) as payment_date,
        cast(amount as decimal(10,2)) as amount,
        cast(principal_paid as decimal(10,2)) as principal_paid,
        cast(interest_paid as decimal(10,2)) as interest_paid,
        cast(late_fee as decimal(10,2)) as late_fee,
        lower(trim(status)) as status

    from source

)

select * from renamed
