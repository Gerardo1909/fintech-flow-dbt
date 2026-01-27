with source as (

    select * from {{ source('raw_fintech', 'finanzas_customers') }}

),

renamed as (

    select
        customer_id,
        trim(first_name) as first_name,
        trim(last_name) as last_name,
        concat(first_name, ' ', last_name) as full_name,
        trim(cast(dni as varchar(20))) as dni,
        trim(email) as email,
        trim(phone) as phone,
        trim(address) as address,
        trim(city) as city,
        cast(birth_date as date) as birth_date,
        cast(registration_date as date) as registration_date,
        cast(credit_score as integer) as credit_score,
        cast(is_vip as boolean) as is_vip,
        preferred_branch_id

    from source

)

select * from renamed
