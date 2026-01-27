with source as (

    select * from {{ source('raw_fintech', 'finanzas_bank_employees') }}

),

renamed as (

    select
        employee_id,
        branch_id,
        trim(first_name) as first_name,
        trim(last_name) as last_name,
        concat(first_name, ' ', last_name) as full_name,
        trim(role) as role,
        trim(email) as email,
        cast(hire_date as date) as hire_date,
        cast(salary as decimal(10,2)) as salary,
        cast(is_active as boolean) as is_active

    from source

)

select * from renamed
