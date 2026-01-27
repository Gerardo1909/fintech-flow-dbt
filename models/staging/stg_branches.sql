with source as (

    select * from {{ source('raw_fintech', 'finanzas_branches') }}

),

renamed as (

    select
        branch_id,
        lower(trim(branch_name)) as branch_name,
        lower(trim(city)) as city,
        lower(trim(address)) as address,
        trim(phone) as phone,
        manager_id as manager_employee_id,
        cast(opened_date as date) as opened_date,
        cast(is_active as boolean) as is_active

    from source

)

select * from renamed
