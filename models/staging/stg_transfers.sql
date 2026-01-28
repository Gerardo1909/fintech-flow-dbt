with source as (

    select * from {{ source('raw_fintech', 'finanzas_transfers') }}

),

renamed as (

    select
        transfer_id,
        from_account_id,
        to_account_id,
        cast(amount as decimal(10,2)) as amount,
        cast(transfer_date as date) as transfer_at,
        lower(trim(concept)) as concept,
        lower(trim(status)) as status

    from source

)

select * from renamed
