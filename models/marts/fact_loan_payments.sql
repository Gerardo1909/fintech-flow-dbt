{{
    config(
        materialized='external',
        format='csv',
        location="data/output/fact_loan_payments.csv",
        options={
            "overwrite": True
        }
    )
}}

with loan_payments_enriched as (
    select * from {{ ref('int_loan_payments_enriched') }}
)

select
    -- Claves
    payment_id,
    loan_id,
    customer_id,

    -- Métricas y atributos
    total_payment_amount,
    principal_paid,
    interest_paid,
    late_fee,
    payment_date,
    payment_year,
    payment_month,
    days_since_loan_start,
    payment_status,
    is_late_payment,
    pct_principal_covered_by_payment

from loan_payments_enriched
