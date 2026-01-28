with loan_payments as (
    select * from {{ ref('stg_loan_payments') }}
),

loans as (
    select * from {{ ref('stg_loans') }}
),

customers as (
    select * from {{ ref('stg_customers') }}
),

loan_payments_enriched as (
    select
        -- Claves
        lp.payment_id,
        lp.loan_id,
        l.customer_id,
        c.preferred_branch_id,

        -- Información del cliente
        c.full_name as customer_full_name,
        c.dni as customer_dni,
        c.email as customer_email,
        c.credit_score as customer_credit_score,
        c.is_vip as customer_is_vip,

        -- Información propia del préstamo
        l.loan_type,
        l.principal_amount as loan_total_principal,
        l.interest_rate as loan_annual_interest_rate,
        l.term_months as loan_term_months,
        l.monthly_payment as loan_expected_monthly_amount,
        l.start_date as loan_start_date,
        l.end_date as loan_end_date,
        l.status as loan_current_status,

        -- Información del pago
        lp.payment_date,
        lp.amount as total_payment_amount,
        lp.principal_paid,
        lp.interest_paid,
        lp.late_fee,
        lp.status as payment_status,

        -- Métricas derivadas (Lógica de negocio)
            -- Porcentaje del préstamo cubierto por este pago
        round((lp.principal_paid / nullif(l.principal_amount, 0)) * 100, 2) as pct_principal_covered_by_payment,

            -- Identificar si es un pago fuera de término
        case
            when lp.late_fee > 0 then true
            else false
        end as is_late_payment,

            -- Tiempo transcurrido desde el inicio del préstamo hasta el pago
        lp.payment_date - l.start_date as days_since_loan_start,

            -- Estacionalidad del pago
        date_part('year', lp.payment_date) as payment_year,
        date_part('month', lp.payment_date) as payment_month

    from loan_payments as lp
    inner join loans as l on lp.loan_id = l.loan_id
    inner join customers as c on l.customer_id = c.customer_id
)

select * from loan_payments_enriched
