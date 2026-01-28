with customers as (
    select * from {{ ref('stg_customers') }}
),
accounts as (
    select * from {{ ref('stg_accounts') }}
),
cards as (
    select * from {{ ref('stg_cards') }}
),
investments as (
    select * from {{ ref('stg_investments') }}
),
account_type as (
    select * from {{ ref('stg_account_types') }}
),
customer_investments_totals as (
    select
        customer_id,
        count(investment_id) as total_investments,
        sum(quantity * purchase_price) as total_invested_amount
    from investments
    group by customer_id
),
ranked_investments as (
    select
        customer_id,
        investment_type,
        dense_rank() over(
            partition by customer_id
            order by count(investment_id) desc, sum(quantity * purchase_price) desc
        ) as rank_priority
    from investments
    group by customer_id, investment_type
),
customer_preferred_investment as (
    select
        customer_id,
        investment_type as top_investment_type
    from ranked_investments
    where rank_priority = 1
),

customers_enriched as (
    select
        --Claves
        c.customer_id,
        a.account_id,
        ca.card_id,
        act.account_type_id,

        -- Información de la cuenta del cliente
        a.account_number,
        a.cbu as account_cbu,
        a.opened_date as account_opened_date,
        act.currency as account_currency,
        act.type_name as account_type,

        -- Información de la tarjeta del cliente
        ca.card_type,
        ca.card_brand,
        ca.expiry_date,
        ca.credit_limit,
        ca.issued_date,

        -- Información personal del cliente
        c.first_name,
        c.last_name,
        c.full_name,
        c.email,
        c.phone,
        c.address,
        c.birth_date,
        c.city,
        c.credit_score,
        c.is_vip,

        -- Información de inversiones
        cit.total_investments,
        cit.total_invested_amount,
        cpi.top_investment_type

    from customers as c
    left join accounts as a on c.customer_id = a.customer_id
    left join cards as ca on a.account_id = ca.account_id
    left join customer_investments_totals as cit on c.customer_id = cit.customer_id
    left join customer_preferred_investment as cpi on c.customer_id = cpi.customer_id
    left join account_type as act on a.account_type_id = act.account_type_id
)

select * from customers_enriched
