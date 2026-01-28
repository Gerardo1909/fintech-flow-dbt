with bank_employees as (
    select * from {{ ref('stg_bank_employees') }}
),

branches as (
    select * from {{ ref('stg_branches') }}
),

bank_employees_enriched as (
    select
        -- Claves
        e.employee_id,
        e.branch_id,
        b.manager_employee_id as branch_manager_id,

        -- Información del empleado
        e.full_name as employee_full_name,
        e.role as employee_role,
        e.email as employee_email,
        e.hire_date as employee_hire_date,
        e.salary as employee_salary,
        e.is_active as is_employee_active,

        -- Información del jefe (Self Join con bank_employees)
        -- Usamos la info de la sucursal para identificar quién es el jefe
        m.full_name as manager_full_name,
        m.role as manager_role,
        m.email as manager_email,

        -- Información de la sucursal
        b.branch_name,
        b.city as branch_city,
        b.address as branch_address,
        b.is_active as is_branch_active,

        -- Métricas derivadas
            -- Antigüedad en años
        floor((current_date - e.hire_date) / 365.25) as years_in_company,

            -- Ratio de salario vs promedio de la sucursal
        round(
            e.salary / nullif(avg(e.salary) over(partition by e.branch_id), 0),
            2
        ) as salary_vs_branch_avg_ratio,

            -- Flag de "High Salary" (ejemplo: top 10% de la empresa)
        case
            when e.salary >= percent_rank() over(order by e.salary) then true
            else false
        end as is_top_salary_tier

    from bank_employees as e
    left join branches as b
        on e.branch_id = b.branch_id
    left join bank_employees as m
        on b.manager_employee_id = m.employee_id
)

select * from bank_employees_enriched
