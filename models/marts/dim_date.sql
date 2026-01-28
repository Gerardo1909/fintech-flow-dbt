select
    datum as date_day,
    extract(year from datum) as year,
    extract(month from datum) as month,
    to_char(datum, 'TMMonth') as month_name,
    extract(dow from datum) in (0, 6) as is_weekend

from generate_series('2023-01-01'::date, '2036-12-31'::date, '1 day'::interval) as datum
