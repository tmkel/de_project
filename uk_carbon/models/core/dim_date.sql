with date_spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2022-01-01' as date)",
        end_date="cast('2027-01-01' as date)"
    ) }}
),

enriched as (
    select
        {{ dbt_utils.generate_surrogate_key(['date_day']) }} as date_key,
        date_day                                as full_date,
        extract(dow from date_day)::int         as day_of_week,
        to_char(date_day, 'Day')                as day_name,
        extract(day from date_day)::int         as day_of_month,
        extract(month from date_day)::int       as month_number,
        to_char(date_day, 'Month')              as month_name,
        extract(quarter from date_day)::int     as quarter,
        extract(year from date_day)::int        as year,
        case
            when extract(dow from date_day) in (0, 6) then true
            else false
        end                                     as is_weekend
    from date_spine
)

select * from enriched
