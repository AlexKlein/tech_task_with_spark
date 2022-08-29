delete from datamart.feature_retention_rate
where  value_date = date_trunc('Month', current_date);
insert into datamart.feature_retention_rate (value_date,
                                             product,
                                             feature_id,
                                             retention_rate,
                                             retention_rate_diff)
with calc_retention_rate
as (select count(r.day_seven_entry) /
               count(r.day_zero_dt)           as retention_rate,
           r.product                          as product,
           r.feature_id                       as feature_id,
           date_trunc('Month', r.day_zero_dt) as value_date
    from   core.feature_retention_rate r
    where  date_trunc('Month', r.day_zero_dt) = date_trunc('Month', current_date)
    group by r.product,
             r.feature_id,
             date_trunc('Month', r.day_zero_dt))
select ret.value_date                       as value_date,
       ret.product                          as product,
       ret.feature_id                       as feature_id,
       ret.retention_rate                   as retention_rate,
       ret.retention_rate -
           lag(ret.retention_rate)
               over (partition by ret.product, ret.feature_id
                   order by ret.feature_id) as retention_rate_diff
from   calc_retention_rate ret;