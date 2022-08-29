delete from datamart.fraction_of_users_upgraded_in_one_month
where  value_date = date_trunc('Month', current_date);
insert into datamart.fraction_of_users_upgraded_in_one_month (value_date,
                                                              product,
                                                              release_id,
                                                              fraction_of_users_upgraded_version)
select date_trunc('Month', n.value_date) as value_date,
       n.product                         as product,
       n.release_id                      as release_id,
       sum(n.number_of_users_upgraded_version) /
           max(n.number_of_users)        as fraction_of_users_upgraded_version
from   core.number_of_users_upgraded_in_one_month n
where  date_trunc('Month', n.value_date) = date_trunc('Month', current_date)
group by n.product,
         n.release_id,
         date_trunc('Month', n.value_date);