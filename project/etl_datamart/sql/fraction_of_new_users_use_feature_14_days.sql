delete from datamart.fraction_of_new_users_use_feature
where  value_date = date_trunc('Month', current_date);
insert into datamart.fraction_of_new_users_use_feature (value_date,
                                                        product,
                                                        feature_id,
                                                        fraction_of_new_feature_users)
select date_trunc('Month', n.value_date) as value_date,
       n.product                         as product,
       n.feature_id                      as feature_id,
       sum(n.number_of_users_use_feature) /
           max(n.number_of_new_users)    as fraction_of_new_feature_users
from   core.number_of_new_users_use_feature n
where  date_trunc('Month', n.value_date) = date_trunc('Month', current_date)
group by n.product,
         n.feature_id,
         date_trunc('Month', n.value_date);