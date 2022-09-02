delete from core.number_of_new_users_use_feature
where  value_date between current_date - 5 and
                          current_date - 1;