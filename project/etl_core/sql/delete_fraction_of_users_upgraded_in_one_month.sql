delete from core.number_of_users_upgraded_in_one_month
where  value_date between current_date - 5 and
                          current_date - 1;