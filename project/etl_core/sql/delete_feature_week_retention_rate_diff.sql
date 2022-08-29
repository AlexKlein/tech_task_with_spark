delete from core.feature_retention_rate
where  day_zero_dt between current_date - 14 and
                           current_date - 1;