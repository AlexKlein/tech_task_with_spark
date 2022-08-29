create schema if not exists core;
create table if not exists core.number_of_new_users_use_feature (value_date                  date,
                                                                 product                     varchar(512),
                                                                 feature_id                  integer,
                                                                 number_of_users_use_feature integer,
                                                                 number_of_new_users         integer) partition by range (value_date);
create table if not exists core.number_of_new_users_use_feature_y2022m06 partition of core.number_of_new_users_use_feature
    for values from ('2022-06-01') TO ('2022-07-01');
create table if not exists core.number_of_new_users_use_feature_y2022m07 partition of core.number_of_new_users_use_feature
    for values from ('2022-07-01') TO ('2022-08-01');
create table if not exists core.number_of_new_users_use_feature_y2022m08 partition of core.number_of_new_users_use_feature
    for values from ('2022-08-01') TO ('2022-09-01');
create table if not exists core.number_of_new_users_use_feature_y2022m09 partition of core.number_of_new_users_use_feature
    for values from ('2022-09-01') TO ('2022-10-01');
create table if not exists core.number_of_new_users_use_feature_y2022m10 partition of core.number_of_new_users_use_feature
    for values from ('2022-10-01') TO ('2022-11-01');
create table if not exists core.number_of_new_users_use_feature_y2022m11 partition of core.number_of_new_users_use_feature
    for values from ('2022-11-01') TO ('2022-12-01');
create table if not exists core.number_of_new_users_use_feature_y2022m12 partition of core.number_of_new_users_use_feature
    for values from ('2022-12-01') TO ('2023-01-01');
create table if not exists core.number_of_users_upgraded_in_one_month (value_date                       date,
                                                                       product                          varchar(512),
                                                                       release_id                       varchar(256),
                                                                       number_of_users_upgraded_version integer,
                                                                       number_of_users                  integer) partition by range (value_date);
create table if not exists core.number_of_users_upgraded_in_one_month_y2022m06 partition of core.number_of_users_upgraded_in_one_month
    for values from ('2022-06-01') TO ('2022-07-01');
create table if not exists core.number_of_users_upgraded_in_one_month_y2022m07 partition of core.number_of_users_upgraded_in_one_month
    for values from ('2022-07-01') TO ('2022-08-01');
create table if not exists core.number_of_users_upgraded_in_one_month_y2022m08 partition of core.number_of_users_upgraded_in_one_month
    for values from ('2022-08-01') TO ('2022-09-01');
create table if not exists core.number_of_users_upgraded_in_one_month_y2022m09 partition of core.number_of_users_upgraded_in_one_month
    for values from ('2022-09-01') TO ('2022-10-01');
create table if not exists core.number_of_users_upgraded_in_one_month_y2022m10 partition of core.number_of_users_upgraded_in_one_month
    for values from ('2022-10-01') TO ('2022-11-01');
create table if not exists core.number_of_users_upgraded_in_one_month_y2022m11 partition of core.number_of_users_upgraded_in_one_month
    for values from ('2022-11-01') TO ('2022-12-01');
create table if not exists core.number_of_users_upgraded_in_one_month_y2022m12 partition of core.number_of_users_upgraded_in_one_month
    for values from ('2022-12-01') TO ('2023-01-01');
create table if not exists core.feature_retention_rate (user_id         integer,
                                                        product         varchar(512),
                                                        feature_id      integer,
                                                        day_zero_dt     date,
                                                        day_seven_entry date) partition by range (day_zero_dt);
create table if not exists core.feature_retention_rate_y2022m06 partition of core.feature_retention_rate
    for values from ('2022-06-01') TO ('2022-07-01');
create table if not exists core.feature_retention_rate_y2022m07 partition of core.feature_retention_rate
    for values from ('2022-07-01') TO ('2022-08-01');
create table if not exists core.feature_retention_rate_y2022m08 partition of core.feature_retention_rate
    for values from ('2022-08-01') TO ('2022-09-01');
create table if not exists core.feature_retention_rate_y2022m09 partition of core.feature_retention_rate
    for values from ('2022-09-01') TO ('2022-10-01');
create table if not exists core.feature_retention_rate_y2022m10 partition of core.feature_retention_rate
    for values from ('2022-10-01') TO ('2022-11-01');
create table if not exists core.feature_retention_rate_y2022m11 partition of core.feature_retention_rate
    for values from ('2022-11-01') TO ('2022-12-01');
create table if not exists core.feature_retention_rate_y2022m12 partition of core.feature_retention_rate
    for values from ('2022-12-01') TO ('2023-01-01');
create schema if not exists datamart;
create table if not exists datamart.fraction_of_new_users_use_feature (value_date                    date,
                                                                       product                       varchar(512),
                                                                       feature_id                    integer,
                                                                       fraction_of_new_feature_users numeric);
create table if not exists datamart.fraction_of_users_upgraded_in_one_month (value_date                         date,
                                                                             product                            varchar(512),
                                                                             release_id                         varchar(256),
                                                                             fraction_of_users_upgraded_version numeric);
create table if not exists datamart.feature_retention_rate (value_date          date,
                                                            product             varchar(512),
                                                            feature_id          integer,
                                                            retention_rate      numeric,
                                                            retention_rate_diff numeric);