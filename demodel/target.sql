-- set mapred.job.queue.name=clv;
-- set hive.exec.copyfile.maxsize=7516192768;

-- Create table schema
-- drop table if exists grp_gdoop_clv_db.ce_keep_deact_target purge

-- create table grp_gdoop_clv_db.ce_keep_deact_target (
--     consumer_id string,
--     last_train_pd_order_date string,
--     first_target_pd_order_date string,
--     deactivated int
-- )
-- partitioned by (record_date string)
-- clustered by (consumer_id) into 256 buckets
-- stored as orc
-- tblproperties ('orc.compress' = 'SNAPPY')



-- Add new partition
insert overwrite table grp_gdoop_clv_db.ce_keep_deact_target
    partition(record_date = '{{ params.feature_date }}')

select
    pop.consumer_id,
    pop.most_recent_order_date as last_train_pd_order_date,
    future.first_order_date as first_target_pd_order_date,
    case
        when future.first_order_date <= date_add(pop.most_recent_order_date, 365) then 0
        else 1
    end as deactivated

from (
    select
        consumer_id,
        date_sub('{{ params.feature_date }}', recency_x) as most_recent_order_date
    from
        grp_gdoop_clv_db.keep_cdf_final_features_t365d
    where
        record_date = '{{ params.feature_date }}'
    ) pop

left join (
    select
        consumer_id,
        min(order_date) as first_order_date
    from
        grp_gdoop_marketing_analytics_db.me_orders_fgt_usd
    where
        platform_key = 1
        and user_brand_affiliation in ('groupon')
        and country_id in ('235','40')
        and action = 'authorize'
        and txn_amount_loc <> 0
        and attribution_type = '3.1'
        and order_date > '{{ params.feature_date }}'
        and order_date < '{{ params.today }}'
    group by
        consumer_id
    ) future on pop.consumer_id = future.consumer_id

distribute by
    consumer_id

sort by
    consumer_id

--;

-- Partition stats
-- analyze table grp_gdoop_clv_db.ce_keep_deact_target
--     partition(record_date = '${feature_date}') compute statistics;

-- Drop old partitions
-- alter table grp_gdoop_clv_db.ce_keep_deact_target
--     drop if exists partition(record_date <= '${three_days_ago}');









-- Comparing results to v2 model : getting deactivation results for July, Aug, Sep, Oct to test if v2 & v3 models
-- made accurate predictions
--insert overwrite table grp_gdoop_clv_db.ce_keep_deact_target
--    partition(record_date = '2017-11-12')
--
--select
--    pop.consumer_id,
--    pop.most_recent_order_date as last_train_pd_order_date,
--    future.first_order_date as first_target_pd_order_date,
--    case
--        when future.first_order_date <= date_add(pop.most_recent_order_date, 365) then 0
--        else 1
--    end as deactivated
--
--from (
--    select
--        consumer_id,
--        recency_x,
--        date_sub('2017-11-12', recency_x) as most_recent_order_date
--    from
--        grp_gdoop_clv_db.keep_cdf_final_features_t365d
--    where
--        record_date = '2017-11-12'
--        --and recency_x between 242 and 364
--    ) pop
--
--left join (
--    select
--        consumer_id,
--        min(order_date) as first_order_date
--    from
--        grp_gdoop_marketing_analytics_db.me_orders_fgt_usd
--    where
--        platform_key = 1
--        and user_brand_affiliation in ('groupon')
--        and country_id in ('235','40')
--        and action = 'authorize'
--        and txn_amount_loc <> 0
--        and attribution_type = '3.1'
--        and order_date > '2017-11-12'
--        and order_date < '2018-11-13'
--    group by
--        consumer_id
--    ) future on pop.consumer_id = future.consumer_id
--
--distribute by
--    consumer_id
--
--sort by
--    consumer_id
--
--;
