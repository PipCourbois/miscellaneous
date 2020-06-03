--optimus job is here: https://optimus.groupondev.com/#/jobs/edit/48575
--dateparam = current_date+358
--sevendaysago = current_date+364


insert overwrite table grp_gdoop_clv_db.eb_pip_deact_train_pop_intl_scoring
partition (record_date = '${dateparam1}')
SELECT 
  brand, 
  consumer_id, 
  user_key , 
  Min( recency_segment)  AS recency_segment, 
  Min(frequency_segment) AS frequency_segment , 
  Min(recency_9block)    AS recency_9block, 
  Min(frequency_9block)  AS frequency_9block 
FROM   
( 
              SELECT 
                     a.record_date,
                     a.brand, 
                     a.consumer_id,
                     a.user_key, 
                     a.recency_segment, 
                     a.frequency_segment , 
                     a.recency_9block, 
                     a.frequency_9block
              FROM cia_realtime.user_attrs_gbl a
              left join user_groupondw.gbl_dim_country b
                on(a.country_code=b.country_iso_code_2)
              WHERE record_date = Add_months(date_sub('${dateparam1}',1),-12)
                AND a.recency_9block IN ( '3-Low Rec (121-365 Days)', '2-Med Rec (31-120 Days)', '1-High Rec (0-30 Days)' ) 
                AND a.brand = 'groupon'
                and a.country_code IN ('IE')
) pop

GROUP BY record_date, 
         brand, 
         consumer_id, 
         user_key
distribute BY consumer_id 
sort BY  consumer_id;analyze table grp_gdoop_clv_db.eb_pip_deact_train_pop_intl_scoring  partition(record_date = '${dateparam1}') compute statistics;ALTER TABLE grp_gdoop_clv_db.eb_pip_deact_train_pop_intl_scoring  DROP IF EXISTS PARTITION(record_date <= '${sevendaysago}') purge;---------
--step 2 execute
--------


insert overwrite table grp_gdoop_clv_db.eb_pip_deact_temp_intl_scoring
partition (record_date = '${dateparam1}')
select 
         consumer_id
        ,max(transaction_date) as most_recent_order_date
        ,max(case when order_rank = 1 then is_activation else 0 end) as is_activation 
        ,max(case when order_rank = 1 then is_reactivation else 0 end) as is_reactivation
        ,max(case when order_rank = 1 then order_id end) as most_recent_order_id
        ,max(case when order_rank = 1 then nob end) as most_recent_order_price
        ,max(case when order_rank = 1 then deal_permalink end) as most_recent_deal
        ,max(case when order_rank = 1 then grt_l1_cat_name end) as most_recent_l1
        ,max(case when order_rank = 1 then grt_l2_cat_name end) as most_recent_l2
        ,max(case when order_rank = 1 then traffic_type end) as most_recent_traffic_type
        ,max(case when order_rank = 1 then traffic_source end) as most_recent_traffic_source

        ,max(case when order_rank = 1 then platform end) as most_recent_platform

        ,max(case when order_rank = 1 then promo_type end) as most_recent_promo_type

        ,count(*) as frequency_T24m
        ,sum(units) as units_T24m
        ,sum(nob) as nob_T24m
        ,sum(nor) as nor_T24m
        ,sum(gp) as gp_T24m
        ,sum(discount_amount) as OD_T24m
        ,sum(ils_discount) as ILS_T24m
        ,sum(wow_subsidy_amount) as wow_T24m

        ,sum(d365) as frequency_T12M
        ,coalesce(sum(d365*units),0) as units_T12m
        ,coalesce(sum(d365*nob),0) as nob_T12m
        ,coalesce(sum(d365*nor),0) as nor_T12m
        ,coalesce(sum(d365*gp),0) as gp_T12m
        ,coalesce(sum(d365*discount_amount),0) as OD_T12m
        ,coalesce(sum(d365*ils_discount),0) as ILS_T12m
        ,coalesce(sum(d365*wow_subsidy_amount),0) as wow_T12m

        ,sum(case when grt_l1_cat_name = 'L1 - Local' then 1 else 0 end) as local_orders_T24m
        ,sum(case when grt_l1_cat_name = 'L1 - Shopping' then 1 else 0 end) as shopping_orders_T24m
        ,sum(case when grt_l1_cat_name = 'L1 - Travel' then 1 else 0 end) as travel_orders_T24m

        ,sum(case when platform = 'app' then 1 else 0 end) as app_orders_T24m
        ,sum(case when platform = 'touch' then 1 else 0 end) as touch_orders_T24m
        ,sum(case when platform = 'web' then 1 else 0 end) as web_orders_T24m

        ,sum(case when traffic_type = 'Paid' then 1 else 0 end) as paid_orders_T24m
        ,sum(case when traffic_type = 'Free' then 1 else 0 end) as free_orders_T24m
        ,sum(case when traffic_type = 'Managed' then 1 else 0 end) as managed_orders_T24m

        ,count(distinct concat(order_quarter,order_year)) as unique_purchase_quarters

from
(

        select 
                   me.consumer_id
                  ,me.transaction_date
                  ,me.order_uuid
                  ,me.order_timestamp
                  ,me.order_id
                  ,is_activation 
                  ,is_reactivation

                  ,me.deal_permalink
                  ,me.grt_l1_cat_name
                  ,me.grt_l2_cat_name

                  ,me.traffic_type
                  ,me.traffic_source
                  ,me.traffic_sub_source

                  ,me.platform
                  ,me.subplatform

                  ,me.units
                  ,me.auth_nob_loc as nob
                  ,me.auth_nor_loc as nor
                  ,me.auth_gp_loc as gp

                  ,im.promo_type
                  ,im.order_discount_type

                  ,im.ils_campaign_name
                  ,im.od_promotion_code  
                  ,im.od_target_channel
                  ,im.wow_merchant
                  ,im.wow_account_name

                  , -me.discount_amount_loc as discount_amount
                  ,me.ils_discount_loc as ils_discount
                  ,coalesce(im.wow_discount,0) as wow_subsidy_amount

                  -- ignored for now: ,case when action = 'refund' then 1 else 0 end as refund_flag

                  ,row_number() over (partition by me.consumer_id order by me.order_timestamp desc) as order_rank
                  ,case 
                           when me.transaction_date >= add_months(date_sub('${dateparam1}',1),-24) then 1 else 0 
                           end as d365
                  ,case 
                           when month(me.transaction_date) in (1,2,3) then 'Q1' 
                           when month(me.transaction_date) in (4,5,6) then 'Q2'
                           when month(me.transaction_date) in (7,8,9) then 'Q3' 
                           else 'Q4' 
                           end as order_quarter
                  ,year(me.transaction_date) as order_year
        
        from  grp_gdoop_marketing_analytics_db.me_orders_fgt_usd me
        left outer join push_analytics.src_incentive_metrics im
             on  me.order_id = im.order_id
        WHERE platform_key = 2
        and user_brand_affiliation in ('groupon')
        and me.country_id in ('107')
        and action = 'authorize' -- ignore returns: = 'authorize'  include returns: <> 'capture'
        and txn_amount_loc <> 0
        and attribution_type = '3.0'  -- 3.1 is active from 2015-04-01 forward
        and me.transaction_date >= add_months(date_sub('${dateparam1}',1),-36)
        and me.transaction_date < add_months(date_sub('${dateparam1}',1),-12)

        DISTRIBUTE BY consumer_id SORT BY consumer_id, transaction_date
) logs
group by consumer_id

DISTRIBUTE BY consumer_id SORT BY consumer_id;analyze table grp_gdoop_clv_db.eb_pip_deact_temp_intl_scoring  partition(record_date = '${dateparam1}') compute statistics;alter table grp_gdoop_clv_db.eb_pip_deact_temp_intl_scoring  DROP IF EXISTS PARTITION(record_date <= '${sevendaysago}') purge;------
--step 3
------
insert overwrite table grp_gdoop_clv_db.eb_pip_deact_response_intl_scoring
partition (record_date = '${dateparam1}')
select 
    consumer_id
    , user_brand_affiliation
    , min(order_date) as min_order_date
    , 1 as retained 
from grp_gdoop_marketing_analytics_db.me_orders_fgt_usd ords
  where transaction_date >= add_months(date_sub('${dateparam1}',1),-12)
	and transaction_date < date_sub('${dateparam1}',1)
  and country_id in (107)
  and platform_key = 2
  and attribution_type = '3.0' 
  and txn_amount_loc <> 0 --will need to include this with next run
  and user_brand_affiliation = 'groupon'
group by consumer_id , user_brand_affiliation;analyze table grp_gdoop_clv_db.eb_pip_deact_response_intl_scoring  partition(record_date = '${dateparam1}') compute statistics;alter table grp_gdoop_clv_db.eb_pip_deact_response_intl_scoring  DROP IF EXISTS PARTITION(record_date <= '${sevendaysago}') purge;------
--step 4
------
insert overwrite table grp_gdoop_clv_db.eb_pip_deact_cookie_map1_intl_scoring  
partition (record_date = '${dateparam1}')

SELECT 
	p.consumer_id
	,m.bcookie
	, max(m.event_date) as last_cookie_date
FROM 
(
    select 
        *
    from  grp_gdoop_clv_db.eb_pip_deact_train_pop_intl_scoring 
    where record_date = '${dateparam1}'
) p

JOIN user_groupondw.user_bcookie_mapping m
on p.consumer_id = m.user_uuid
and m.country_code = 'IE'
and m.event_date >= add_months(date_sub('${dateparam1}',1), -15) 
and m.event_date < add_months(date_sub('${dateparam1}',1), -12)
group by p.consumer_id, m.bcookie;analyze table grp_gdoop_clv_db.eb_pip_deact_cookie_map1_intl_scoring  partition(record_date = '${dateparam1}') compute statistics;alter table grp_gdoop_clv_db.eb_pip_deact_cookie_map1_intl_scoring  DROP IF EXISTS PARTITION(record_date <= '${sevendaysago}') purge;------
--step 5
------

insert overwrite table grp_gdoop_clv_db.eb_pip_deact_cookie_map2_intl_scoring
partition (record_date = '${dateparam1}')
SELECT 
	p.consumer_id
	,ords.bcookie
	, max(ords.transaction_date) as last_cookie_date
FROM 
(
    select 
        *
    from grp_gdoop_clv_db.eb_pip_deact_train_pop_intl_scoring
    where record_date = '${dateparam1}'
) p

JOIN grp_gdoop_marketing_analytics_db.me_orders_fgt ords
      on p.consumer_id = ords.consumer_id
      and transaction_date < add_months(date_sub('${dateparam1}',1),-12)
      and transaction_date >= add_months(date_sub('${dateparam1}',1),-36)
      and country_id in (107)
      and platform_key = 2
      and attribution_type = '3.0'

group by p.consumer_id,ords.bcookie;analyze table grp_gdoop_clv_db.eb_pip_deact_cookie_map2_intl_scoring  partition (record_date = '${dateparam1}') compute statistics;-- union
alter table grp_gdoop_clv_db.eb_pip_deact_cookie_map2_intl_scoring  DROP IF EXISTS PARTITION(record_date <= '${sevendaysago}') purge;------
--step 6
------
insert overwrite table grp_gdoop_clv_db.eb_pip_deact_cookie_map_intl_scoring 
partition (record_date = '${dateparam1}')
select 
	consumer_id
	, bcookie
	, max(last_cookie_date) as last_cookie_date
from
(
	select 
		consumer_id
		, bcookie
		, last_cookie_date
	from grp_gdoop_clv_db.eb_pip_deact_cookie_map1_intl_scoring
	where record_date = '${dateparam1}'	
	union all
	select 
		consumer_id
		, bcookie
		, last_cookie_date
	from grp_gdoop_clv_db.eb_pip_deact_cookie_map2_intl_scoring
	where record_Date = '${dateparam1}'
) a
group by consumer_id, bcookie;analyze table grp_gdoop_clv_db.eb_pip_deact_cookie_map_intl_scoring  partition(record_date = '${dateparam1}') compute statistics;alter table grp_gdoop_clv_db.eb_pip_deact_cookie_map_intl_scoring  DROP IF EXISTS PARTITION(record_date <= '${sevendaysago}') purge;------
--step 7
------
insert overwrite table grp_gdoop_clv_db.eb_pip_deact_engagement_temp_intl_scoring  
partition (record_date = '${dateparam1}')

select 
	cookie_b
	 ,max(event_date) as event_date
	 ,count(distinct event_date) as visit_freq_90d
	 ,count(distinct case when event_date >= date_sub(add_months(date_sub('${dateparam1}',1), -12),7) then event_date end) as visit_freq_7d
	 ,count(distinct case when event_date >= date_sub(add_months(date_sub('${dateparam1}',1), -12),14) then event_date end) visit_freq_14d
	 ,count(distinct case when event_date >= date_sub(add_months(date_sub('${dateparam1}',1), -12),28) then event_date end) visit_freq_28d

	 ,sum(available_deal_views) as available_deal_views_90d

	 ,count(distinct case when cookie_first_platform = 'App' then event_date end) as AppFrequency
	,count(distinct case when cookie_first_platform = 'Web' then event_date end) as WebFrequency
	,count(distinct case when cookie_first_platform = 'Touch' then event_date end) as TouchFrequency

  from user_groupondw.gbl_traffic_superfunnel

  where event_date >= add_months(date_sub('${dateparam1}',1), -15) 
	and event_date < add_months(date_sub('${dateparam1}',1), -12)
  and cookie_first_country_code = 'IE'
  group by cookie_b;analyze table grp_gdoop_clv_db.eb_pip_deact_engagement_temp_intl_scoring  partition(record_date = '${dateparam1}') compute statistics;alter table grp_gdoop_clv_db.eb_pip_deact_engagement_temp_intl_scoring  DROP IF EXISTS PARTITION(record_date <= '${sevendaysago}') purge;-----
--step 8
--------
insert overwrite table grp_gdoop_clv_db.eb_pip_deact_engagement_features_intl_scoring
partition(record_date = '${dateparam1}')

select 
	p.consumer_id
	,max(event_date) as visit_recency
	,sum(visit_freq_90d) as visit_freq_90d
	,sum(visit_freq_7d) as visit_freq_7d, sum(visit_freq_14d) as visit_freq_14d, sum(visit_freq_28d) as visit_freq_28d
	,sum(available_deal_views_90d) as available_deal_views_90d

	,sum(AppFrequency) as AppFrequency
	,sum(WebFrequency) as WebFrequency
	,sum(TouchFrequency) as TouchFrequency

	,count(distinct bcookie) as bcookies

from 
(
select * from grp_gdoop_clv_db.eb_pip_deact_cookie_map_intl_scoring  where record_date = '${dateparam1}' ) p
join 
(
select * from grp_gdoop_clv_db.eb_pip_deact_engagement_temp_intl_scoring  where record_date = '${dateparam1}' ) sf
on p.bcookie = sf.cookie_b
group by p.consumer_id;analyze table grp_gdoop_clv_db.eb_pip_deact_engagement_features_intl_scoring  partition(record_date = '${dateparam1}') compute statistics;alter table grp_gdoop_clv_db.eb_pip_deact_engagement_features_intl_scoring  DROP IF EXISTS PARTITION(record_date <= '${sevendaysago}') purge;------
--step 9
-------
insert overwrite table grp_gdoop_clv_db.eb_pip_consumer_lt_orders_intl_scoring  
partition(record_date = '${dateparam1}')
select 
  consumer_id
  , min(order_date) as first_purchase_date
  ,max(order_date) as last_purchase_date
  ,count(distinct parent_order_uuid) as lt_orders
from grp_gdoop_marketing_analytics_db.me_orders_fgt
where country_id in (107)
and user_brand_affiliation = 'groupon'
and platform_key = 2
and order_date < add_months(date_sub('${dateparam1}',1),-12)
group by consumer_id;analyze table grp_gdoop_clv_db.eb_pip_consumer_lt_orders_intl_scoring  partition(record_date = '${dateparam1}')  compute statistics;alter table grp_gdoop_clv_db.eb_pip_consumer_lt_orders_intl_scoring  DROP IF EXISTS PARTITION(record_date <= '${sevendaysago}') purge;------
  --step 10
  ------
  
insert overwrite table grp_gdoop_clv_db.deact_email_metrics_intl_scoring 
partition (record_date = '${dateparam1}')
Select
        pop.consumer_id
	,min(datediff(add_months(date_sub('${dateparam1}',1), -12),send_date)) as send_recency
	,count(distinct case when send_date >= date_sub(add_months(date_sub('${dateparam1}'
,1), -12),7) then send_id end) as sends_7d
	,count(distinct case when send_date >= date_sub(add_months(date_sub('${dateparam1}',1), -12),30) then send_id end) as sends_30d

	,min(case when d3_open_cnt >= 1 then datediff(add_months(date_sub('${dateparam1}',1), -12),send_date) end) as open_recency
        ,sum(case when send_date >= date_sub(add_months(date_sub('${dateparam1}',1), -12),7) and d3_open_cnt >= 1 then 1 else 0 end) as uniq_3day_opens_7d
        ,sum(case when send_date >= date_sub(add_months(date_sub('${dateparam1}',1), -12),30) and d3_open_cnt >= 1 then 1 else 0 end) as uniq_3day_opens_30d

       ,min(case when d3_click_cnt >= 1 then datediff(add_months(date_sub('${dateparam1}',1), -12),send_date) end) as click_recency
	,sum(case when send_date >= date_sub(add_months(date_sub('${dateparam1}',1), -12),7) and d3_click_cnt >= 1 then 1 else 0 end) as uniq_3day_clicks_7d
	,sum(case when send_date >= date_sub(add_months(date_sub('${dateparam1}',1), -12),30) and d3_click_cnt >= 1 then 1 else 0 end) as uniq_3day_clicks_30d

        , sum(case when d3_unsub_cnt >= 1 then 1 else 0 end) as unsubscription_30d

from  user_groupondw.agg_email hist

join  
(
select * from grp_gdoop_clv_db.eb_pip_deact_train_pop_intl_scoring  where record_date = '${dateparam1}') pop
        on hist.user_uuid = pop.consumer_id

where country_code in ('IE')
and send_date between date_sub(add_months(date_sub('${dateparam1}',1), -12),30) and add_months(date_sub('${dateparam1}',1), -12)
and business_group not in ('transactional')

group by  pop.consumer_id;analyze table grp_gdoop_clv_db.deact_email_metrics_intl_scoring  partition (record_date = '${dateparam1}') compute statistics;alter table grp_gdoop_clv_db.deact_email_metrics_intl_scoring  DROP IF EXISTS PARTITION(record_date <= '${sevendaysago}') purge;-------
--step 11
-------
insert overwrite table grp_gdoop_clv_db.eb_pip_deact_all_features_intl_scoring  
partition(record_date = '${dateparam1}')
select
    pop.brand
    , pop.consumer_id
    , case when rand() < 0.66 then 'training' else 'validation' end as data_set
    , recency_segment
    , frequency_segment
    , recency_9block
    , frequency_9block
    , date_add(t.most_recent_order_date,365) as deact_date
    , r.min_order_date as next_order_date
    , 1 - (case when r.min_order_date <= date_add(t.most_recent_order_date,365) then 1 else 0 end) as deact_flag
    , datediff(pop.training_date,lt.first_purchase_date) as tenure_days
    , datediff(pop.training_date,t.most_recent_order_date) as recency
    , coalesce(t.frequency_T24m,0) as frequency_T24m
    , coalesce(t.nob_T24m,0) as nob_T24m
    , coalesce(t.gp_T24m,0) as gp_T24m
    , coalesce(t.frequency_T12M,0) as frequency_T12M
    , coalesce(t.nob_T12m,0) as nob_T12m
    , coalesce(t.gp_T12m,0) as gp_T12m
    , coalesce(t.local_orders_T24m,0) as local_orders_T24m
    , coalesce(t.shopping_orders_T24m,0) as shopping_orders_T24m
    , coalesce(t.travel_orders_T24m,0) as travel_orders_T24m
    , coalesce(t.app_orders_T24m,0) as app_orders_T24m
    , coalesce(t.touch_orders_T24m,0) as touch_orders_T24m
    , coalesce(t.web_orders_T24m,0) as web_orders_T24m
    , t.most_recent_l1
    , t.most_recent_l2
    , t.most_recent_promo_type
    , most_recent_platform
    , is_activation
    , is_reactivation
    , unique_purchase_quarters as unique_purchase_quarters_T24m
    , visit_recency as last_visit_date
    , datediff(pop.training_date,ef.visit_recency) as visit_recency
    , coalesce(ef.visit_freq_90d,0) as visit_freq_90d
    , coalesce(ef.visit_freq_7d,0) as visit_freq_7d
    , coalesce(ef.visit_freq_14d,0) as visit_freq_14d
    , coalesce(ef.visit_freq_28d,0) as visit_freq_28d
    , coalesce(ef.available_deal_views_90d,0) as available_deal_views_90d
    , coalesce(ef.AppFrequency,0) as AppFrequency
    , coalesce(ef.WebFrequency,0) as WebFrequency
    , coalesce(ef.TouchFrequency,0) as TouchFrequency
    , ef.bcookies
    , email.send_recency as send_recency
    , coalesce(email.sends_7d,0) as sends_7d
    , coalesce(email.sends_30d,0) as sends_30d
    , email.open_recency
    , coalesce(email.uniq_3day_opens_7d,0) as uniq_3day_opens_7d
    , coalesce(email.uniq_3day_opens_30d,0) as uniq_3day_opens_30d
    , email.click_recency
    , coalesce(email.uniq_3day_clicks_7d,0) as uniq_3day_clicks_7d
    , coalesce(email.uniq_3day_clicks_30d,0) as uniq_3day_clicks_30d
    , coalesce(email.unsubscription_30d,0) as unsubscription_30d

from 
(
    select *, record_date as training_date from grp_gdoop_clv_db.eb_pip_deact_train_pop_intl_scoring  where record_date = '${dateparam1}') pop

left outer join 
( select * from grp_gdoop_clv_db.eb_pip_deact_response_intl_scoring where record_date = '${dateparam1}') r
  on  pop.consumer_id = r.consumer_id

left outer join 
(
select * from grp_gdoop_clv_db.eb_pip_deact_temp_intl_scoring  where record_date = '${dateparam1}') t
  on pop.consumer_id = t.consumer_id

left outer join 
(select * from grp_gdoop_clv_db.eb_pip_deact_engagement_features_intl_scoring  where record_date = '${dateparam1}') ef
  on pop.consumer_id = ef.consumer_id
  
left outer join 
(select * from grp_gdoop_clv_db.eb_pip_consumer_lt_orders_intl_scoring  where record_date = '${dateparam1}') lt
  on pop.consumer_id = lt.consumer_id
  
left outer join 
( select * from grp_gdoop_clv_db.deact_email_metrics_intl_scoring  where record_date = '${dateparam1}') email
  on pop.consumer_id = email.consumer_id

DISTRIBUTE BY consumer_id SORT BY consumer_id;analyze table grp_gdoop_clv_db.eb_pip_deact_all_features_intl_scoring  partition (record_date = '${dateparam1}') compute statistics;analyze table grp_gdoop_clv_db.eb_pip_deact_all_features_intl_scoring  partition (record_date = '${dateparam1}') compute statistics;alter table grp_gdoop_clv_db.eb_pip_deact_all_features_intl_scoring  DROP IF EXISTS PARTITION(record_date <= '${sevendaysago}') purge;------
--step 12
------
insert overwrite table grp_gdoop_clv_db.eb_pip_deact_training_intl_scoring 
partition (record_date = '${dateparam1}')

select 
     brand
    , consumer_id 
    , data_set 
    , recency_segment 
    , frequency_segment 
    , recency_9block 
    , frequency_9block 
    , deact_date 
    , next_order_date 
    , deact_flag 
    , tenure_days 
    , recency
    , frequency_T24m 
    , nob_T24m 
    , gp_T24m 
    , frequency_T12M 
    , nob_T12m 
    , gp_T12m 
    , local_orders_T24m 
    , shopping_orders_T24m 
    , travel_orders_T24m 
    , app_orders_T24m 
    , touch_orders_T24m 
    , web_orders_T24m 
    , most_recent_l1
    , most_recent_l2
    , most_recent_promo_type
    , most_recent_platform
    , is_activation
    , is_reactivation 
    , unique_purchase_quarters_T24m
    , last_visit_date
    , visit_recency
    , visit_freq_90d
    , visit_freq_7d
    , visit_freq_14d
    , visit_freq_28d
    , available_deal_views_90d
    , AppFrequency
    , WebFrequency
    , TouchFrequency
    , bcookies
    , send_recency
    , sends_7d 
    , sends_30d
    , open_recency
    , uniq_3day_opens_7d
    , uniq_3day_opens_30d
    , click_recency
    , uniq_3day_clicks_7d
    , uniq_3day_clicks_30d
    , unsubscription_30d
from grp_gdoop_clv_db.eb_pip_deact_all_features_intl_scoring
where data_set = 'training'
and record_date = '${dateparam1}'

DISTRIBUTE BY consumer_id SORT BY consumer_id;analyze table grp_gdoop_clv_db.eb_pip_deact_training_intl_scoring  partition (record_date = '${dateparam1}') compute statistics;alter table grp_gdoop_clv_db.eb_pip_deact_training_intl_scoring  DROP IF EXISTS PARTITION(record_date <= '${sevendaysago}') purge;-------
--step 13
-------
insert overwrite table grp_gdoop_clv_db.eb_pip_deact_validation_intl_scoring 
partition (record_date = '${dateparam1}')
select 
  brand
    , consumer_id 
    , data_set 
    , recency_segment 
    , frequency_segment 
    , recency_9block 
    , frequency_9block 
    , deact_date 
    , next_order_date 
    , deact_flag 
    , tenure_days 
    , recency
    , frequency_T24m 
    , nob_T24m 
    , gp_T24m 
    , frequency_T12M 
    , nob_T12m 
    , gp_T12m 
    , local_orders_T24m 
    , shopping_orders_T24m 
    , travel_orders_T24m 
    , app_orders_T24m 
    , touch_orders_T24m 
    , web_orders_T24m 
    , most_recent_l1
    , most_recent_l2
    , most_recent_promo_type
    , most_recent_platform
    , is_activation
    , is_reactivation 
    , unique_purchase_quarters_T24m
    , last_visit_date
    , visit_recency
    , visit_freq_90d
    , visit_freq_7d
    , visit_freq_14d
    , visit_freq_28d
    , available_deal_views_90d
    , AppFrequency
    , WebFrequency
    , TouchFrequency
    , bcookies
    , send_recency
    , sends_7d 
    , sends_30d
    , open_recency
    , uniq_3day_opens_7d
    , uniq_3day_opens_30d
    , click_recency
    , uniq_3day_clicks_7d
    , uniq_3day_clicks_30d
    , unsubscription_30d
from grp_gdoop_clv_db.eb_pip_deact_all_features_intl_scoring 
where data_set = 'validation'

DISTRIBUTE BY consumer_id SORT BY consumer_id;analyze table grp_gdoop_clv_db.eb_pip_deact_validation_intl_scoring  partition (record_date = '${dateparam1}') compute statistics;alter table grp_gdoop_clv_db.eb_pip_deact_validation_intl_scoring  DROP IF EXISTS PARTITION(record_date <= '${sevendaysago}') purge
