
--select
--    deact_date,
--    sum(v3s) as v3s,
--    sum(v3s_3) as v3s_3,
--    sum(v3s_3_95) as v3s_3_95
--from (
--    select
--        date_add(record_date, 365-recency_x) as deact_date,
--        prob_deact as v3s,
--        case when recency_x >= 362 then 1 else prob_deact end as v3s_3,
--        case when recency_x >= 362 and prob_deact >= 0.95 then 1 else prob_deact end as v3s_3_95
--    from
--        grp_gdoop_clv_db.ce_keep_deact_predictions
--    where
--        record_date = '2018-12-01'
--    ) x
--group by
--    deact_date
--order by
--    deact_date
--;


-- V2 MODEL RESULTS
with summary as (
select
    -- p.consumer_id,
    -- p.prob_deact,
    round(p.prob_deact, 0) as prediction,
    t.deactivated,
    case
        when t.deactivated = 1 then -ln(p.prob_deact)
        else -ln(1 - p.prob_deact)
    end as log_loss
from
    grp_gdoop_clv_db.eb_deact_predictions1 p
left join grp_gdoop_clv_db.ce_keep_deact_target t
    on p.consumer_id = t.consumer_id and p.ds = t.record_date
where
    p.ds = '2018-12-01'
)

select
    int(prediction) as prediction,
    deactivated,
    count(*) as cnt,
    avg(log_loss) as avg_log_loss
from
    summary
group by
    int(prediction),
    deactivated
;


-- V3 MODEL RESULTS
with summary as (
select
    -- p.consumer_id,
    -- case when p.recency_x >= 362 then 1. else p.prob_deact end as prob_deact,
    round(case when p.recency_x >= 362 then 1. else p.prob_deact end, 0) as prediction,
    t.deactivated,
    case
        when t.deactivated = 1 then -ln(case when p.recency_x >= 362 then 1. else p.prob_deact end)
        else -ln(1 - (case when p.recency_x >= 362 then 1. else p.prob_deact end))
    end as log_loss
from
    grp_gdoop_clv_db.ce_keep_deact_predictions p
left join grp_gdoop_clv_db.ce_keep_deact_target t
    on p.consumer_id = t.consumer_id and p.record_date = t.record_date
where
    p.record_date = '2018-12-01'
)

select
    int(prediction) as prediction,
    deactivated,
    count(*) as cnt,
    avg(log_loss) as avg_log_loss
from
    summary
group by
    int(prediction),
    deactivated
;

