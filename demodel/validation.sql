
drop table if exists grp_gdoop_clv_db.ce_tmp_deact_comparison purge;

create table grp_gdoop_clv_db.ce_tmp_deact_comparison
    stored as orc tblproperties('orc.compress' = 'SNAPPY') as

select
    v2.consumer_id as consumer_id_v2,
    v3.consumer_id as consumer_id_v3,
    v2.deact_date as deact_date_v2,
    v3.deact_date as deact_date_v3,
    v2.recency_x as recency_v2,
    v3.recency_x as recency_v3,
    v2.prob_deact as prob_deact_v2,
    v3.prob_deact as prob_deact_v3,
    (v2.prob_deact - v3.prob_deact) as err,
    abs(v2.prob_deact - v3.prob_deact) as abs_err,
    (v2.prob_deact - v3.prob_deact) * (v2.prob_deact - v3.prob_deact) as sq_err,
    v3.record_date

from (
    select
        *,
        date_add(record_date, 365-recency_x) as deact_date
    from
        grp_gdoop_clv_db.ce_keep_deact_predictions
    where
        record_date = '2018-10-31'
    ) v3

full outer join (
    select
        consumer_id,
        deact_date,
        365 - datediff(deact_date, ds) as recency_x,
        prob_deact,
        ds
    from
        grp_gdoop_clv_db.eb_deact_predictions1
    where
        ds = '2018-10-31'
    ) v2 on v2.consumer_id = v3.consumer_id and v2.ds = v3.record_date

group by
    1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12

;


drop table if exists grp_gdoop_clv_db.ce_tmp_deact_accuracy purge;

create table grp_gdoop_clv_db.ce_tmp_deact_accuracy
    stored as orc tblproperties('orc.compress' = 'SNAPPY') as

select
    t.consumer_id,
    c.deact_date_v3 as deact_date,
    c.recency_v3 as recency_x,
    c.prob_deact_v2,
    c.prob_deact_v3,
    t.deactivated

from (
    select
        consumer_id,
        deactivated
    from
        grp_gdoop_clv_db.ce_keep_deact_target
    where
       record_date = '2018-10-31'
    ) t

inner join grp_gdoop_clv_db.ce_tmp_deact_comparison c
    on c.consumer_id_v3 = t.consumer_id

where
    c.recency_v3 between 0 and 364

;


--------------
-- Log Loss --
--------------

select
    case
        when recency_x >= 335 then    '1-30 days'
        when recency_x >= 305 then   '31-60 days'
        when recency_x >= 275 then   '61-90 days'
        when recency_x >= 245 then  '91-120 days'
        when recency_x >= 185 then '121-180 days'
        when recency_x >= 0   then '181-365 days'
    end as days_until_deact,
    sum(prob_deact_v2) as pred_deacts_v2,
    sum(prob_deact_v3) as pred_deacts_v3,
    sum(deactivated) as actual_deacts,
    avg(case when deactivated = 1 then -log(prob_deact_v2) else -log(1-prob_deact_v2) end) as log_loss_v2,
    avg(case when deactivated = 1 then -log(prob_deact_v3) else -log(1-prob_deact_v3) end) as log_loss_v3
from
    grp_gdoop_clv_db.ce_tmp_deact_accuracy
group by
    1
;


---------
-- ROC --
---------

set hive.mapred.mode=nonstrict;

drop table if exists grp_gdoop_clv_db.ce_tmp_deact_roc purge;

create table grp_gdoop_clv_db.ce_tmp_deact_roc
    stored as orc tblproperties('orc.compress' = 'SNAPPY') as

select
    cutoff,
    p,
    n,
    deactivated,
    prob_deact_v2,
    prob_deact_v3
from
    grp_gdoop_clv_db.ce_tmp_deact_accuracy

inner join (
    select
        sum(deactivated) as p, -- 19,002,551
        count(deactivated) - sum(deactivated) as n -- 10,879,090
    from
        grp_gdoop_clv_db.ce_tmp_deact_accuracy
    ) x

inner join (
    select
        explode(array(
        0.00, 0.01, 0.02, 0.03, 0.04, 0.05, 0.06, 0.07, 0.08, 0.09,
        0.10, 0.11, 0.12, 0.13, 0.14, 0.15, 0.16, 0.17, 0.18, 0.19,
        0.20, 0.21, 0.22, 0.23, 0.24, 0.25, 0.26, 0.27, 0.28, 0.29,
        0.30, 0.31, 0.32, 0.33, 0.34, 0.35, 0.36, 0.37, 0.38, 0.39,
        0.40, 0.41, 0.42, 0.43, 0.44, 0.45, 0.46, 0.47, 0.48, 0.49,
        0.50, 0.51, 0.52, 0.53, 0.54, 0.55, 0.56, 0.57, 0.58, 0.59,
        0.60, 0.61, 0.62, 0.63, 0.64, 0.65, 0.66, 0.67, 0.68, 0.69,
        0.70, 0.71, 0.72, 0.73, 0.74, 0.75, 0.76, 0.77, 0.78, 0.79,
        0.80, 0.81, 0.82, 0.83, 0.84, 0.85, 0.86, 0.87, 0.88, 0.89,
        0.90, 0.91, 0.92, 0.93, 0.94, 0.95, 0.96, 0.97, 0.98, 0.99,
        1.00
        )) as cutoff
    ) y

;


select
    cutoff,
    tp_v2 / p as tpr_v2,
    tp_v3 / p as tpr_v3,
    fp_v2 / n as fpr_v2,
    fp_v3 / n as fpr_v3

from (

    select
        cutoff,
        p,
        n,
        sum(case when deactivated = 1 and prob_deact_v2 >= cutoff then 1 else 0 end) as tp_v2,
        sum(case when deactivated = 1 and prob_deact_v3 >= cutoff then 1 else 0 end) as tp_v3,
        sum(case when deactivated = 0 and prob_deact_v2 >= cutoff then 1 else 0 end) as fp_v2,
        sum(case when deactivated = 0 and prob_deact_v3 >= cutoff then 1 else 0 end) as fp_v3

    from
        grp_gdoop_clv_db.ce_tmp_deact_roc

    group by
        cutoff, p, n

    ) x

order by
    cutoff
;


----------------
-- By Recency --
----------------

select
    recency_x,
    avg(deactivated) as pct_deact_actual,
    avg(prob_deact_v2) as avg_prob_deact_v2,
    avg(prob_deact_v3) as avg_prob_deact_v3,
    avg(case when deactivated = 1 then -log(prob_deact_v2) else -log(1-prob_deact_v2) end) as log_loss_v2,
    avg(case when deactivated = 1 then -log(prob_deact_v3) else -log(1-prob_deact_v3) end) as log_loss_v3
from
    grp_gdoop_clv_db.ce_tmp_deact_accuracy
group by
    recency_x
order by
    recency_x
;

