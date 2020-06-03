
drop table if exists grp_gdoop_clv_db.ce_keep_deact_predictions;

create external table if not exists grp_gdoop_clv_db.ce_keep_deact_predictions (
    consumer_id string,
    recency_x int,
    prob_deact double
)
partitioned by (record_date string)
stored as orc
location '/user/grp_gdoop_clv/deact-model/ce_keep_deact_predictions'
tblproperties('orc.compress','SNAPPY')
;


-- alter table grp_gdoop_clv_db.ce_keep_deact_predictions
--     add partition(record_date='2018-09-23');