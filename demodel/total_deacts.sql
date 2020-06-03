
drop table if exists grp_gdoop_clv_db.ce_keep_deact_totals;

create external table if not exists grp_gdoop_clv_db.ce_keep_deact_totals (
    deact_date string,
    total_deacts int
)
partitioned by (record_date string)
stored as orc
location '/user/grp_gdoop_clv/deact-model/ce_keep_deact_totals'
tblproperties('orc.compress','SNAPPY')
;

