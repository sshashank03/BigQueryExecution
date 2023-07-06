CREATE TABLE IF NOT EXISTS `{}.{}.{}` (
    version string,
    var_id string,
    is_control int,
    vtc string,
    qualified boolean,
    is_outlier boolean,
    dimensional_split map<STRING, STRING>,
    mtrc_nm_val map<STRING,DOUBLE>,
    tenant string,
    snapshot_day string,
    exp_id string
)
PARTITIONED BY (
    tenant,
    snapshot_day,
    exp_id)
OPTIONS(
  expiration_timestamp=TIMESTAMP "2028-01-01 00:00:00 UTC",
  description=""
);