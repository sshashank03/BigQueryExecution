CREATE TABLE IF NOT EXISTS `{}.{}.{}` (
    exp_id string,
    version INT64,
    var_id string,
    is_control INT64,
    vtc string,
    qualified boolean,
    gmv FLOAT64,
    units FLOAT64,
    orders FLOAT64,
    cp FLOAT64,
    partition_day string)
PARTITIONED BY
    partition_day
OPTIONS(
  expiration_timestamp=TIMESTAMP "2028-01-01 00:00:00 UTC",
  description=""
);