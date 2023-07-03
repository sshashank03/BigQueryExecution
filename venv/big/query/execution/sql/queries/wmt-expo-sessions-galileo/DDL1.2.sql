CREATE EXTERNAL TABLE IF NOT EXISTS `{}.{}.{}` (
    exp_id string,
    version INT64,
    var_id string,
    is_control INT64,
    vtc string,
    qualified boolean,
    gmv FLOAT64,
    units FLOAT64,
    orders FLOAT64,
    cp FLOAT64)
PARTITIONED BY (
    partition_day string)
ROW FORMAT SERDE
    'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
STORED AS INPUTFORMAT
    'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'
LOCATION
    'gs://galileo-core--galileo_step2_vtc_rollup_web/'
;