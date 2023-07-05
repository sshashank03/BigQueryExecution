CREATE EXTERNAL TABLE IF NOT EXISTS `{}.{}.{}` (
    version string,
    var_id string,
    is_control int,
    vtc string,
    qualified boolean,
    is_outlier boolean,
    dimensional_split map<STRING, STRING>,
    mtrc_nm_val map<STRING,DOUBLE>
)
PARTITIONED BY (
    tenant string,
    snapshot_day string,
    exp_id string)
ROW FORMAT SERDE
    'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES (
    'serialization.format' = '1'
)
STORED AS INPUTFORMAT
    'org.apache.hadoop.mapred.SequenceFileInputFormat'
OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat'
LOCATION
    'gs://galileo-core--galileo_valid_vtc_rollups/'
;
