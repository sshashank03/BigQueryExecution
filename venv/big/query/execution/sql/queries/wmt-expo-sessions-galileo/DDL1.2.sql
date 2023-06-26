CREATE TABLE IF NOT EXISTS `{}.{}.{}`
        (
            exp_id STRING,
            version INT64,
            var_id STRING,
            vtc STRING,
            bstc STRING,
            unit_type STRING
            partition_day DATE
        )
        PARTITION BY
            (
                partition_day
            )