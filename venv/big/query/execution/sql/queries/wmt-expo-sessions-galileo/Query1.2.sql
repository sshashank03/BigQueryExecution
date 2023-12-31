

INSERT OVERWRITE TABLE expfw.galileo_step2_vtc_rollup_web PARTITION (partition_day)
SELECT
    exp_id,
    version,
    var_id,
    is_control,
    vtc,
    MAX(qualified) AS qualified,
    SUM(gmv) AS gmv,
    SUM(units) AS units,
    SUM(orders) AS orders,
    SUM(cp) AS cp,
    snapshot_day AS partition_day
FROM
    expfw.galileo_step2_active_vtc_totals_web
WHERE
    snapshot_day = '@partition_day'
GROUP BY
    exp_id,
    version,
    var_id,
    is_control,
    vtc,
    snapshot_day
HAVING
    qualified = 1
;