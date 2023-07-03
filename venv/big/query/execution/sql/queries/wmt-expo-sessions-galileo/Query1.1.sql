
INSERT OVERWRITE TABLE expfw.galileo_step2_active_vtc_totals_web PARTITION (snapshot_day)
SELECT
    *
FROM
    expfw.galileo_step2_active_vtc_totals_temp_web
WHERE
    snapshot_day = '@partition_day'
AND (CASE WHEN unit_type = 'cid' THEN partition_day <> '@partition_day' ELSE 1=1 END)

UNION ALL

SELECT
    *
FROM
    expfw.galileo_step2_active_vtc_totals_temp_web
WHERE
    unit_type = 'cid'
AND partition_day = '@partition_day'
AND snapshot_day = '@partition_day'
AND qualified = false

UNION ALL

SELECT
    main.*
FROM (
    SELECT
        *
    FROM
        expfw.galileo_step2_active_vtc_totals_temp_web
    WHERE
        snapshot_day = '@partition_day'
) main
LEFT JOIN (
    SELECT
        exp_id,
        vtc
    FROM (
        SELECT
            exp_id,
            var_id,
            is_control,
            vtc
        FROM
            expfw.galileo_step2_active_vtc_totals_temp_web
        WHERE
            qualified = true
        AND snapshot_day = '@partition_day'
        AND unit_type = 'cid'
        GROUP BY
            1,2,3,4
    )tbl
    GROUP BY
        1,2
    HAVING COUNT(*) > 1
) remover
ON
    main.exp_id = remover.exp_id
AND main.vtc = remover.vtc
WHERE
    main.partition_day = '@partition_day'
AND main.unit_type = 'cid'
AND main.qualified = true
AND remover.exp_id IS NULL
;