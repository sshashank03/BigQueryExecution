WITH vtc_rollup AS (
    SELECT
        exp_id,
        version,
        var_id,
        is_control,
        vtc,
        MAX(qualified) AS qualified,
        CAST(SUM(gmv) AS DOUBLE) AS gmv,
        CAST(SUM(units) AS DOUBLE) AS units,
        CAST(SUM(orders) AS DOUBLE) AS orders,
        CAST(SUM(cp) AS DOUBLE) AS cp,
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
),

buyer_cnt AS (
    SELECT
        exp_id,
        version,
        var_id,
        is_control,
        COUNT(vtc) AS vtc_cnt,
        COUNT(CASE WHEN gmv > 0 THEN vtc ELSE NULL END) AS buyer_cnt,
        CAST(COUNT(CASE WHEN gmv > 0 THEN vtc ELSE NULL END) AS double)/COUNT(*) AS buyer_pct,
        partition_day
    FROM
        expfw.galileo_step2_vtc_rollup_web
    WHERE
        partition_day = '@partition_day'
    GROUP BY
        exp_id,
        version,
        var_id,
        is_control,
        partition_day
),

buyer_cap_cnt AS (
    SELECT
        a.exp_id,
        a.version,
        a.var_id,
        a.is_control,
        CASE WHEN a.is_control = 1 THEN CAST(a.buyer_cnt * 0.005 AS INT) ELSE CAST(a.vtc_cnt * b.buyer_pct * 0.005 AS INT) END AS buyer_995,
        CASE WHEN a.is_control = 1 THEN CAST(a.buyer_cnt * 0.001 AS INT) ELSE CAST(a.vtc_cnt * b.buyer_pct * 0.001 AS INT) END AS buyer_999
    FROM buyer_cnt a
    INNER JOIN (
        SELECT
            exp_id,
            version,
            buyer_pct
        FROM
            buyer_cnt
        WHERE
            is_control = 1
        AND partition_day = '@partition_day'
    ) b
    ON
        a.exp_id = b.exp_id
    AND a.version = b.version
),

exp_outlier_vtc_pctl AS (
    SELECT
        a.exp_id,
        a.version,
        a.var_id,
        a.is_control,
        CASE WHEN a.is_control = 1 THEN 0.995 ELSE (1 - CAST(a.buyer_995 AS DOUBLE)/b.buyer_cnt) END AS buyer_995_pctl,
        CASE WHEN a.is_control = 1 THEN 0.999 ELSE (1 - CAST(a.buyer_999 AS DOUBLE)/b.buyer_cnt) END AS buyer_999_pctl,
        CASE WHEN a.is_control = 1 THEN 0.005 ELSE (CAST(a.buyer_995 AS DOUBLE)/b.buyer_cnt) END AS buyer_005_pctl
    FROM
        buyer_cap_cnt a
    INNER JOIN
        buyer_cap_cnt b
    ON
        a.exp_id = b.exp_id
    AND a.version = b.version
    AND a.var_id = b

exp_vtc_cap_val AS (
    SELECT
        a.exp_id,
        a.version,
        a.var_id,
        a.is_control,
        b.buyer_995_pctl AS buyer_995_pctl,
        PERCENTILE(CAST((gmv*100) AS BIGINT), b.buyer_995_pctl) / 100 AS gmv_capped,
        PERCENTILE(CAST(units AS BIGINT), b.buyer_995_pctl) AS units_capped,
        PERCENTILE(CAST(orders AS BIGINT), b.buyer_999_pctl) AS orders_capped,
        b.buyer_999_pctl as buyer_999_pctl,
        PERCENTILE(CAST((cp * 100) AS BIGINT), b.buyer_995_pctl) / 100 AS cp_capped,
        PERCENTILE(CAST((cp * 100) AS BIGINT), b.buyer_005_pctl) / 100 AS cp_negative_capped
    FROM
        expfw.galileo_step2_vtc_rollup_web a
    INNER JOIN
        expfw.galileo_step2_exp_outlier_vtc_pctl_web b
    ON
        a.exp_id = b.exp_id
        AND a.version = b.version
        AND a.var_id = b.var_id
    WHERE
        a.gmv > 0
    GROUP BY
        a.exp_id,
        a.version,
        a.var_id,
        a.is_control,
        b.buyer_995_pctl,
        b.buyer_999_pctl
)

CREATE TEMPORARY TABLE galileo_step2_exp_vtc_cap_val_web AS
SELECT *
FROM exp_vtc_cap_val;