INSERT OVERWRITE TABLE expfw.galileo_valid_vtc_rollups PARTITION (tenant, snapshot_day, exp_id)
SELECT
    vtc_rlp.version,
    vtc_rlp.var_id,
    vtc_rlp.is_control,
    vtc,
    qualified,
    CASE
        WHEN
            vtc_rlp.gmv > outlier_capped.gmv_capped
            OR vtc_rlp.units > outlier_capped.units_capped
            OR vtc_rlp.orders > outlier_capped.orders_capped
            OR vtc_rlp.cp > outlier_capped.cp_capped
            OR vtc_rlp.cp < outlier_capped.cp_negative_capped THEN true
        ELSE false
    END AS is_outlier,
    MAP("experience_lvl2", CAST(experience_lvl2 AS STRING)) AS dimensional_split,
    MAP(
        "event_cnt", CAST(event_cnt AS DOUBLE),
        "homepage_pv_cnt", CAST(homepage_pv_cnt AS DOUBLE),
        "search_pv_cnt", CAST(search_pv_cnt AS DOUBLE),
        "browse_pv_cnt", CAST(browse_pv_cnt AS DOUBLE),
        "item_pv_cnt", CAST(item_pv_cnt AS DOUBLE),
        "signin_pv_cnt", CAST(signin_pv_cnt AS DOUBLE),
        "atc_cnt", CAST(atc_cnt AS DOUBLE),
        "cart_remove_cnt", CAST(cart_remove_cnt AS DOUBLE),
        "buynow_cnt", CAST(buynow_cnt AS DOUBLE),
        "cart_pv_cnt", CAST(cart_pv_cnt AS DOUBLE),
        "pac_pv_cnt", CAST(pac_pv_cnt AS DOUBLE),
        "stockup_pv_cnt", CAST(stockup_pv_cnt AS DOUBLE),
        "bookslot_pv_cnt", CAST(bookslot_pv_cnt AS DOUBLE),
        "bookslot_confirm_cnt", CAST(bookslot_confirm_cnt AS DOUBLE),
        "chkout_buttontap_cnt", CAST(chkout_buttontap_cnt AS DOUBLE),
        "checkout_attempt_cnt", CAST(checkout_attempt_cnt AS DOUBLE),
        "review_order_pv_cnt", CAST(review_order_pv_cnt AS DOUBLE),
        "myitems_pv_cnt", CAST(myitems_pv_cnt AS DOUBLE),
        "thankyou_amended_pv_cnt", CAST(thankyou_amended_pv_cnt AS DOUBLE),
        "orders", CAST(orders AS DOUBLE),
        "gmv", CAST(gmv AS DOUBLE),
        "units", CAST(units AS DOUBLE),
        "ftb_cnt", CAST(ftb_cnt AS DOUBLE),
        "ff_method_digital_gmv", CAST(ff_method_digital_gmv AS DOUBLE),
        "ff_method_digital_units", CAST(ff_method_digital_units AS DOUBLE),
        "ff_method_shipping_gmv", CAST(ff_method_shipping_gmv AS DOUBLE),
        "ff_method_shipping_units", CAST(ff_method_shipping_units AS DOUBLE),
        "ff_method_pickup_gmv", CAST(ff_method_pickup_gmv AS DOUBLE),
        "ff_method_pickup_units", CAST(ff_method_pickup_units AS DOUBLE),
        "ff_method_delivery_gmv", CAST(ff_method_delivery_gmv AS DOUBLE),
        "ff_method_delivery_units", CAST(ff_method_delivery_units AS DOUBLE),
        "ff_method_other_gmv", CAST(ff_method_other_gmv AS DOUBLE),
        "ff_method_other_units", CAST(ff_method_other_units AS DOUBLE),
        "node_type_fc_gmv", CAST(node_type_fc_gmv AS DOUBLE),
        "node_type_fc_units", CAST(node_type_fc_units AS DOUBLE),
        "node_type_store_gmv", CAST(node_type_store_gmv AS DOUBLE),
        "node_type_store_units", CAST(node_type_store_units AS DOUBLE),
        "node_type_3p_gmv", CAST(node_type_3p_gmv AS DOUBLE),
        "node_type_3p_units", CAST(node_type_3p_units AS DOUBLE),
        "node_type_digital_gmv", CAST(node_type_digital_gmv AS DOUBLE),
        "node_type_digital_units", CAST(node_type_digital_units AS DOUBLE),
        "node_type_other_gmv", CAST(node_type_other_gmv AS DOUBLE),
        "node_type_other_units", CAST(node_type_other_units AS DOUBLE),
        "div_apprl_gmv", CAST(div_apprl_gmv AS DOUBLE),
        "div_apprl_units", CAST(div_apprl_units AS DOUBLE),
        "div_cnsmbl_gmv", CAST(div_cnsmbl_gmv AS DOUBLE),
        "div_cnsmbl_units", CAST(div_cnsmbl_units AS DOUBLE),
        "div_entmnt_gmv", CAST(div_entmnt_gmv AS DOUBLE),
        "div_entmnt_units", CAST(div_entmnt_units AS DOUBLE),
        "div_food_gmv", CAST(div_food_gmv AS DOUBLE),
        "div_food_units", CAST(div_food_units AS DOUBLE),
        "div_hardln_gmv", CAST(div_hardln_gmv AS DOUBLE),
        "div_hardln_units", CAST(div_hardln_units AS DOUBLE),
        "div_health_gmv", CAST(div_health_gmv AS DOUBLE),
        "div_health_units", CAST(div_health_units AS DOUBLE),
        "div_home_gmv", CAST(div_home_gmv AS DOUBLE),
        "div_home_units", CAST(div_home_units AS DOUBLE),
        "div_srvc_gmv", CAST(div_srvc_gmv AS DOUBLE),
        "div_srvc_units", CAST(div_srvc_units AS DOUBLE),
        "div_other_gmv", CAST(div_other_gmv AS DOUBLE),
        "div_other_units", CAST(div_other_units AS DOUBLE),
        "converted_vtc_cnt", CAST(converted_vtc_cnt AS DOUBLE),
        "cp", CAST(cp AS DOUBLE)
    ) AS mtrc_nm_val,
   'gm_app' AS tenant,
   '@partition_day' AS snapshot_day,
   vtc_rlp.exp_id AS exp_id
FROM (
    SELECT
        *
    FROM
        expfw.galileo_step2_valid_vtc_rollup_temp_app
    WHERE
        partition_day = '@partition_day'
) vtc_rlp
LEFT JOIN (
    SELECT
        *
    FROM
        expfw.galileo_step2_exp_vtc_cap_val_app
    WHERE
        tenant = 'gm_app'
    AND partition_day = '@partition_day'
) outlier_capped
ON
    1 = 1
AND vtc_rlp.exp_id = outlier_capped.exp_id
AND vtc_rlp.version = outlier_capped.version
AND vtc_rlp.var_id = outlier_capped.var_id
AND vtc_rlp.is_control = outlier_capped.is_control
;