CREATE TABLE IF NOT EXISTS `{}.{}.{}` (
    partition_day string,
    exp_id string,
    version INT64,
    var_id string,
    unit_type string,
    is_control INT64,
    exp_start_date string,
    exp_end_date string,
    vtc string,
    qualified boolean,
    experience_lvl2 string,
    event_cnt FLOAT64,
    homepage_pv_cnt FLOAT64,
    search_pv_cnt FLOAT64,
    browse_pv_cnt FLOAT64,
    item_pv_cnt FLOAT64,
    signin_pv_cnt FLOAT64,
    atc_cnt FLOAT64,
    cart_remove_cnt FLOAT64,
    buynow_cnt FLOAT64,
    cart_pv_cnt FLOAT64,
    pac_pv_cnt FLOAT64,
    stockup_pv_cnt FLOAT64,
    bookslot_pv_cnt FLOAT64,
    bookslot_confirm_cnt FLOAT64,
    chkout_buttontap_cnt FLOAT64,
    checkout_attempt_cnt FLOAT64,
    review_order_pv_cnt FLOAT64,
    myitems_pv_cnt FLOAT64,
    thankyou_amended_pv_cnt FLOAT64,
    orders FLOAT64,
    gmv FLOAT64,
    units FLOAT64,
    ftb_cnt FLOAT64,
    ff_method_digital_gmv FLOAT64,
    ff_method_digital_units FLOAT64,
    ff_method_shipping_gmv FLOAT64,
    ff_method_shipping_units FLOAT64,
    ff_method_pickup_gmv FLOAT64,
    ff_method_pickup_units FLOAT64,
    ff_method_delivery_gmv FLOAT64,
    ff_method_delivery_units FLOAT64,
    ff_method_other_gmv FLOAT64,
    ff_method_other_units FLOAT64,
    node_type_fc_gmv FLOAT64,
    node_type_fc_units FLOAT64,
    node_type_store_gmv FLOAT64,
    node_type_store_units FLOAT64,
    node_type_3p_gmv FLOAT64,
    node_type_3p_units FLOAT64,
    node_type_digital_gmv FLOAT64,
    node_type_digital_units FLOAT64,
    node_type_other_gmv FLOAT64,
    node_type_other_units FLOAT64,
    div_apprl_gmv FLOAT64,
    div_apprl_units FLOAT64,
    div_cnsmbl_gmv FLOAT64,
    div_cnsmbl_units FLOAT64,
    div_entmnt_gmv FLOAT64,
    div_entmnt_units FLOAT64,
    div_food_gmv FLOAT64,
    div_food_units FLOAT64,
    div_hardln_gmv FLOAT64,
    div_hardln_units FLOAT64,
    div_health_gmv FLOAT64,
    div_health_units FLOAT64,
    div_home_gmv FLOAT64,
    div_home_units FLOAT64,
    div_srvc_gmv FLOAT64,
    div_srvc_units FLOAT64,
    div_other_gmv FLOAT64,
    div_other_units FLOAT64,
    cp FLOAT64,
    snapshot_day string)
PARTITIONED BY
    snapshot_day
OPTIONS(
  expiration_timestamp=TIMESTAMP "2028-01-01 00:00:00 UTC",
  description=""
);



