SELECT
            exp_id,
            version,
            var_id,
            vtc,
            bstc,
            unit_type,
            qualified_request,
            qualified_beacon,
            experience_lvl2,
            event_cnt,
            homepage_pv_cnt,
            search_pv_cnt,
            browse_pv_cnt,
            item_pv_cnt,
            signin_pv_cnt,
            atc_cnt,
            cart_remove_cnt,
            buynow_cnt,
            cart_pv_cnt,
            pac_pv_cnt,
            stockup_pv_cnt,
            bookslot_pv_cnt,
            bookslot_confirm_cnt,
            chkout_buttontap_cnt,
            checkout_attempt_cnt,
            review_order_pv_cnt,
            myitems_pv_cnt,
            thankyou_amended_pv_cnt,
            orders,
            gmv,
            units,
            ftb_cnt,
            ff_method_digital_gmv,
            ff_method_digital_units,
            ff_method_shipping_gmv,
            ff_method_shipping_units,
            ff_method_pickup_gmv,
            ff_method_pickup_units,
            ff_method_delivery_gmv,
            ff_method_delivery_units,
            ff_method_other_gmv,
            ff_method_other_units,
            node_type_fc_gmv,
            node_type_fc_units,
            node_type_store_gmv,
            node_type_store_units,
            node_type_3p_gmv,
            node_type_3p_units,
            node_type_digital_gmv,
            node_type_digital_units,
            node_type_other_gmv,
            node_type_other_units,
            div_apprl_gmv,
            div_apprl_units,
            div_cnsmbl_gmv,
            div_cnsmbl_units,
            div_entmnt_gmv,
            div_entmnt_units,
            div_food_gmv,
            div_food_units,
            div_hardln_gmv,
            div_hardln_units,
            div_health_gmv,
            div_health_units,
            div_home_gmv,
            div_home_units,
            div_srvc_gmv,
            div_srvc_units,
            div_other_gmv,
            div_other_units,
            cp,
            partition_day
FROM
      galileo_session_base_table;