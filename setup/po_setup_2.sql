-- assume our SYSADMIN role
USE ROLE sysadmin;

/*---------------------------*/
-- use and scale our PO_DS_WH warehouse
/*---------------------------*/
USE WAREHOUSE po_ds_wh;

ALTER warehouse po_ds_wh SET warehouse_size='large';


/*---------------------------*/
-- create analytics tables
/*---------------------------*/

--> create the table that the app will write back to
CREATE OR REPLACE TABLE tb_po_prod.analytics.pricing_final
(
	brand VARCHAR(16777216),
	item VARCHAR(16777216),
	day_of_week VARCHAR(16777216),
    new_price FLOAT,
	current_price FLOAT,
	recommended_price FLOAT,
	profit_lift FLOAT,
	comment VARCHAR(16777216),
	timestamp TIMESTAMP_NTZ(9)
);

--> create the table with required pricing information for the app
CREATE OR REPLACE TABLE tb_po_prod.analytics.pricing_detail AS
SELECT 
    a.truck_brand_name AS brand,
    a.menu_item_name AS item,
    CASE 
        WHEN a.day_of_week = 0 THEN '7 - Sunday'
        WHEN a.day_of_week = 1 THEN '1 - Monday'
        WHEN a.day_of_week = 2 THEN '2 - Tuesday'
        WHEN a.day_of_week = 3 THEN '3 - Wednesday'
        WHEN a.day_of_week = 4 THEN '4 - Thursday'
        WHEN a.day_of_week = 5 THEN '5 - Friday'
        ELSE '6 - Saturday'
    END AS day_of_week,
    ROUND(b.price::FLOAT, 2) AS current_price,
    ROUND(a.price::FLOAT, 2) AS recommended_price,
    tb_po_prod.analytics.DEMAND_ESTIMATION_MODEL!PREDICT(
        current_price,
        current_price - c.base_price,
        c.base_price,
        c.price_hist_dow,
        c.price_year_dow,
        c.price_month_dow,
        c.price_change_hist_dow,
        c.price_change_year_dow,
        c.price_change_month_dow,
        c.price_hist_roll,
        c.price_year_roll,
        c.price_month_roll,
        c.price_change_hist_roll,
        c.price_change_year_roll,
        c.price_change_month_roll
    )::INT AS current_price_demand,
    tb_po_prod.analytics.demand_estimate_model!PREDICT(
        recommended_price,
        recommended_price - c.base_price,
        c.base_price,
        c.price_hist_dow,
        c.price_year_dow,
        c.price_month_dow,
        c.price_change_hist_dow,
        c.price_change_year_dow,
        c.price_change_month_dow,
        c.price_hist_roll,
        c.price_year_roll,
        c.price_month_roll,
        c.price_change_hist_roll,
        c.price_change_year_roll,
        c.price_change_month_roll
    )::INT AS recommended_price_demand,
    ROUND(
        (
            (recommended_price_demand * (d.prev_avg_profit_wo_item + recommended_price - ROUND(a.cost_of_goods_usd, 2))) 
            - (current_price_demand * (d.prev_avg_profit_wo_item + current_price - ROUND(a.cost_of_goods_usd, 2)))
        ), 0
    ) AS profit_lift,
    c.base_price,
    c.price_hist_dow,
    c.price_year_dow,
    c.price_month_dow,
    c.price_change_hist_dow,
    c.price_change_year_dow,
    c.price_change_month_dow,
    c.price_hist_roll,
    c.price_year_roll,
    c.price_month_roll,
    c.price_change_hist_roll,
    c.price_change_year_roll,
    c.price_change_month_roll,
    d.prev_avg_profit_wo_item AS average_basket_profit,
    ROUND(a.cost_of_goods_usd, 2) AS item_cost,
    recommended_price_demand * (average_basket_profit + recommended_price - item_cost) AS recommended_price_profit,
    current_price_demand * (average_basket_profit + current_price - item_cost) AS current_price_profit
FROM (
    SELECT p.*, m.menu_item_name 
    FROM tb_po_prod.analytics.price_recommendations p
    LEFT JOIN tb_po_prod.raw_pos.menu m ON p.menu_item_id = m.menu_item_id
) a
LEFT JOIN (
    SELECT * 
    FROM tb_po_prod.analytics.demand_est_input_full
    WHERE month = 3 AND year = 2023
) b ON a.day_of_week = b.day_of_week AND a.menu_item_id = b.menu_item_id
LEFT JOIN (
    SELECT * 
    FROM tb_po_prod.analytics.demand_est_input_full
    WHERE month = 4 AND year = 2023
) c ON a.day_of_week = c.day_of_week AND a.menu_item_id = c.menu_item_id
LEFT JOIN (
    SELECT * 
    FROM tb_po_prod.analytics.order_item_cost_agg_v
    WHERE month = 4 AND year = 2023
) d ON a.menu_item_id = d.menu_item_id
ORDER BY brand, item, day_of_week;


--> create pricing table to be displayed in the app
CREATE OR REPLACE TABLE tb_po_prod.analytics.pricing
    AS 
SELECT
    brand, 
    item, 
    day_of_week, 
    current_price AS new_price, 
    current_price, 
    recommended_price, 
    profit_lift
FROM tb_po_prod.analytics.pricing_detail;
