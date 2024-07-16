-- FROSTBYTE_TASTY_BYTES_SETUP_S-- assume our SYSADMIN role
USE ROLE accountadmin;

-- create tb_po_data_scientist
CREATE OR REPLACE ROLE tb_po_data_scientist;

/*---------------------------*/
-- create our Database
/*---------------------------*/
CREATE OR REPLACE DATABASE tb_po_prod;

/*---------------------------*/
-- create our Schemas
/*---------------------------*/
CREATE OR REPLACE SCHEMA tb_po_prod.raw_pos;
CREATE OR REPLACE SCHEMA tb_po_prod.raw_supply_chain;
CREATE OR REPLACE SCHEMA tb_po_prod.raw_customer;
CREATE OR REPLACE SCHEMA tb_po_prod.harmonized;
CREATE OR REPLACE SCHEMA tb_po_prod.analytics;
CREATE OR REPLACE SCHEMA tb_po_prod.raw_safegraph;
CREATE OR REPLACE SCHEMA tb_po_prod.public;

/*---------------------------*/
-- create our Warehouses
/*---------------------------*/

-- data science warehouse
CREATE OR REPLACE WAREHOUSE tb_po_ds_wh
    WAREHOUSE_SIZE = 'xxxlarge'
    WAREHOUSE_TYPE = 'standard'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
COMMENT = 'data science warehouse for tasty bytes';

CREATE OR REPLACE WAREHOUSE tb_po_app_wh
    WAREHOUSE_SIZE = 'xsmall'
    WAREHOUSE_TYPE = 'standard'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
COMMENT = 'streamlit app warehouse for tasty bytes';

-- use our Warehouse
USE WAREHOUSE tb_po_ds_wh;

-- grant tb_po_ds_wh priviledges to tb_po_data_scientist role
GRANT USAGE ON WAREHOUSE tb_po_ds_wh TO ROLE tb_po_data_scientist;
GRANT OPERATE ON WAREHOUSE tb_po_ds_wh TO ROLE tb_po_data_scientist;
GRANT MONITOR ON WAREHOUSE tb_po_ds_wh TO ROLE tb_po_data_scientist;
GRANT MODIFY ON WAREHOUSE tb_po_ds_wh TO ROLE tb_po_data_scientist;

-- grant tb_po_app_wh priviledges to tb_po_data_scientist role
GRANT USAGE ON WAREHOUSE tb_po_app_wh TO ROLE tb_po_data_scientist;
GRANT OPERATE ON WAREHOUSE tb_po_app_wh TO ROLE tb_po_data_scientist;
GRANT MONITOR ON WAREHOUSE tb_po_app_wh TO ROLE tb_po_data_scientist;
GRANT MODIFY ON WAREHOUSE tb_po_app_wh TO ROLE tb_po_data_scientist;

-- grant tb_doc_ai database privileges
GRANT ALL ON DATABASE tb_po_prod TO ROLE tb_po_data_scientist;

GRANT ALL ON SCHEMA tb_po_prod.raw_pos TO ROLE tb_po_data_scientist;
GRANT ALL ON SCHEMA tb_po_prod.raw_supply_chain TO ROLE tb_po_data_scientist;
GRANT ALL ON SCHEMA tb_po_prod.raw_customer TO ROLE tb_po_data_scientist;
GRANT ALL ON SCHEMA tb_po_prod.harmonized TO ROLE tb_po_data_scientist;
GRANT ALL ON SCHEMA tb_po_prod.analytics TO ROLE tb_po_data_scientist;
GRANT ALL ON SCHEMA tb_po_prod.raw_safegraph TO ROLE tb_po_data_scientist;
GRANT ALL ON SCHEMA tb_po_prod.public TO ROLE tb_po_data_scientist;

GRANT CREATE STAGE ON SCHEMA tb_po_prod.analytics TO ROLE tb_po_data_scientist;
GRANT CREATE STAGE ON SCHEMA tb_po_prod.public TO ROLE tb_po_data_scientist;

GRANT ALL ON ALL STAGES IN SCHEMA tb_po_prod.analytics TO ROLE tb_po_data_scientist;
GRANT ALL ON ALL STAGES IN SCHEMA tb_po_prod.public TO ROLE tb_po_data_scientist;

-- set my_user_var variable to equal the logged-in user
SET my_user_var = (SELECT  '"' || CURRENT_USER() || '"' );

-- grant the logged in user the doc_ai_role
GRANT ROLE tb_po_data_scientist TO USER identifier($my_user_var);

USE ROLE tb_po_data_scientist;

show grants to role accountadmin;

/*---------------------------*/
-- create file format
/*---------------------------*/
-- CREATE OR REPLACE FILE FORMAT tb_po_prod.public.csv_ff
-- type = 'csv';

create or replace file format tb_po_prod.public.csv_ff
type = 'CSV'
field_delimiter = ','
record_delimiter = '\n'
field_optionally_enclosed_by = '"'
skip_header = 1;


/*---------------------------*/
-- create Stages
/*---------------------------*/
CREATE OR REPLACE STAGE tb_po_prod.public.analytics_s3
  URL = 's3://sfquickstarts/frostbyte_tastybytes/analytics'
  FILE_FORMAT = tb_po_prod.public.csv_ff;

CREATE OR REPLACE STAGE tb_po_prod.public.harmonized_s3
  URL = 's3://sfquickstarts/frostbyte_tastybytes/harmonized'
  FILE_FORMAT = tb_po_prod.public.csv_ff;

-- raw_safegraph s3
CREATE OR REPLACE STAGE tb_po_prod.public.raw_safegraph_s3
  URL = 's3://sfquickstarts/frostbyte_tastybytes/raw_safegraph'
  FILE_FORMAT = tb_po_prod.public.csv_ff;

-- raw_supply_chain s3
CREATE OR REPLACE STAGE tb_po_prod.public.raw_supply_chain_s3
  URL = 's3://sfquickstarts/frostbyte_tastybytes/raw_supply_chain'
  FILE_FORMAT = tb_po_prod.public.csv_ff;

CREATE OR REPLACE STAGE tb_po_prod.public.excel_s3
  URL = 's3://sfquickstarts/frostbyte_tastybytes/excel'
  FILE_FORMAT = tb_po_prod.public.csv_ff;

-- raw_pos
CREATE OR REPLACE STAGE tb_po_prod.public.raw_pos_s3
  URL = 's3://sfquickstarts/frostbyte_tastybytes/raw_pos'
  FILE_FORMAT = tb_po_prod.public.csv_ff;


-- raw_customer
CREATE OR REPLACE STAGE tb_po_prod.public.raw_customer_s3
  URL = 's3://sfquickstarts/frostbyte_tastybytes/raw_customer'
  FILE_FORMAT = tb_po_prod.public.csv_ff;


/*---------------------------*/
-- create raw_pos tables
/*---------------------------*/

--> menu
CREATE OR REPLACE TABLE tb_po_prod.raw_pos.menu
(
    menu_id NUMBER(19,0),
    menu_type_id NUMBER(38,0),
    menu_type VARCHAR(16777216),
    truck_brand_name VARCHAR(16777216),
    menu_item_id NUMBER(38,0),
    menu_item_name VARCHAR(16777216),
    item_category VARCHAR(16777216),
    item_subcategory VARCHAR(16777216),
    cost_of_goods_usd NUMBER(38,4),
    sale_price_usd NUMBER(38,4),
    menu_item_health_metrics_obj VARIANT
);

--> menu
COPY INTO tb_po_prod.raw_pos.menu
FROM @tb_po_prod.public.raw_pos_s3/menu/menu.csv
file_format = (format_name = 'tb_po_prod.public.csv_ff');

--> truck
CREATE OR REPLACE TABLE tb_po_prod.raw_pos.truck
(
    truck_id NUMBER(38,0),
    menu_type_id NUMBER(38,0),
    primary_city VARCHAR(16777216),
    region VARCHAR(16777216),
    iso_region VARCHAR(16777216),
    country VARCHAR(16777216),
    iso_country_code VARCHAR(16777216),
    franchise_flag NUMBER(38,0),
    year NUMBER(38,0),
    make VARCHAR(16777216),
    model VARCHAR(16777216),
    ev_flag NUMBER(38,0),
    franchise_id NUMBER(38,0),
    truck_opening_date DATE
);

--> truck
COPY INTO tb_po_prod.raw_pos.truck
FROM @tb_po_prod.public.raw_pos_s3/truck/truck.csv
file_format = (format_name = 'tb_po_prod.public.csv_ff');


--> country
CREATE OR REPLACE TABLE tb_po_prod.raw_pos.country
(
	COUNTRY_ID NUMBER(18,0),
	COUNTRY VARCHAR(16777216),
	ISO_CURRENCY VARCHAR(3),
	ISO_COUNTRY VARCHAR(2),
	CITY_ID NUMBER(19,0),
	CITY VARCHAR(16777216),
	CITY_POPULATION NUMBER(38,0)
);

--> country
COPY INTO tb_po_prod.raw_pos.country
FROM @tb_po_prod.public.raw_pos_s3/country/country.csv
file_format = (format_name = 'tb_po_prod.public.csv_ff');

--> franchise
CREATE OR REPLACE TABLE tb_po_prod.raw_pos.franchise
(
    FRANCHISE_ID NUMBER(38,0),
	FIRST_NAME VARCHAR(16777216),
	LAST_NAME VARCHAR(16777216),
	CITY VARCHAR(16777216),
	COUNTRY VARCHAR(16777216),
	E_MAIL VARCHAR(16777216),
	PHONE_NUMBER VARCHAR(16777216)
);

--> franchise
COPY INTO tb_po_prod.raw_pos.franchise
FROM @tb_po_prod.public.raw_pos_s3/franchise/franchise.csv
file_format = (format_name = 'tb_po_prod.public.csv_ff');


--> location
CREATE OR REPLACE TABLE tb_po_prod.raw_pos.location
(
	LOCATION_ID NUMBER(19,0),
	PLACEKEY VARCHAR(16777216),
	LOCATION VARCHAR(16777216),
	CITY VARCHAR(16777216),
	REGION VARCHAR(16777216),
	ISO_COUNTRY_CODE VARCHAR(16777216),
	COUNTRY VARCHAR(16777216)
);

--> location
COPY INTO tb_po_prod.raw_pos.location
FROM @tb_po_prod.public.raw_pos_s3/location/location.csv
file_format = (format_name = 'tb_po_prod.public.csv_ff');

--> order_header
CREATE OR REPLACE TABLE tb_po_prod.raw_pos.order_header
(
    order_id NUMBER(38,0),
    truck_id NUMBER(38,0),
    location_id FLOAT,
    customer_id NUMBER(38,0),
    discount_id VARCHAR(16777216),
    shift_id NUMBER(38,0),
    shift_start_time TIME(9),
    shift_end_time TIME(9),
    order_channel VARCHAR(16777216),
    order_ts TIMESTAMP_NTZ(9),
    served_ts VARCHAR(16777216),
    order_currency VARCHAR(3),
    order_amount NUMBER(38,4),
    order_tax_amount VARCHAR(16777216),
    order_discount_amount VARCHAR(16777216),
    order_total NUMBER(38,4)
);

COPY INTO tb_po_prod.raw_pos.order_header
FROM @tb_po_prod.public.raw_pos_s3/order_header
file_format = (format_name = 'tb_po_prod.public.csv_ff');


--> order_detail
CREATE OR REPLACE TABLE tb_po_prod.raw_pos.order_detail
(
	ORDER_DETAIL_ID NUMBER(38,0),
	ORDER_ID NUMBER(38,0),
	MENU_ITEM_ID NUMBER(38,0),
	DISCOUNT_ID VARCHAR(16777216),
	LINE_NUMBER NUMBER(38,0),
	QUANTITY NUMBER(5,0),
	UNIT_PRICE NUMBER(38,4),
	PRICE NUMBER(38,4),
	ORDER_ITEM_DISCOUNT_AMOUNT VARCHAR(16777216)
);

--> order_detail
COPY INTO tb_po_prod.raw_pos.order_detail
FROM @tb_po_prod.public.raw_pos_s3/order_detail
file_format = (format_name = 'tb_po_prod.public.csv_ff');

/*---------------------------*/
-- create raw_customer table
/*---------------------------*/

--> customer_loyalty
CREATE OR REPLACE TABLE tb_po_prod.raw_customer.customer_loyalty
(
	CUSTOMER_ID NUMBER(38,0),
	FIRST_NAME VARCHAR(16777216),
	LAST_NAME VARCHAR(16777216),
	CITY VARCHAR(16777216),
	COUNTRY VARCHAR(16777216),
	POSTAL_CODE VARCHAR(16777216),
	PREFERRED_LANGUAGE VARCHAR(16777216),
	GENDER VARCHAR(16777216),
	FAVOURITE_BRAND VARCHAR(16777216),
	MARITAL_STATUS VARCHAR(16777216),
	CHILDREN_COUNT VARCHAR(16777216),
	SIGN_UP_DATE DATE,
	BIRTHDAY_DATE DATE,
	E_MAIL VARCHAR(16777216),
	PHONE_NUMBER VARCHAR(16777216)
);

--> customer_loyalty
COPY INTO tb_po_prod.raw_customer.customer_loyalty
FROM @tb_po_prod.public.raw_customer_s3/customer_loyalty
file_format = (format_name = 'tb_po_prod.public.csv_ff');

/*---------------------------*/
-- create raw_supply_chain tables
/*---------------------------*/

--> item
CREATE OR REPLACE TABLE tb_po_prod.raw_supply_chain.item
(
	ITEM_ID NUMBER(38,0),
	NAME VARCHAR(16777216),
	CATEGORY VARCHAR(16777216),
	UNIT VARCHAR(16777216),
	UNIT_PRICE NUMBER(38,9),
	UNIT_CURRENCY VARCHAR(16777216),
	SHELF_LIFE_DAYS NUMBER(38,0),
	VENDOR_ID NUMBER(38,0),
	IMAGE_URL VARCHAR(16777216)
);

--> item
COPY INTO tb_po_prod.raw_supply_chain.item
FROM @tb_po_prod.public.raw_supply_chain_s3/item.csv
file_format = (format_name = 'tb_po_prod.public.csv_ff');


--> recipe
CREATE OR REPLACE TABLE tb_po_prod.raw_supply_chain.recipe
(
	RECIPE_ID NUMBER(38,0),
	MENU_ITEM_ID NUMBER(38,0),
	MENU_ITEM_LINE_ITEM NUMBER(38,0),
	ITEM_ID NUMBER(38,0),
	UNIT_QUANTITY NUMBER(38,9)
);

--> recipe
COPY INTO tb_po_prod.raw_supply_chain.recipe
FROM @tb_po_prod.public.raw_supply_chain_s3/recipe.csv
file_format = (format_name = 'tb_po_prod.public.csv_ff');

--> item_prices
CREATE OR REPLACE TABLE tb_po_prod.raw_supply_chain.item_prices
(
	ITEM_ID NUMBER(38,0),
	UNIT_PRICE NUMBER(38,2),
	START_DATE DATE,
	END_DATE DATE
);

--> item_prices
COPY INTO tb_po_prod.raw_supply_chain.item_prices
FROM @tb_po_prod.public.raw_supply_chain_s3/item_prices.csv
file_format = (format_name = 'tb_po_prod.public.csv_ff');

--> price_elasticity
CREATE OR REPLACE TABLE tb_po_prod.raw_supply_chain.price_elasticity
(
	PE_ID NUMBER(11,0),
	MENU_ITEM_ID NUMBER(38,0),
	PRICE NUMBER(38,2),
	CURRENCY VARCHAR(3),
	FROM_DATE DATE,
	THROUGH_DATE DATE,
	DAY_OF_WEEK NUMBER(2,0)
);

--> price_elasticity
COPY INTO tb_po_prod.raw_supply_chain.price_elasticity
FROM @tb_po_prod.public.raw_supply_chain_s3/price_elasticity.csv
file_format = (format_name = 'tb_po_prod.public.csv_ff');

--> menu_prices
CREATE OR REPLACE TABLE tb_po_prod.raw_supply_chain.menu_prices
(
	MENU_ITEM_ID NUMBER(38,0),
	SALES_PRICE_USD NUMBER(38,2),
	START_DATE DATE,
	END_DATE DATE
);

--> menu_prices
COPY INTO tb_po_prod.raw_supply_chain.menu_prices
FROM @tb_po_prod.public.raw_supply_chain_s3/menu_prices.csv
file_format = (format_name = 'tb_po_prod.public.csv_ff');

/*---------------------------*/
-- create raw_safegraph table
/*---------------------------*/

create or replace TABLE tb_po_prod.raw_safegraph.core_poi_geometry (
	PLACEKEY VARCHAR(16777216),
	PARENT_PLACEKEY VARCHAR(16777216),
	SAFEGRAPH_BRAND_IDS VARCHAR(16777216),
	LOCATION_NAME VARCHAR(16777216),
	BRANDS VARCHAR(16777216),
	STORE_ID VARCHAR(16777216),
	TOP_CATEGORY VARCHAR(16777216),
	SUB_CATEGORY VARCHAR(16777216),
	NAICS_CODE NUMBER(38,0),
	LATITUDE FLOAT,
	LONGITUDE FLOAT,
	STREET_ADDRESS VARCHAR(16777216),
	CITY VARCHAR(16777216),
	REGION VARCHAR(16777216),
	POSTAL_CODE VARCHAR(16777216),
	OPEN_HOURS VARIANT,
	CATEGORY_TAGS VARCHAR(16777216),
	OPENED_ON VARCHAR(16777216),
	CLOSED_ON VARCHAR(16777216),
	TRACKING_CLOSED_SINCE VARCHAR(16777216),
	GEOMETRY_TYPE VARCHAR(16777216),
	POLYGON_WKT VARCHAR(16777216),
	POLYGON_CLASS VARCHAR(16777216),
	ENCLOSED BOOLEAN,
	PHONE_NUMBER VARCHAR(16777216),
	IS_SYNTHETIC BOOLEAN,
	INCLUDES_PARKING_LOT BOOLEAN,
	ISO_COUNTRY_CODE VARCHAR(16777216),
	WKT_AREA_SQ_METERS FLOAT,
	COUNTRY VARCHAR(16777216)
);

--> core_poi_geometry
COPY INTO tb_po_prod.raw_safegraph.core_poi_geometry
FROM @tb_po_prod.public.raw_safegraph_s3/core_poi_geometry.csv
file_format = (format_name = 'tb_po_prod.public.csv_ff');

/*---------------------------*/
-- harmonized views
/*---------------------------*/

--> harmonized orders_v
create or replace view TB_PO_PROD.HARMONIZED.ORDERS_V(
	ORDER_ID,
	TRUCK_ID,
	ORDER_TS,
	ORDER_DETAIL_ID,
	LINE_NUMBER,
	TRUCK_BRAND_NAME,
	MENU_TYPE,
	PRIMARY_CITY,
	REGION,
	COUNTRY,
	FRANCHISE_FLAG,
	FRANCHISE_ID,
	FRANCHISEE_FIRST_NAME,
	FRANCHISEE_LAST_NAME,
	LOCATION_ID,
	PLACEKEY,
	LOCATION_NAME,
	TOP_CATEGORY,
	SUB_CATEGORY,
	LATITUDE,
	LONGITUDE,
	CUSTOMER_ID,
	FIRST_NAME,
	LAST_NAME,
	E_MAIL,
	PHONE_NUMBER,
	CHILDREN_COUNT,
	GENDER,
	MARITAL_STATUS,
	MENU_ITEM_ID,
	MENU_ITEM_NAME,
	QUANTITY,
	UNIT_PRICE,
	PRICE,
	ORDER_AMOUNT,
	ORDER_TAX_AMOUNT,
	ORDER_DISCOUNT_AMOUNT,
	ORDER_TOTAL
) as (


SELECT
    oh.order_id,
    oh.truck_id,
    oh.order_ts,
    od.order_detail_id,
    od.line_number,
    m.truck_brand_name,
    m.menu_type,
    t.primary_city,
    t.region,
    t.country,
    t.franchise_flag,
    t.franchise_id,
    f.first_name AS franchisee_first_name,
    f.last_name AS franchisee_last_name,
    l.location_id,
    cpg.placekey,
    cpg.location_name,
    cpg.top_category,
    cpg.sub_category,
    cpg.latitude,
    cpg.longitude,
    cl.customer_id,
    cl.first_name,
    cl.last_name,
    cl.e_mail,
    cl.phone_number,
    cl.children_count,
    cl.gender,
    cl.marital_status,
    od.menu_item_id,
    m.menu_item_name,
    od.quantity,
    od.unit_price,
    od.price,
    oh.order_amount,
    oh.order_tax_amount,
    oh.order_discount_amount,
    oh.order_total
FROM TB_PO_PROD.raw_pos.ORDER_DETAIL od
JOIN TB_PO_PROD.raw_pos.ORDER_HEADER oh
    ON od.order_id = oh.order_id
JOIN TB_PO_PROD.raw_pos.TRUCK t
    ON oh.truck_id = t.truck_id
JOIN TB_PO_PROD.raw_pos.MENU m
    ON od.menu_item_id = m.menu_item_id
JOIN TB_PO_PROD.raw_pos.FRANCHISE f
    ON t.franchise_id = f.franchise_id
JOIN TB_PO_PROD.raw_pos.LOCATION l
    ON oh.location_id = l.location_id
JOIN TB_PO_PROD.raw_safegraph.CORE_POI_GEOMETRY cpg
    ON cpg.placekey = l.placekey
LEFT JOIN TB_PO_PROD.raw_customer.CUSTOMER_LOYALTY cl
    ON oh.customer_id = cl.customer_id
  );

--> menu_item_cogs_and_price_v
CREATE OR REPLACE VIEW tb_po_prod.harmonized.menu_item_cogs_and_price_v
	AS
SELECT DISTINCT
    r.menu_item_id,
    ip.start_date,
    ip.end_date,
    SUM(ip.unit_price * r.unit_quantity)
        OVER (PARTITION BY r.menu_item_id, ip.start_date, ip.end_date)
            AS cost_of_menu_item_usd,
    mp.sales_price_usd
FROM tb_po_prod.raw_supply_chain.ITEM i
JOIN tb_po_prod.raw_supply_chain.RECIPE r
    ON i.item_id = r.item_id
JOIN tb_po_prod.raw_supply_chain.ITEM_PRICES ip
    ON ip.item_id = r.item_id
JOIN tb_po_prod.raw_supply_chain.MENU_PRICES mp
    ON mp.menu_item_id = r.menu_item_id
    AND mp.start_date = ip.start_date
ORDER BY r.menu_item_id, ip.start_date
  ;

--> order_item_cost_v
CREATE OR REPLACE VIEW tb_po_prod.harmonized.order_item_cost_v
	AS
WITH menu_item_cogs_and_price_v AS
(
    SELECT DISTINCT
        r.menu_item_id,
        ip.start_date,
        ip.end_date,
        SUM(ip.unit_price * r.unit_quantity) OVER (PARTITION BY r.menu_item_id, ip.start_date, ip.end_date) AS cost_of_goods_usd,
        mp.sales_price_usd AS base_price
    FROM tb_po_prod.raw_supply_chain.item i
    JOIN tb_po_prod.raw_supply_chain.recipe r
        ON i.item_id = r.item_id
    JOIN tb_po_prod.raw_supply_chain.item_prices ip
        ON ip.item_id = r.item_id
    JOIN tb_po_prod.raw_supply_chain.menu_prices mp
        ON mp.menu_item_id = r.menu_item_id
        AND mp.start_date = ip.start_date
    JOIN tb_po_prod.raw_pos.menu m
        ON m.menu_item_id = mp.menu_item_id
    WHERE m.item_category <> 'Extra'
),
order_item_total AS
(
    SELECT
        oh.order_id,
        oh.order_ts,
        od.menu_item_id,
        od.quantity,
        m.base_price AS price,
        m.cost_of_goods_usd,
        m.base_price * od.quantity AS order_item_tot,
        oh.order_amount,
        m.cost_of_goods_usd * od.quantity AS order_item_cog,
        SUM(order_item_cog) OVER (PARTITION BY oh.order_id) AS order_cog
    FROM tb_po_prod.raw_pos.order_header oh
    JOIN tb_po_prod.raw_pos.order_detail od
        ON oh.order_id = od.order_id
    JOIN menu_item_cogs_and_price_v m
        ON od.menu_item_id = m.menu_item_id
        AND DATE(oh.order_ts) BETWEEN m.start_date AND m.end_date
)
SELECT
        oi.order_id,
        DATE(oi.order_ts) AS date,
        oi.menu_item_id,
        oi.quantity,
        oi.price,
        oi.cost_of_goods_usd,
        oi.order_item_tot,
        oi.order_item_cog,
        oi.order_amount,
        oi.order_cog,
        oi.order_amount - oi.order_item_tot AS order_amt_wo_item,
        oi.order_cog - oi.order_item_cog AS order_cog_wo_item
FROM order_item_total oi
  ;

--> menu_item_aggregate_dt
CREATE OR REPLACE TABLE TB_PO_PROD.HARMONIZED.MENU_ITEM_AGGREGATE_DT (
	DATE DATE,
	DAY_OF_WEEK NUMBER(2,0),
	MENU_TYPE_ID NUMBER(38,0),
	TRUCK_BRAND_NAME VARCHAR(16777216),
	MENU_ITEM_ID NUMBER(38,0),
	MENU_ITEM_NAME VARCHAR(16777216),
	SALE_PRICE NUMBER(38,2),
	BASE_PRICE NUMBER(38,2),
	COST_OF_GOODS_USD NUMBER(38,2),
	COUNT_ORDERS NUMBER(18,0),
	TOTAL_QUANTITY_SOLD NUMBER(17,0),
	COMPETITOR_PRICE VARCHAR(16777216)
);

--> menu_item_aggregate_dt
COPY INTO tb_po_prod.harmonized.menu_item_aggregate_dt
FROM @tb_po_prod.public.harmonized_s3/menu_item_aggregate_dt
file_format = (format_name = 'tb_po_prod.public.csv_ff');

/*---------------------------*/
-- analytics views
/*---------------------------*/

--> orders_v
create or replace view TB_PO_PROD.ANALYTICS.ORDERS_V(
	DATE,
	ORDER_ID,
	TRUCK_ID,
	ORDER_TS,
	ORDER_DETAIL_ID,
	LINE_NUMBER,
	TRUCK_BRAND_NAME,
	MENU_TYPE,
	PRIMARY_CITY,
	REGION,
	COUNTRY,
	FRANCHISE_FLAG,
	FRANCHISE_ID,
	FRANCHISEE_FIRST_NAME,
	FRANCHISEE_LAST_NAME,
	LOCATION_ID,
	PLACEKEY,
	LOCATION_NAME,
	TOP_CATEGORY,
	SUB_CATEGORY,
	LATITUDE,
	LONGITUDE,
	CUSTOMER_ID,
	FIRST_NAME,
	LAST_NAME,
	E_MAIL,
	PHONE_NUMBER,
	CHILDREN_COUNT,
	GENDER,
	MARITAL_STATUS,
	MENU_ITEM_ID,
	MENU_ITEM_NAME,
	QUANTITY,
	UNIT_PRICE,
	PRICE,
	ORDER_AMOUNT,
	ORDER_TAX_AMOUNT,
	ORDER_DISCOUNT_AMOUNT,
	ORDER_TOTAL
) as (


SELECT DATE(o.order_ts) AS date, * FROM TB_PO_PROD.harmonized.ORDERS_V o
  );

--> menu_item_aggregate_v
CREATE OR REPLACE VIEW tb_po_prod.analytics.menu_item_aggregate_v
	AS
SELECT * RENAME sale_price AS price
FROM tb_po_prod.harmonized.menu_item_aggregate_dt; -- should be menu_item_aggregate_dt



--> menu_item_cogs_and_price_v
CREATE OR REPLACE VIEW tb_po_prod.analytics.menu_item_cogs_and_price_v
	AS
SELECT * FROM tb_po_prod.harmonized.menu_item_cogs_and_price_v;

--> order_item_cost_agg_v
CREATE OR REPLACE VIEW tb_po_prod.analytics.order_item_cost_agg_v
	AS
SELECT
    year,
    month,
    menu_item_id,
	avg_revenue_wo_item,
    avg_cost_wo_item,
    avg_profit_wo_item,
	LAG(avg_revenue_wo_item,1) OVER (PARTITION BY menu_item_id ORDER BY year,month) AS prev_avg_revenue_wo_item,
    LAG(avg_cost_wo_item,1) OVER (PARTITION BY menu_item_id ORDER BY year,month) AS prev_avg_cost_wo_item,
    LAG(avg_profit_wo_item,1) OVER (PARTITION BY menu_item_id ORDER BY year,month) AS prev_avg_profit_wo_item
FROM
(SELECT * FROM (
    (
        SELECT
            oic1.menu_item_id,
            YEAR(oic1.date) AS year,
            MONTH(oic1.date) AS month,
            SUM(oic1.order_amt_wo_item) / SUM(oic1.quantity) AS avg_revenue_wo_item,
            SUM(oic1.order_cog_wo_item) / SUM(oic1.quantity) AS avg_cost_wo_item,
            (SUM(oic1.order_amt_wo_item) - SUM(oic1.order_cog_wo_item)) /SUM(oic1.quantity) AS avg_profit_wo_item
        FROM tb_po_prod.harmonized.order_item_cost_v oic1
        GROUP BY oic1.menu_item_id, YEAR(oic1.date), MONTH(oic1.date)
    )
UNION
    (
    SELECT
            oic2.menu_item_id,
            CASE
                WHEN max_date.max_month = 12 THEN max_date.max_year + 1
            ELSE max_date.max_year
            END AS year,
            CASE
                WHEN max_date.max_month = 12 THEN 1
            ELSE max_date.max_month + 1
            END AS month,
            0 AS avg_revenue_wo_item,
            0 AS avg_cost_wo_item,
            0 AS avg_profit_wo_item
    FROM (
            SELECT DISTINCT
                oh.menu_item_id,
                DATE(oh.order_ts) AS date
            FROM tb_po_prod.harmonized.orders_v oh
        ) oic2
    JOIN
        (
        SELECT
            MONTH(MAX(DATE(oh.order_ts))) AS max_month,
            YEAR(MAX(DATE(oh.order_ts))) AS max_year
        FROM tb_po_prod.harmonized.orders_v oh
        ) max_date
ON YEAR(oic2.date) = max_date.max_year AND MONTH(oic2.date) = max_date.max_month
    )
) oic
ORDER BY oic.menu_item_id, oic.year, oic.month)avg_r_c_wo_item;


/*---------------------------*/
-- scale down warehouse after load
/*---------------------------*/
ALTER WAREHOUSE tb_po_ds_wh SET WAREHOUSE_SIZE = 'Large';


/*---------------------------*/
-- setup completion note
/*---------------------------*/
SELECT 'price optimization setup is now complete' AS note;
