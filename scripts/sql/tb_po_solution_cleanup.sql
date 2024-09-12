USE ROLE securityadmin;
DROP ROLE IF EXISTS tb_po_data_scientist;

USE ROLE accountadmin;
DROP DATABASE IF EXISTS tb_po_prod;
DROP WAREHOUSE IF EXISTS tb_po_ds_wh;
DROP WAREHOUSE IF EXISTS tb_po_app_wh;