-- Run this only if database exists
USE SQLPROJECT1;
GO

-- ===== BRONZE TABLES =====
CREATE SCHEMA IF NOT EXISTS BRONZE;
GO

CREATE TABLE BRONZE.CRM_cust_info (
    cst_id INT,
    cst_key NVARCHAR(50),
    cst_firstname NVARCHAR(50),
    cst_lastname NVARCHAR(50),
    cst_marital_status NVARCHAR(50),
    cst_gndr NVARCHAR(50),
    cst_create_date DATE
);
GO

CREATE TABLE BRONZE.CRM_prd_info (
    prd_id INT,
    prd_key NVARCHAR(50),
    prd_nm NVARCHAR(50),
    prd_cost INT,
    prd_line NVARCHAR(50),
    prd_start_dt DATETIME,
    prd_end_dt DATETIME
);
GO

CREATE TABLE BRONZE.CRM_sales_details (
    sls_ord_num NVARCHAR(50),
    sls_prd_key NVARCHAR(50),
    sls_cust_id INT,
    sls_order_dt INT,
    sls_ship_dt INT,
    sls_due_dt INT,
    sls_sales INT,
    sls_quantity INT,
    sls_priceINT INT
);
GO

CREATE TABLE BRONZE.erp_cust_az12 (
    CID NVARCHAR(50),
    BDATE DATE,
    GEN VARCHAR(50)
);
GO

CREATE TABLE BRONZE.erp_loc_a10 (
    CID NVARCHAR(50),
    CNTRY VARCHAR(50)
);
GO

CREATE TABLE BRONZE.erp_px_cat_g1v2 (
    ID NVARCHAR
