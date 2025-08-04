-- this script is for table creation in bronze layer.
USE SQLPROJECT1;
GO
--EXEC BRONZE.load_bronze
CREATE OR ALTER PROCEDURE BRONZE.load_bronze AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME;

    BEGIN TRY
        PRINT 'BRONZE LAYER - TABLE CREATION';
        PRINT '==========================================';

        -- CRM_cust_info
        SET @start_time = GETDATE();
        IF OBJECT_ID('BRONZE.CRM_cust_info', 'U') IS NOT NULL
            RAISERROR('❌ BRONZE.CRM_cust_info already exists. Creation blocked.', 16, 1);
        ELSE
            CREATE TABLE BRONZE.CRM_cust_info (
                cst_id INT,
                cst_key NVARCHAR(50),
                cst_firstname NVARCHAR(50),
                cst_lastname NVARCHAR(50),
                cst_marital_status NVARCHAR(50),
                cst_gndr NVARCHAR(50),
                cst_create_date DATE
            );
        SET @end_time = GETDATE();
        PRINT '✅ BRONZE.CRM_cust_info created in ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' seconds';

        -- CRM_prd_info
        SET @start_time = GETDATE();
        IF OBJECT_ID('BRONZE.CRM_prd_info', 'U') IS NOT NULL
            RAISERROR('❌ BRONZE.CRM_prd_info already exists. Creation blocked.', 16, 1);
        ELSE
            CREATE TABLE BRONZE.CRM_prd_info (
                prd_id INT,
                prd_key NVARCHAR(50),
                prd_nm NVARCHAR(50),
                prd_cost INT,
                prd_line NVARCHAR(50),
                prd_start_dt DATETIME,
                prd_end_dt DATETIME
            );
        SET @end_time = GETDATE();
        PRINT '✅ BRONZE.CRM_prd_info created in ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' seconds';

        -- CRM_sales_details
        SET @start_time = GETDATE();
        IF OBJECT_ID('BRONZE.CRM_sales_details', 'U') IS NOT NULL
            RAISERROR('❌ BRONZE.CRM_sales_details already exists. Creation blocked.', 16, 1);
        ELSE
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
        SET @end_time = GETDATE();
        PRINT '✅ BRONZE.CRM_sales_details created in ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' seconds';

        -- erp_cust_az12
        SET @start_time = GETDATE();
        IF OBJECT_ID('BRONZE.erp_cust_az12', 'U') IS NOT NULL
            RAISERROR('❌ BRONZE.erp_cust_az12 already exists. Creation blocked.', 16, 1);
        ELSE
            CREATE TABLE BRONZE.erp_cust_az12 (
                CID NVARCHAR(50),
                BDATE DATE,
                GEN VARCHAR(50)
            );
        SET @end_time = GETDATE();
        PRINT '✅ BRONZE.erp_cust_az12 created in ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' seconds';

        -- erp_loc_a10
        SET @start_time = GETDATE();
        IF OBJECT_ID('BRONZE.erp_loc_a10', 'U') IS NOT NULL
            RAISERROR('❌ BRONZE.erp_loc_a10 already exists. Creation blocked.', 16, 1);
        ELSE
            CREATE TABLE BRONZE.erp_loc_a10 (
                CID NVARCHAR(50),
                CNTRY VARCHAR(50)
            );
        SET @end_time = GETDATE();
        PRINT '✅ BRONZE.erp_loc_a10 created in ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' seconds';

        -- erp_px_cat_g1v2
        SET @start_time = GETDATE();
        IF OBJECT_ID('BRONZE.erp_px_cat_g1v2', 'U') IS NOT NULL
            RAISERROR('❌ BRONZE.erp_px_cat_g1v2 already exists. Creation blocked.', 16, 1);
        ELSE
            CREATE TABLE BRONZE.erp_px_cat_g1v2 (
                ID NVARCHAR(50),
                CAT NVARCHAR(50),
                SUBCAT NVARCHAR(50),
                MAINTENANCE NVARCHAR(50)
            );
        SET @end_time = GETDATE();
        PRINT '✅ BRONZE.erp_px_cat_g1v2 created in ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' seconds';

        PRINT '✅ BRONZE TABLE SETUP COMPLETED SUCCESSFULLY';

    END TRY
    BEGIN CATCH
        PRINT '⚠️ ERROR OCCURRED DURING BRONZE SETUP';
        PRINT ERROR_MESSAGE();
    END CATCH
END;
GO
