-- same as bronze discription 
USE SQLPROJECT1;
GO
--EXEC SILVER.load_silver1;
CREATE OR ALTER PROCEDURE SILVER.load_silver1 AS 
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME;

    BEGIN TRY
        PRINT ' SILVER LAYER START';
        PRINT '============================';
        PRINT 'CRM SECTION';
        PRINT '============================';

        -- CRM_cust_info
        SET @start_time = GETDATE();
        PRINT 'TRUNCATE: SILVER.CRM_cust_info';
        TRUNCATE TABLE SILVER.CRM_cust_info;

        PRINT 'INSERT: SILVER.CRM_cust_info';
        BULK INSERT SILVER.CRM_cust_info
        FROM 'C:\SQL project satyam\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        WITH (FIRSTROW=2, FIELDTERMINATOR=',', ROWTERMINATOR='\n', TABLOCK);
        SET @end_time = GETDATE();
        PRINT '>> LOAD TIME: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' seconds';

        -- CRM_prd_info
        SET @start_time = GETDATE();
        PRINT 'TRUNCATE: SILVER.CRM_prd_info';
        TRUNCATE TABLE SILVER.CRM_prd_info;

        PRINT 'INSERT: SILVER.CRM_prd_info';
        BULK INSERT SILVER.CRM_prd_info
        FROM 'C:\SQL project satyam\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        WITH (FIRSTROW=2, FIELDTERMINATOR=',', ROWTERMINATOR='\n', TABLOCK);
        SET @end_time = GETDATE();
        PRINT '>> LOAD TIME: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' seconds';

        -- CRM_sales_details
        SET @start_time = GETDATE();
        PRINT 'TRUNCATE: SILVER.CRM_sales_details';
        TRUNCATE TABLE SILVER.CRM_sales_details;

        PRINT 'INSERT: SILVER.CRM_sales_details';
        BULK INSERT SILVER.CRM_sales_details
        FROM 'C:\SQL project satyam\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        WITH (FIRSTROW=2, FIELDTERMINATOR=',', ROWTERMINATOR='\n', TABLOCK);
        SET @end_time = GETDATE();
        PRINT '>> LOAD TIME: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' seconds';

        PRINT '============================';
        PRINT 'ERP SECTION';
        PRINT '============================';

        -- erp_cust_az12
        SET @start_time = GETDATE();
        PRINT 'TRUNCATE: SILVER.erp_cust_az12';
        TRUNCATE TABLE SILVER.erp_cust_az12;

        PRINT 'INSERT: SILVER.erp_cust_az12';
        BULK INSERT SILVER.erp_cust_az12
        FROM 'C:\SQL project satyam\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
        WITH (FIRSTROW=2, FIELDTERMINATOR=',', ROWTERMINATOR='\n', TABLOCK);
        SET @end_time = GETDATE();
        PRINT '>> LOAD TIME: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' seconds';

        -- erp_loc_a10
        SET @start_time = GETDATE();
        PRINT 'TRUNCATE: SILVER.erp_loc_a10';
        TRUNCATE TABLE SILVER.erp_loc_a10;

        PRINT 'INSERT: SILVER.erp_loc_a10';
        BULK INSERT SILVER.erp_loc_a10
        FROM 'C:\SQL project satyam\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
        WITH (FIRSTROW=2, FIELDTERMINATOR=',', ROWTERMINATOR='\n', TABLOCK);
        SET @end_time = GETDATE();
        PRINT '>> LOAD TIME: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' seconds';

        -- erp_px_cat_g1v2
        SET @start_time = GETDATE();
        PRINT 'TRUNCATE: SILVER.erp_px_cat_g1v2';
        TRUNCATE TABLE SILVER.erp_px_cat_g1v2;

        PRINT 'INSERT: SILVER.erp_px_cat_g1v2';
        BULK INSERT SILVER.erp_px_cat_g1v2
        FROM 'C:\SQL project satyam\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
        WITH (FIRSTROW=2, FIELDTERMINATOR=',', ROWTERMINATOR='\n', TABLOCK);
        SET @end_time = GETDATE();
        PRINT '>> LOAD TIME: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' seconds';

        PRINT 'SILVER LAYER LOAD COMPLETED';
    END TRY
    BEGIN CATCH
        PRINT 'ERROR OCCURRED!';
        PRINT ERROR_MESSAGE();
    END CATCH
END;
