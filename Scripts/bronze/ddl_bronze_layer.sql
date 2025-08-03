EXEC BRONZE.load_bronze;
USE SQLPROJECT1;
GO

CREATE OR ALTER PROCEDURE BRONZE.load_bronze AS 
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME;

    BEGIN TRY
        PRINT ' BRONZE LAYER START';
        PRINT '============================';
        PRINT 'CRM SECTION';
        PRINT '============================';

        -- CRM_cust_info
        SET @start_time = GETDATE();
        PRINT 'TRUNCATE: BRONZE.CRM_cust_info';
        TRUNCATE TABLE BRONZE.CRM_cust_info;

        PRINT 'INSERT: BRONZE.CRM_cust_info';
        BULK INSERT BRONZE.CRM_cust_info
        FROM 'C:\Users\akash\OneDrive\Desktop\sayam dsa\data sci\sql database\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        WITH (FIRSTROW=2, FIELDTERMINATOR=',', ROWTERMINATOR='\n', TABLOCK);
        SET @end_time = GETDATE();
        PRINT '>> LOAD TIME: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' seconds';

        -- CRM_prd_info
        SET @start_time = GETDATE();
        PRINT 'TRUNCATE: BRONZE.CRM_prd_info';
        TRUNCATE TABLE BRONZE.CRM_prd_info;

        PRINT 'INSERT: BRONZE.CRM_prd_info';
        BULK INSERT BRONZE.CRM_prd_info
        FROM 'C:\Users\akash\OneDrive\Desktop\sayam dsa\data sci\sql database\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        WITH (FIRSTROW=2, FIELDTERMINATOR=',', ROWTERMINATOR='\n', TABLOCK);
        SET @end_time = GETDATE();
        PRINT '>> LOAD TIME: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' seconds';

        -- CRM_sales_details
        SET @start_time = GETDATE();
        PRINT 'TRUNCATE: BRONZE.CRM_sales_details';
        TRUNCATE TABLE BRONZE.CRM_sales_details;

        PRINT 'INSERT: BRONZE.CRM_sales_details';
        BULK INSERT BRONZE.CRM_sales_details
        FROM 'C:\Users\akash\OneDrive\Desktop\sayam dsa\data sci\sql database\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        WITH (FIRSTROW=2, FIELDTERMINATOR=',', ROWTERMINATOR='\n', TABLOCK);
        SET @end_time = GETDATE();
        PRINT '>> LOAD TIME: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' seconds';

        PRINT '============================';
        PRINT 'ERP SECTION';
        PRINT '============================';

        -- erp_cust_az12
        SET @start_time = GETDATE();
        PRINT 'TRUNCATE: BRONZE.erp_cust_az12';
        TRUNCATE TABLE BRONZE.erp_cust_az12;

        PRINT 'INSERT: BRONZE.erp_cust_az12';
        BULK INSERT BRONZE.erp_cust_az12
        FROM 'C:\Users\akash\OneDrive\Desktop\sayam dsa\data sci\sql database\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
        WITH (FIRSTROW=2, FIELDTERMINATOR=',', ROWTERMINATOR='\n', TABLOCK);
        SET @end_time = GETDATE();
        PRINT '>> LOAD TIME: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' seconds';

        -- erp_loc_a10
        SET @start_time = GETDATE();
        PRINT 'TRUNCATE: BRONZE.erp_loc_a10';
        TRUNCATE TABLE BRONZE.erp_loc_a10;

        PRINT 'INSERT: BRONZE.erp_loc_a10';
        BULK INSERT BRONZE.erp_loc_a10
        FROM 'C:\Users\akash\OneDrive\Desktop\sayam dsa\data sci\sql database\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
        WITH (FIRSTROW=2, FIELDTERMINATOR=',', ROWTERMINATOR='\n', TABLOCK);
        SET @end_time = GETDATE();
        PRINT '>> LOAD TIME: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' seconds';

        -- erp_px_cat_g1v2
        SET @start_time = GETDATE();
        PRINT 'TRUNCATE: BRONZE.erp_px_cat_g1v2';
        TRUNCATE TABLE BRONZE.erp_px_cat_g1v2;

        PRINT 'INSERT: BRONZE.erp_px_cat_g1v2';
        BULK INSERT BRONZE.erp_px_cat_g1v2
        FROM 'C:\Users\akash\OneDrive\Desktop\sayam dsa\data sci\sql database\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
        WITH (FIRSTROW=2, FIELDTERMINATOR=',', ROWTERMINATOR='\n', TABLOCK);
        SET @end_time = GETDATE();
        PRINT '>> LOAD TIME: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' seconds';

        PRINT 'BRONZE LAYER LOAD COMPLETED';
    END TRY
    BEGIN CATCH
        PRINT 'ERROR OCCURRED! '; -- THIS IS SUDO CATCH 
        PRINT ERROR_MESSAGE();
    END CATCH
END;
