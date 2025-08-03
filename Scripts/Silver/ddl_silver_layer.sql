--EXEC SILVER.load_silver;
USE SQLPROJECT1;
GO

CREATE OR ALTER PROCEDURE SILVER.load_silver AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME;

    BEGIN TRY
        PRINT 'SILVER LAYER - TABLE CREATION';
        PRINT '==========================================';


		
        -- 1 CRM_cust_info
        SET @start_time = GETDATE();
        IF OBJECT_ID('Silver.CRM_cust_info', 'U') IS NOT NULL
            RAISERROR('Silver.CRM_cust_info already exists. Creation blocked.', 16, 1);
        ELSE
            CREATE TABLE Silver.CRM_cust_info (
                cst_id INT,
                cst_key NVARCHAR(50),
                cst_firstname NVARCHAR(50),
                cst_lastname NVARCHAR(50),
                cst_marital_status NVARCHAR(50),
                cst_gndr NVARCHAR(50),
                cst_create_date DATE,
				dwh_create_date DATETIME2 DEFAULT GETDATE() -- this extra column represents one of metadata(sudo)
            );
        SET @end_time = GETDATE();
        PRINT 'Silver.CRM_cust_info created in ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' seconds';



        -- 2 CRM_prd_info
        SET @start_time = GETDATE();
        IF OBJECT_ID('Silver.CRM_prd_info', 'U') IS NOT NULL
            RAISERROR('Silver.CRM_prd_info already exists. Creation blocked.', 16, 1);
        ELSE
            CREATE TABLE Silver.CRM_prd_info (
                prd_id INT,
                prd_key NVARCHAR(50),
                prd_nm NVARCHAR(50),
                prd_cost INT,
                prd_line NVARCHAR(50),
                prd_start_dt DATETIME,
                prd_end_dt DATETIME,
				dwh_create_date DATETIME2 DEFAULT GETDATE() -- this extra column represents one of metadata(sudo)
            );
        SET @end_time = GETDATE();

        PRINT 'Silver.CRM_prd_info created in ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' seconds';

       
	   

		-- 3 CRM_sales_details
        SET @start_time = GETDATE();
        IF OBJECT_ID('Silver.CRM_sales_details', 'U') IS NOT NULL
            RAISERROR('Silver.CRM_sales_details already exists. Creation blocked.', 16, 1);
        ELSE
            CREATE TABLE Silver.CRM_sales_details (
                sls_ord_num NVARCHAR(50),
                sls_prd_key NVARCHAR(50),
                sls_cust_id INT,
                sls_order_dt INT,
                sls_ship_dt INT,
                sls_due_dt INT,
                sls_sales INT,
                sls_quantity INT,
                sls_priceINT INT,
				dwh_create_date DATETIME2 DEFAULT GETDATE() -- this extra column represents one of metadata(sudo)
            );
        SET @end_time = GETDATE();
        PRINT 'Silver.CRM_sales_details created in ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' seconds';

       
	
	
	   --4 erp_cust_az12
        SET @start_time = GETDATE();
        IF OBJECT_ID('Silver.erp_cust_az12', 'U') IS NOT NULL
            RAISERROR('Silver.erp_cust_az12 already exists. Creation blocked.', 16, 1);
        ELSE
            CREATE TABLE Silver.erp_cust_az12 (
                CID NVARCHAR(50),
                BDATE DATE,
                GEN VARCHAR(50),
				dwh_create_date DATETIME2 DEFAULT GETDATE() -- this extra column represents one of metadata(sudo)
            );
        SET @end_time = GETDATE();
        PRINT 'Silver.erp_cust_az12 created in ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' seconds';

       
	   
	 
	 
	   --5 erp_loc_a10
        SET @start_time = GETDATE();
        IF OBJECT_ID('Silver.erp_loc_a10', 'U') IS NOT NULL
            RAISERROR('Silver.erp_loc_a10 already exists. Creation blocked.', 16, 1);
        ELSE
            CREATE TABLE Silver.erp_loc_a10 (
                CID NVARCHAR(50),
                CNTRY VARCHAR(50),
				dwh_create_date DATETIME2 DEFAULT GETDATE() -- this extra column represents one of metadata(sudo)
            );
        SET @end_time = GETDATE();
        PRINT 'Silver.erp_loc_a10 created in ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' seconds';

     
	 
	 
	 --6 erp_px_cat_g1v2
        SET @start_time = GETDATE();
        IF OBJECT_ID('Silver.erp_px_cat_g1v2', 'U') IS NOT NULL
            RAISERROR('Silver.erp_px_cat_g1v2 already exists. Creation blocked.', 16, 1);
        ELSE
            CREATE TABLE Silver.erp_px_cat_g1v2 (
                ID NVARCHAR(50),
                CAT NVARCHAR(50),
                SUBCAT NVARCHAR(50),
                MAINTENANCE NVARCHAR(50),
				dwh_create_date DATETIME2 DEFAULT GETDATE() -- this extra column represents one of metadata(sudo)
            );
        SET @end_time = GETDATE();
        PRINT 'Silver.erp_px_cat_g1v2 created in ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' seconds';

        PRINT 'SILVER TABLE SETUP COMPLETED SUCCESSFULLY';

    END TRY
    BEGIN CATCH
        PRINT 'ERROR OCCURRED DURING SILVER SETUP';
        PRINT ERROR_MESSAGE();
    END CATCH
END;
