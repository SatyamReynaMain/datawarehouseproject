
/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Purpose:
    Executes the ETL process to load data from the 'bronze' schema to the 'silver' schema.
    - Truncates target silver tables before loading new data.
    - Transforms and cleanses data during the load from bronze to silver tables.

Actions:
    - Cleanse and normalize data (e.g., trim spaces, map codes to readable formats).
    - Selects the most recent records from the bronze schema.
    - Handles missing or invalid data (e.g., defaulting, recalculating).

Parameters:
    - None. This procedure does not accept any parameters nor return any values.

Usage:
    - Execute with: `EXEC Silver.load_silver;`
===============================================================================
*/


/* Create or replace the procedure each time this script runs */
/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)                     -- Doc header for humans; not executed.
===============================================================================
Script Purpose:                                                            -- Explains intent of the procedure.
    This stored procedure performs the ETL (Extract, Transform, Load)      -- States it’s an ETL routine.
    process to populate the 'silver' schema tables from the 'bronze' schema.-- Source=bronze, target=silver.
	Actions Performed:                                                      -- High-level steps list.
		- Truncates Silver tables.                                          -- Clears target tables before load.
		- Inserts transformed and cleansed data from Bronze into Silver tables.-- Describes the load action.
		
Parameters:                                                                -- Parameter section.
    None.                                                                  -- No inputs expected.
	  This stored procedure does not accept any parameters or return any values.-- Confirms no outputs.

Usage Example:                                                             -- How to execute.
    EXEC Silver.load_silver;                                               -- Example invocation.
===============================================================================
*/
-- ^^^ Doc comment block for humans; not executed. Good for discoverability.   -- Clarifies the block above.

CREATE OR ALTER PROCEDURE silver.load_silver AS                           -- Create or replace the proc in silver schema.
BEGIN                                                                     -- Begin procedure body.
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
                                                                           -- Declare timers for per-step and whole-batch durations.

    BEGIN TRY                                                             -- Start error-protected region.
        SET @batch_start_time = GETDATE();                                -- Capture batch start timestamp.
        PRINT '================================================';          -- Log visual separator.
        PRINT 'Loading Silver Layer';                                      -- Log batch title.
        PRINT '================================================';          -- Log visual separator.

		PRINT '------------------------------------------------';          -- Log section separator.
		PRINT 'Loading CRM Tables';                                        -- Announce CRM group loads.
		PRINT '------------------------------------------------';          -- Log section separator.

		-- Loading silver.crm_cust_info                                    -- Comment marker for next table.
        SET @start_time = GETDATE();                                       -- Start timer for crm_cust_info.
		PRINT '>> Truncating Table: silver.crm_cust_info';                  -- Log upcoming truncate.
		TRUNCATE TABLE silver.crm_cust_info;                                -- Remove all rows; reset identity; requires no FK refs.
		PRINT '>> Inserting Data Into: silver.crm_cust_info';               -- Log upcoming insert.

		INSERT INTO silver.crm_cust_info (                                  -- Begin insert into target columns.
			cst_id,                                                         -- Target: customer ID (key).
			cst_key,                                                        -- Target: customer business key.
			cst_firstname,                                                  -- Target: normalized first name.
			cst_lastname,                                                   -- Target: normalized last name.
			cst_marital_status,                                             -- Target: normalized marital status.
			cst_gndr,                                                       -- Target: normalized gender.
			cst_create_date                                                 -- Target: record create date.
		)
		SELECT                                                              -- Select source + transforms for insert.
			cst_id,                                                         -- Pass-through cst_id from bronze.
			cst_key,                                                        -- Pass-through cst_key from bronze.
			TRIM(cst_firstname) AS cst_firstname,                           -- Remove leading/trailing spaces.
			TRIM(cst_lastname) AS cst_lastname,                             -- Remove leading/trailing spaces.
			CASE                                                            -- Normalize marital status code ? label.
				WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'    -- 'S' ? 'Single'.
				WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'   -- 'M' ? 'Married'.
				ELSE 'n/a'                                                  -- Anything else ? 'n/a'.
			END AS cst_marital_status,                                      -- Output normalized marital status.
			CASE                                                            -- Normalize gender code ? label.
				WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'              -- 'F' ? 'Female'.
				WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'                -- 'M' ? 'Male'.
				ELSE 'n/a'                                                  -- Anything else ? 'n/a'.
			END AS cst_gndr,                                                -- Output normalized gender.
			cst_create_date                                                 -- Pass-through create date.
		FROM (                                                              -- Subquery to deduplicate by latest record.
			SELECT
				*,                                                          -- Select all columns from bronze row.
				ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
                                                                           -- Rank rows per customer; latest gets 1.
			FROM bronze.crm_cust_info                                       -- Source table: raw CRM customer info.
			WHERE cst_id IS NOT NULL                                        -- Exclude rows without customer ID.
		) t                                                                 -- Alias of subquery.
		WHERE flag_last = 1;                                                -- Keep only latest record per cst_id.
		SET @end_time = GETDATE();                                          -- Capture end time of this load.
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
                                                                           -- Log elapsed seconds for this table. 
        PRINT '>> -------------';                                           -- Log a small separator.


		-- Loading silver.crm_prd_info  and clean data 


        SET @start_time = GETDATE();                                       -- Start timer for crm_prd_info.
		PRINT '>> Truncating Table: silver.crm_prd_info';                   -- Log upcoming truncate.
		TRUNCATE TABLE silver.crm_prd_info;                                  -- Clear target product table.
		PRINT '>> Inserting Data Into: silver.crm_prd_info';                -- Log upcoming insert.

		INSERT INTO silver.crm_prd_info (                                   -- Begin insert into product master.
			prd_id,                                                         -- Target: product ID.
			cat_id,                                                         -- Target: derived category ID.
			prd_key,                                                        -- Target: normalized product key.
			prd_nm,                                                         -- Target: product name.
			prd_cost,                                                       -- Target: cost (defaulted if null).
			prd_line,                                                       -- Target: decoded product line label.
			prd_start_dt,                                                   -- Target: effective start date.
			prd_end_dt                                                      -- Target: effective end date.
		)
		SELECT
			prd_id,                                                         -- Pass-through product ID.
			REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,          -- Derive cat_id from first 5 chars; '-'?'_'.
			SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,                 -- Derive prd_key from char 7 to end.
			prd_nm,                                                         -- Pass-through product name.
			ISNULL(prd_cost, 0) AS prd_cost,                                -- Default null cost to 0.
			CASE                                                            -- Decode product line code ? label.
				WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'            -- 'M' ? 'Mountain'.
				WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'                -- 'R' ? 'Road'.
				WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'         -- 'S' ? 'Other Sales'.
				WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'             -- 'T' ? 'Touring'.
				ELSE 'n/a'                                                  -- Unrecognized ? 'n/a'.
			END AS prd_line,                                                -- Output normalized product line.
			CAST(prd_start_dt AS DATE) AS prd_start_dt,                     -- Coerce to DATE (strip time).
			CAST(                                                           -- Compute effective end date as day before next start.
				LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 
				AS DATE
			) AS prd_end_dt                                                 -- If no next row, result may be NULL (open-ended).
		FROM bronze.crm_prd_info;                                           -- Source: raw product master.
        SET @end_time = GETDATE();                                          -- Capture end time of this load.
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
                                                                           -- Log elapsed seconds for this table.
        PRINT '>> -------------';                                           -- Log a small separator.

        -- Loading crm_sales_details                                         -- Comment marker for next table.
        SET @start_time = GETDATE();                                       -- Start timer for crm_sales_details.
		PRINT '>> Truncating Table: silver.crm_sales_details';               -- Log upcoming truncate.
		TRUNCATE TABLE silver.crm_sales_details;                             -- Clear sales details table.
		PRINT '>> Inserting Data Into: silver.crm_sales_details';            -- Log upcoming insert.

		INSERT INTO silver.crm_sales_details (                               -- Begin insert into sales fact-like table.
			sls_ord_num,                                                    -- Target: order number.
			sls_prd_key,                                                    -- Target: product business key.
			sls_cust_id,                                                    -- Target: customer ID.
			sls_order_dt,                                                   -- Target: validated order date.
			sls_ship_dt,                                                    -- Target: validated ship date.
			sls_due_dt,                                                     -- Target: validated due date.
			sls_sales,                                                      -- Target: corrected sales amount.
			sls_quantity,                                                   -- Target: quantity.
			sls_price                                                       -- Target: corrected price.
		)
		SELECT 
			sls_ord_num,                                                    -- Pass-through order number.
			sls_prd_key,                                                    -- Pass-through product key.
			sls_cust_id,                                                    -- Pass-through customer ID.
			CASE                                                            -- Validate/convert order date.
				WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL   -- 0 or wrong length ? NULL.
				ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)            -- yyyymmdd int?varchar?DATE.
			END AS sls_order_dt,                                            -- Output validated order date.
			CASE                                                            -- Validate/convert ship date.
				WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL     -- 0 or wrong length ? NULL.
				ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)             -- yyyymmdd int?varchar?DATE.
			END AS sls_ship_dt,                                             -- Output validated ship date.
			CASE                                                            -- Validate/convert due date.
				WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL       -- 0 or wrong length ? NULL.
				ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)              -- yyyymmdd int?varchar?DATE.
			END AS sls_due_dt,                                              -- Output validated due date.
			CASE                                                            -- Fix inconsistent sales.
				WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
					THEN sls_quantity * ABS(sls_price)                      -- Recompute sales = qty * |price|.
				ELSE sls_sales                                              -- Otherwise keep source value.
			END AS sls_sales,                                               -- Output corrected sales.
			sls_quantity,                                                   -- Pass-through quantity.
			CASE                                                            -- Fix invalid price.
				WHEN sls_price IS NULL OR sls_price <= 0 
					THEN sls_sales / NULLIF(sls_quantity, 0)                -- Back-calc price = sales / qty; avoid /0.
				ELSE sls_price                                              -- Otherwise keep source value.
			END AS sls_price                                                -- Output corrected price.
		FROM bronze.crm_sales_details;                                      -- Source: raw sales details.
        SET @end_time = GETDATE();                                          -- Capture end time of this load.
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
                                                                           -- Log elapsed seconds for this table.
        PRINT '>> -------------';                                           -- Log a small separator.

        -- Loading erp_cust_az12                                             -- Comment marker for next table.
        SET @start_time = GETDATE();                                       -- Start timer for erp_cust_az12.
		PRINT '>> Truncating Table: silver.erp_cust_az12';                  -- Log upcoming truncate.
		TRUNCATE TABLE silver.erp_cust_az12;                                -- Clear ERP customer table.
		PRINT '>> Inserting Data Into: silver.erp_cust_az12';               -- Log upcoming insert.

		INSERT INTO silver.erp_cust_az12 (                                  -- Begin insert into ERP customer table.
			cid,                                                            -- Target: cleaned customer ID.
			bdate,                                                          -- Target: validated birthdate.
			gen                                                             -- Target: normalized gender.
		)
		SELECT
			CASE                                                            -- Normalize customer ID prefix.
				WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))       -- Strip 'NAS' if present.
				ELSE cid                                                     -- Otherwise keep as-is.
			END AS cid,                                                     -- Output normalized cid.
			CASE                                                            -- Validate birthdate.
				WHEN bdate > GETDATE() THEN NULL                            -- Future dates ? NULL.
				ELSE bdate                                                  -- Otherwise keep as-is.
			END AS bdate,                                                   -- Output validated bdate.
			CASE                                                            -- Normalize gender variants.
				WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'      -- Map to 'Female'.
				WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'          -- Map to 'Male'.
				ELSE 'n/a'                                                  -- Unknown/other ? 'n/a'.
			END AS gen                                                      -- Output normalized gender.
		FROM bronze.erp_cust_az12;                                          -- Source: raw ERP customers.
	    SET @end_time = GETDATE();                                          -- Capture end time of this load.
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
                                                                           -- Log elapsed seconds for this table.
        PRINT '>> -------------';                                           -- Log a small separator.

		PRINT '------------------------------------------------';          -- Log section separator.
		PRINT 'Loading ERP Tables';                                        -- Announce ERP group loads (locations, categories).
		PRINT '------------------------------------------------';          -- Log section separator.

        -- Loading erp_loc_a101                                              -- Comment marker for next table.
        SET @start_time = GETDATE();                                       -- Start timer for erp_loc_a101.
		PRINT '>> Truncating Table: silver.erp_loc_a101';                   -- Log upcoming truncate.
		TRUNCATE TABLE silver.erp_loc_a101;                                 -- Clear ERP locations.
		PRINT '>> Inserting Data Into: silver.erp_loc_a101';                -- Log upcoming insert.

		INSERT INTO silver.erp_loc_a101 (                                   -- Begin insert into ERP location table.
			cid,                                                            -- Target: cleaned customer/location ID.
			cntry                                                          -- Target: expanded/normalized country.
		)
		SELECT
			REPLACE(cid, '-', '') AS cid,                                   -- Remove dashes from cid.
			CASE                                                            -- Normalize country representation.
				WHEN TRIM(cntry) = 'DE' THEN 'Germany'                      -- Expand 'DE' ? 'Germany'.
				WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'      -- Map US codes ? full name.
				WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'           -- Blank/null ? 'n/a'.
				ELSE TRIM(cntry)                                            -- Otherwise keep trimmed value.
			END AS cntry                                                    -- Output normalized country.
		FROM bronze.erp_loc_a101;                                           -- Source: raw ERP locations.
	    SET @end_time = GETDATE();                                          -- Capture end time of this load.
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
                                                                           -- Log elapsed seconds for this table.
        PRINT '>> -------------';                                           -- Log a small separator.
		
		-- Loading erp_px_cat_g1v2                                           -- Comment marker for next table.
		SET @start_time = GETDATE();                                       -- Start timer for erp_px_cat_g1v2.
		PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';                -- Log upcoming truncate.
		TRUNCATE TABLE silver.erp_px_cat_g1v2;                              -- Clear price/category mapping.
		PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2';             -- Log upcoming insert.

		INSERT INTO silver.erp_px_cat_g1v2 (                                -- Begin insert; straight copy.
			id,                                                             -- Target: id.
			cat,                                                            -- Target: category.
			subcat,                                                         -- Target: subcategory.
			maintenance                                                     -- Target: maintenance flag/info.
		)
		SELECT
			id,                                                             -- Pass-through id.
			cat,                                                            -- Pass-through category.
			subcat,                                                         -- Pass-through subcategory.
			maintenance                                                     -- Pass-through maintenance.
		FROM bronze.erp_px_cat_g1v2;                                        -- Source: raw ERP price/category data.
		SET @end_time = GETDATE();                                          -- Capture end time of this load.
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
                                                                           -- Log elapsed seconds for this table.
        PRINT '>> -------------';                                           -- Log a small separator.

		SET @batch_end_time = GETDATE();                                    -- Capture batch end time.
		PRINT '=========================================='                  -- Log overall completion banner.
		PRINT 'Loading Silver Layer is Completed';                          -- Announce successful completion.
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
                                                                           -- Log total elapsed seconds for batch.
		PRINT '=========================================='                  -- Log closing banner.
		
	END TRY                                                                -- End TRY block.
	BEGIN CATCH                                                            -- On any error inside TRY, control comes here.
		PRINT '=========================================='                  -- Error banner.
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'                   -- Error message (note: should say SILVER).
		PRINT 'Error Message' + ERROR_MESSAGE();                            -- Print error text.
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);          -- Print error number.
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);           -- Print error state.
		PRINT '=========================================='                  -- End error banner.
	END CATCH                                                              -- End CATCH block.
END                                                                        -- End procedure body.
