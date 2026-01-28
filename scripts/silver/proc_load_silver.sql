/*
================================================================================
Procedure Name : silver.load_silver
Purpose        :
    This stored procedure loads data from Bronze Layer into the Silver Layer layer
    tables of the data warehouse. The Silver layer acts as transforamtion for
    the data and stores it in its cleaned and transformed form.

    The procedure performs the following actions:
      - Truncates existing Silver tables to ensure a fresh load
      - Loads CRM and ERP source data using BRONZE TABLES
      - Captures start and end time for each table load
      - Logs per-table load duration for operational visibility
      - Captures total batch execution time
      - Provides structured logging using PRINT statements
      - Handles runtime errors using TRY/CATCH blocks

Source Systems :
    - CRM (cust_info, prd_info, sales_details)
    - ERP (cust_az12, loc_a101, px_cat_g1v2)

Layer          : Silver
Load Type      : Full Refresh (Truncate & Load)
Execution Mode : Manual / Scheduled (SQL Agent / Orchestration Tool)

================================================================================
*/
/*======================================================================================
  SILVER LAYER LOAD SCRIPT
======================================================================================*/

CREATE OR ALTER PROCEDURE silver.load_silver
AS
BEGIN
    DECLARE 
        @start_time DATETIME,
        @end_time DATETIME,
        @batch_start_time DATETIME,
        @batch_end_time DATETIME;

    BEGIN TRY
        SET @batch_start_time = GETDATE();

        PRINT '================================';
        PRINT 'Loading Silver Layer';
        PRINT '================================';

        PRINT '--------------------------------';
        PRINT 'LOADING CRM Tables';
        PRINT '--------------------------------';

        --------------------------------------------------------------------------------
        -- silver.crm_cust_info
        --------------------------------------------------------------------------------
        SET @start_time = GETDATE();

        TRUNCATE TABLE silver.crm_cust_info;
        PRINT '>> Table truncated : silver.crm_cust_info';
        PRINT '>> Inserting data into : silver.crm_cust_info';

        INSERT INTO silver.crm_cust_info (
            cst_id,
            cst_key,
            cst_firstname,
            cst_lastname,
            cst_marital_status,
            cst_gndr,
            cst_create_date
        )
        SELECT
            cst_id,
            cst_key,
            TRIM(cst_firstname),
            TRIM(cst_lastname),
            CASE 
                WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
                WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
                ELSE 'n/a'
            END,
            CASE 
                WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
                WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
                ELSE 'n/a'
            END,
            cst_create_date
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
            FROM bronze.crm_cust_info
            WHERE cst_id IS NOT NULL
        ) t
        WHERE flag_last = 1;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration (crm_cust_info): '
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>>----------------';


        --------------------------------------------------------------------------------
        -- silver.crm_prd_info
        --------------------------------------------------------------------------------
        SET @start_time = GETDATE();

        TRUNCATE TABLE silver.crm_prd_info;
        PRINT '>> Table truncated : silver.crm_prd_info';
        PRINT '>> Inserting data into : silver.crm_prd_info';

        INSERT INTO silver.crm_prd_info (
            prd_id, 
            cat_id, 
            prd_key, 
            prd_nm, 
            prd_cost, 
            prd_line, 
            prd_start_dt, 
            prd_end_dt
        )
        SELECT
            prd_id,
            REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_'),
            SUBSTRING(prd_key, 7, LEN(prd_key)),
            prd_nm,
            ISNULL(prd_cost, 0),
            CASE UPPER(TRIM(prd_line))
                WHEN 'M' THEN 'Mountain'
                WHEN 'R' THEN 'Road'
                WHEN 'S' THEN 'Other Sales'
                WHEN 'T' THEN 'Touring'
                ELSE 'n/a'
            END,
            CAST(prd_start_dt AS DATE),
            CAST(
                CASE 
                    WHEN LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) IS NULL
                        THEN NULL
                    ELSE LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1
                END AS DATE
            )
        FROM bronze.crm_prd_info;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration (crm_prd_info): '
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>>----------------';


        --------------------------------------------------------------------------------
        -- silver.crm_sales_details
        --------------------------------------------------------------------------------
        SET @start_time = GETDATE();

        TRUNCATE TABLE silver.crm_sales_details;
        PRINT '>> Table truncated : silver.crm_sales_details';
        PRINT '>> Inserting data into : silver.crm_sales_details';

        INSERT INTO silver.crm_sales_details (
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            sls_order_dt,
            sls_ship_dt,
            sls_due_dt,
            sls_sales,
            sls_quantity,
            sls_price
        )
        SELECT
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) <> 8 THEN NULL
                 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
            END,
            CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) <> 8 THEN NULL
                 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
            END,
            CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) <> 8 THEN NULL
                 ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
            END,
            CASE 
                WHEN sls_sales IS NULL 
                  OR sls_sales <= 0 
                  OR sls_sales <> sls_quantity * ABS(sls_price)
                    THEN sls_quantity * ABS(sls_price)
                ELSE sls_sales
            END,
            sls_quantity,
            CASE 
                WHEN sls_price IS NULL OR sls_price <= 0
                    THEN sls_sales / NULLIF(sls_quantity, 0)
                ELSE sls_price
            END
        FROM bronze.crm_sales_details;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration (crm_sales_details): '
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>>----------------';


        --------------------------------------------------------------------------------
        -- silver.erp_cust_az12
        --------------------------------------------------------------------------------
        SET @start_time = GETDATE();

        TRUNCATE TABLE silver.erp_cust_az12;
        PRINT '>> Table truncated : silver.erp_cust_az12';
        PRINT '>> Inserting data into : silver.erp_cust_az12';

        INSERT INTO silver.erp_cust_az12 (
            cid,
            bdate,
            gen
        )
        SELECT 
            CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid)) ELSE cid END,
            CASE WHEN bdate > GETDATE() THEN NULL ELSE bdate END,
            CASE 
                WHEN REPLACE(REPLACE(UPPER(TRIM(gen)), CHAR(13), ''), CHAR(10), '') IN ('F','FEMALE') THEN 'Female'
                WHEN REPLACE(REPLACE(UPPER(TRIM(gen)), CHAR(13), ''), CHAR(10), '') IN ('M','MALE') THEN 'Male'
                ELSE 'n/a'
            END
        FROM bronze.erp_cust_az12;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration (erp_cust_az12): '
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>>----------------';


        --------------------------------------------------------------------------------
        -- silver.erp_loc_a101
        --------------------------------------------------------------------------------
        SET @start_time = GETDATE();

        TRUNCATE TABLE silver.erp_loc_a101;
        PRINT '>> Table truncated : silver.erp_loc_a101';
        PRINT '>> Inserting data into : silver.erp_loc_a101';

        INSERT INTO silver.erp_loc_a101 (
            cid,
            cntry
        )
        SELECT 
            REPLACE(cid, '-', ''),
            CASE 
                WHEN cleaned_cntry = 'DE' THEN 'Germany'
                WHEN cleaned_cntry IN ('US', 'USA') THEN 'United States'
                WHEN cleaned_cntry = '' OR cleaned_cntry IS NULL THEN 'n/a'
                ELSE cleaned_cntry
            END
        FROM (
            SELECT
                cid,
                LTRIM(RTRIM(
                    REPLACE(REPLACE(cntry, CHAR(13), ''), CHAR(10), '')
                )) AS cleaned_cntry
            FROM bronze.erp_loc_a101
        ) t;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration (erp_loc_a101): '
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>>----------------';


        --------------------------------------------------------------------------------
        -- silver.erp_px_cat_g1v2
        --------------------------------------------------------------------------------
        SET @start_time = GETDATE();

        TRUNCATE TABLE silver.erp_px_cat_g1v2;
        PRINT '>> Table truncated : silver.erp_px_cat_g1v2';
        PRINT '>> Inserting data into : silver.erp_px_cat_g1v2';

        INSERT INTO silver.erp_px_cat_g1v2 (
            id,
            cat,
            subcat,
            maintenance
        )
        SELECT 
            id,
            cat,
            subcat,
            CASE 
                WHEN cleaned_maintenance = '' OR cleaned_maintenance IS NULL THEN 'n/a'
                ELSE cleaned_maintenance
            END
        FROM (
            SELECT       
                id,
                cat,
                subcat,
                LTRIM(RTRIM(
                    REPLACE(REPLACE(maintenance, CHAR(13), ''), CHAR(10), '')
                )) AS cleaned_maintenance
            FROM bronze.erp_px_cat_g1v2
        ) t;

        SET @end_time = GETDATE();
        PRINT '>> Load Duration (erp_px_cat_g1v2): '
              + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';


        --------------------------------------------------------------------------------
        -- Batch End
        --------------------------------------------------------------------------------
        SET @batch_end_time = GETDATE();
        PRINT '================================';
        PRINT 'Silver Layer Load Completed';
        PRINT 'Total Batch Duration: '
              + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR)
              + ' seconds';
        PRINT '================================';

    END TRY
    BEGIN CATCH
        PRINT '❌ Error occurred while loading Silver Layer';
        PRINT ERROR_MESSAGE();
    END CATCH
END;
